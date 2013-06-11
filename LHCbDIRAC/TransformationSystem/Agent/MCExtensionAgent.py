''' An agent to extend MC productions based on the remaning events to produce.
'''

from DIRAC import S_OK, S_ERROR
from DIRAC.TransformationSystem.Agent.MCExtensionAgent import MCExtensionAgent as DIRACMCExtensionAgent
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from LHCbDIRAC.Workflow.Modules.ModulesUtilities import getCPUNormalizationFactorAvg, getEventsToProduce
from LHCbDIRAC.Core.Utilities.XMLTreeParser import XMLTreeParser
import math

AGENT_NAME = 'Transformation/MCExtensionAgent'

class MCExtensionAgent( DIRACMCExtensionAgent ):
  ''' MCExtensionAgent
  '''

  def __init__( self, agentName, loadName, baseAgentName, properties = {} ):
    DIRACMCExtensionAgent.__init__( self, agentName, loadName, baseAgentName, properties )

    self.rpcProductionRequest = RPCClient( 'ProductionManagement/ProductionRequest' )
    self.enableFlag = True

  #############################################################################
  def initialize( self ):
    ''' Logs some parameters
    '''

    self.log.info( "Will consider the following transformation types: %s" % str( self.transformationTypes ) )
    self.log.info( "Will create a maximum of %s tasks per iteration" % self.maxIterationTasks )

    return S_OK()

  #############################################################################
  def execute( self ):
    ''' The MCExtensionAgent execution method.
    '''

    self.enableFlag = self.am_getOption( 'EnableFlag', 'True' )
    if not self.enableFlag == 'True':
      self.log.info( 'MCExtensionAgent is disabled by configuration option EnableFlag' )
      return S_OK( 'Disabled via CS flag' )

    # get the production requests in which we are interested
    productionRequests = self.rpcProductionRequest.getProductionRequestSummary( 'Active', 'Simulation' )
    if productionRequests['OK']:
      productionRequests = productionRequests['Value']
    else:
      self.log.error( 'RPC call to ProductionRequest service failed : ' + productionRequests['Message'] )
      return S_ERROR()

    for productionRequestID, productionRequestSummary in productionRequests.items():
      self._checkProductionRequest( productionRequestID, productionRequestSummary )

    return S_OK()

  #############################################################################
  def _checkProductionRequest( self, productionRequestID, productionRequestSummary ):
    ''' Check if a production request need to be extended and do it if needed
    '''

    # check if enough events have been produced
    missingEvents = productionRequestSummary['bkTotal'] - productionRequestSummary['reqTotal']
    if productionRequestSummary['bkTotal'] > 0 and missingEvents <= 0:
      self.log.verbose( 'Enough events produced for production request %d' % productionRequestID )
      return

    # get the associated productions/transformations progress
    productionsProgress = self.rpcProductionRequest.getProductionProgressList( long( productionRequestID ) )
    if productionsProgress['OK']:
      productionsProgress = productionsProgress['Value']
    else:
      self.log.error( 'Failed to get productions progress : ' + productionsProgress['Message'] )
      return
    productionsProgress = productionsProgress['Rows']


    # get the informations for the productions/transformations
    productions = {}
    for productionProgress in productionsProgress:
      productionID = productionProgress['ProductionID']
      production = self.transClient.getTransformation( productionID )
      if not production['OK']:
        self.log.error( 'Failed to get informations on production %d : %s' % ( productionID, production['Message'] ) )
        # return ?
        continue
      production = production['Value']
      productions[productionID] = production

    # determine which one is the simulation production
    simulationID = self._getSimulationProductionID( productions )
    simulation = productions[simulationID]
    for productionProgress in productionsProgress:
      if productionProgress['ProductionID'] == simulationID:
        simulationProgress = productionProgress

    if simulation['Status'].lower() != 'idle':
      return

    if simulationProgress['BkEvents'] < productionRequestSummary['reqTotal']:
      # no extension factor
      self._extendProduction( simulation, simulationID, 1.0, missingEvents )
    else:
      if all( production['Status'].lower() == 'idle' for production in productions ):
        # extension factor
        extensionFactor = float( simulationProgress['BkEvents'] ) / float( productionRequestSummary['bkTotal'] )
        self._extendProduction( simulation, simulationID, extensionFactor, missingEvents )

  #############################################################################
  def _getSimulationProductionID( self, productions ):
    ''' Given a productionID -> production infos dictionnary,
        find the simulation production and returns its id (or None if not found)
    '''

    for productionID, production in productions.items():
      if 'Type' in production and production['Type'] in self.transformationTypes:
        return productionID

    return None

  #############################################################################
  def _extendProduction( self, production, productionID, extensionFactor, eventsNeeded ):
    ''' Extend a production to produce eventsNeeded*extensionFactor more events.
    '''

    eventsToProduce = eventsNeeded * extensionFactor

    # maximum number of events to produce
    # try to get the CPU parameters from the configuration if possible

    # TODO: maybe get these from ProductionRequest ?
    # TODO: don't get time and normalization factor each time
    # TODO: eliminate code redundency with ProductionRequest
    cpuTimeAvgDefault = 1000000.0
    cpuNormalizationFactorAvgDefault = 1.0
    cpuEDefault = 1.0

    op = Operations()
    cpuTimeAvg = op.getValue( 'Transformations/cpuTimeAvg' )
    if cpuTimeAvg is None:
      self.log.info( 'Could not get cpuTimeAvg from config, defaulting to %d' % cpuTimeAvgDefault )
      cpuTimeAvg = cpuTimeAvgDefault

    try:
      cpuNormalizationFactorAvg = getCPUNormalizationFactorAvg()
    except RuntimeError:
      self.log.info( 'Could not get CPUNormalizationFactorAvg from config, defaulting to %d'
                     % cpuNormalizationFactorAvgDefault )
      cpuNormalizationFactorAvg = cpuNormalizationFactorAvgDefault

    cpuE = self._getProductionParameterValue( production, 'CPUe' )
    if cpuE is None:
      self.log.info( 'Could not get CPUe from production, defaulting to %d' % cpuEDefault )
      cpuE = cpuEDefault

    max_e = getEventsToProduce( cpuE, cpuTimeAvg, cpuNormalizationFactorAvg )

    numberOfTasks = int( math.ceil( float( eventsToProduce ) / float( max_e ) ) )

    # extend the transformation by the determined number of tasks
    res = self.transClient.extendTransformation( productionID, numberOfTasks )
    if not res['OK']:
      self.log.error( 'Failed to extend transformation", "%s %s' % ( productionID, res['Message'] ) )
    else:
      self.log.info( 'Successfully extended transformation %d by %d tasks' % ( productionID, numberOfTasks ) )

  #############################################################################
  def _getProductionParameterValue( self, production, parameterName ):
    ''' Get a parameter value from a production
    '''

    # TODO: move to utilities ?
    # lets assume no parameters are different only by case, as it would be a bad idea
    parameterName = parameterName.lower()

    body = production['Body']
    parser = XMLTreeParser()
    parser.parseString( body )
    tree = parser.tree.pop()

    for parameterElement in tree.childrens( 'Parameter' ):
      if parameterElement.attributes['name'].lower() == parameterName:
        valueElement = parameterElement.children
        if valueElement.empty():
          return None
        valueElement = valueElement[0]

        cdataElement = valueElement.children
        if cdataElement.empty():
          return None

        return cdataElement.value

    return None
