''' An agent to extend MC productions based on the remaning events to produce.
'''

from DIRAC import S_OK, S_ERROR
from DIRAC.TransformationSystem.Agent.MCExtensionAgent import MCExtensionAgent as DIRACMCExtensionAgent
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from LHCbDIRAC.ProductionManagementSystem.Client.ProductionRequest import ProductionRequest
from LHCbDIRAC.Workflow.Modules.ModulesUtilities import getCPUNormalizationFactorAvg, getEventsToProduce, getProductionParameterValue

import math

AGENT_NAME = 'Transformation/MCExtensionAgent'

class MCExtensionAgent( DIRACMCExtensionAgent ):
  ''' MCExtensionAgent
  '''

  def __init__( self, agentName, loadName, baseAgentName, properties = {} ):
    DIRACMCExtensionAgent.__init__( self, agentName, loadName, baseAgentName, properties )

    self.rpcProductionRequest = RPCClient( 'ProductionManagement/ProductionRequest' )
    self.enableFlag = True

    # default values
    self.cpuE = 1.0
    self.cpuTimeAvg = 1000000.0
    self.cpuNormalizationFactorAvg = 1.0

  #############################################################################
  def initialize( self ):
    ''' Logs some parameters
    '''

    self.log.info( 'Will consider the following transformation types: %s' % str( self.transformationTypes ) )
    self.log.info( 'Will create a maximum of %s tasks per iteration' % self.maxIterationTasks )

    return S_OK()

  #############################################################################
  def execute( self ):
    ''' The MCExtensionAgent execution method.
    '''

    self.enableFlag = self.am_getOption( 'EnableFlag', 'True' )
    if not self.enableFlag == 'True':
      self.log.info( 'MCExtensionAgent is disabled by configuration option EnableFlag' )
      return S_OK( 'Disabled via CS flag' )

    # done every cycle, as they may have changed
    self._getCPUParameters()

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

  def _getCPUParameters( self ):
    ''' Get the CPUTimeAvg and CPUNormalizationFactorAvg from config,
        or as a fail-over, from ProductionRequest defaults.
    '''

    productionRequest = ProductionRequest()

    op = Operations()
    self.cpuTimeAvg = op.getValue( 'Transformations/cpuTimeAvg' )
    if self.cpuTimeAvg is None:
      self.cpuTimeAvg = productionRequest.CPUTimeAvg
      self.log.info( 'Could not get cpuTimeAvg from config, defaulting to %d' % self.cpuTimeAvg )

    try:
      self.cpuNormalizationFactorAvg = getCPUNormalizationFactorAvg()
    except RuntimeError:
      self.cpuNormalizationFactorAvg = productionRequest
      self.log.info( 'Could not get CPUNormalizationFactorAvg from config, defaulting to %d'
                     % self.cpuNormalizationFactorAvg )

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
      productions.update( production )

      # determine which one is the simulation production
      if production['Type'] in self.transformationTypes:
        simulation = production
        simulationID = productionID
        for productionProgress in productionsProgress:
          if productionProgress['ProductionID'] == simulationID:
            simulationProgress = productionProgress

    if simulation == None:
      self.log.error( 'Failed to get simulation production for request %d' % productionRequestID )

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
  def _extendProduction( self, production, productionID, extensionFactor, eventsNeeded ):
    ''' Extend a production to produce eventsNeeded*extensionFactor more events.
    '''

    eventsToProduce = eventsNeeded * extensionFactor

    cpuE = getProductionParameterValue( production['Body'], 'CPUe' )
    if cpuE is None:
      self.log.info( 'Could not get CPUe from production, defaulting to %d' % self.cpuE )
      cpuE = self.cpuE

    max_e = getEventsToProduce( cpuE, self.cpuTimeAvg, self.cpuNormalizationFactorAvg )

    numberOfTasks = int( math.ceil( float( eventsToProduce ) / float( max_e ) ) )

    # extend the transformation by the determined number of tasks
    res = self.transClient.extendTransformation( productionID, numberOfTasks )
    if not res['OK']:
      self.log.error( 'Failed to extend transformation", "%s %s' % ( productionID, res['Message'] ) )
    else:
      self.log.info( 'Successfully extended transformation %d by %d tasks' % ( productionID, numberOfTasks ) )
