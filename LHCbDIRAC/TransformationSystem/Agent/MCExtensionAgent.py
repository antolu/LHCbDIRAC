''' An agent to extend MC productions based on the remaning events to produce.
'''

from DIRAC import S_OK, S_ERROR
from DIRAC.TransformationSystem.Agent.MCExtensionAgent import MCExtensionAgent as DIRACMCExtensionAgent
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from LHCbDIRAC.ProductionManagementSystem.Client.ProductionsClient import ProductionsClient
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
    self.transClient = ProductionsClient()
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
      message = 'RPC call to ProductionRequest service failed : %s' % productionRequests['Message']
      self.log.error( message )
      return S_ERROR( message )

    for productionRequestID, productionRequestSummary in productionRequests.items():
      ret = self._checkProductionRequest( productionRequestID, productionRequestSummary )
      if not ret['OK']:
        return ret

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
      self.cpuNormalizationFactorAvg = productionRequest.CPUNormalizationFactorAvg
      self.log.info( 'Could not get CPUNormalizationFactorAvg from config, defaulting to %d'
                     % self.cpuNormalizationFactorAvg )

  #############################################################################
  def _checkProductionRequest( self, productionRequestID, productionRequestSummary ):
    ''' Check if a production request need to be extended and do it if needed
    '''

    # check if enough events have been produced
    missingEvents = productionRequestSummary['reqTotal'] - productionRequestSummary['bkTotal']
    if productionRequestSummary['bkTotal'] > 0 and missingEvents <= 0:
      message = 'Enough events produced for production request %d' % productionRequestID
      self.log.verbose( message )
      return S_OK( message )

    # get the associated productions/transformations progress
    productionsProgress = self.rpcProductionRequest.getProductionProgressList( long( productionRequestID ) )
    if productionsProgress['OK']:
      productionsProgress = productionsProgress['Value']
    else:
      message = 'Failed to get productions progress : %s' % productionsProgress['Message']
      self.log.error( message )
      return S_ERROR( message )
    productionsProgress = productionsProgress['Rows']

    # get the informations for the productions/transformations
    productions = []
    simulation = None
    for productionProgress in productionsProgress:
      productionID = productionProgress['ProductionID']
      production = self.transClient.getTransformation( productionID )
      if not production['OK']:
        message = 'Failed to get informations on production %d : %s' % ( productionID, production['Message'] )
        self.log.error( message )
        return S_ERROR( message )
      production = production['Value']
      productions.append( production )

      # determine which one is the simulation production
      if production['Type'] in self.transformationTypes:
        simulation = production
        simulationID = productionID
        for productionProgress in productionsProgress:
          if productionProgress['ProductionID'] == simulationID:
            simulationProgress = productionProgress

    if simulation == None:
      message = 'Failed to get simulation production for request %d' % productionRequestID
      self.log.error( message )
      return S_ERROR( message )

    if simulation['Status'].lower() != 'idle':
      # the simulation is still producting events
      message = 'Simulation for production request %d is not Idle (%s)' % ( productionRequestID, simulation['Status'] )
      self.log.verbose( message )
      return S_OK( message )

    if simulationProgress['BkEvents'] < productionRequestSummary['reqTotal']:
      # the number of events produced by the simulation is of the order of the number of events requested
      # -> there is probably no stripping production, no extension factor necessary
      return self._extendProduction( simulation, 1.0, missingEvents )
    else:
      # the number of events produced by the simulation is more than the number of events requested, yet events are missing
      # -> there is probably a stripping production, an extension factor is needed to account for stripped events
      # some events may still be processed (eg. merged), so wait that all the productions are idle
      if all( production['Status'].lower() == 'idle' for production in productions ):
        extensionFactor = float( simulationProgress['BkEvents'] ) / float( productionRequestSummary['bkTotal'] )
        return self._extendProduction( simulation, extensionFactor, missingEvents )

  #############################################################################
  def _extendProduction( self, production, extensionFactor, eventsNeeded ):
    ''' Extend a production to produce eventsNeeded*extensionFactor more events.
    '''

    productionID = production['TransformationID']
    eventsToProduce = eventsNeeded * extensionFactor

    cpuE = float( getProductionParameterValue( production['Body'], 'CPUe' ) )
    if cpuE is None:
      self.log.info( 'Could not get CPUe from production, defaulting to %d' % self.cpuE )
      cpuE = self.cpuE

    max_e = getEventsToProduce( cpuE, self.cpuTimeAvg, self.cpuNormalizationFactorAvg )

    numberOfTasks = int( math.ceil( float( eventsToProduce ) / float( max_e ) ) )

    # extend the transformation by the determined number of tasks
    res = self.transClient.extendTransformation( productionID, numberOfTasks )
    if not res['OK']:
      message = 'Failed to extend transformation %d : %s' % ( productionID, res['Message'] )
      self.log.error( message )
      return S_ERROR( message )
    else:
      message = 'Successfully extended transformation %d by %d tasks' % ( productionID, numberOfTasks )
      self.log.info( message )

      originalStatus = production.get( 'Status', '' )
      res = self.transClient.setStatus( productionID, 'Active', originalStatus )
      if not res['OK']:
        message = 'Failed to set transformation %d to Active (from %s)' % ( productionID, originalStatus )
        self.log.error( message )
        return S_ERROR( message )

      return S_OK( message )
