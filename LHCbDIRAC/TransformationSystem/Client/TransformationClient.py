""" Module that contains client access to the transformation DB handler.
    This is a very simple extension to the DIRAC one
"""

from DIRAC                                                        import S_OK, gLogger
from DIRAC.ConfigurationSystem.Client.Helpers.Operations          import Operations
from DIRAC.TransformationSystem.Client.TransformationClient       import TransformationClient as DIRACTransformationClient
from LHCbDIRAC.ProductionManagementSystem.Utilities.StateMachine  import ProductionsStateMachine
from LHCbDIRAC.TransformationSystem.Utilities.StateMachine        import TransformationFilesStateMachine


class TransformationClient( DIRACTransformationClient ):

  """ Exposes the functionality available in the LHCbDIRAC/TransformationHandler

      This inherits the DIRAC base Client for direct execution of server functionality.
      The following methods are available (although not visible here).

      BK query manipulation
          deleteBookkeepingQuery(queryID)
          deleteTransformationBookkeepingQuery(transName)
          createTransformationQuery(transName,queryDict)
          getBookkeepingQueryForTransformation(transName)
  """

  def __init__( self, **kwargs ):
    DIRACTransformationClient.__init__( self, **kwargs )
    self.opsH = Operations()

  def addTransformation( self, transName, description, longDescription, transfType, plugin, agentType, fileMask,
                         transformationGroup = 'General',
                         groupSize = 1,
                         inheritedFrom = 0,
                         body = '',
                         maxTasks = 0,
                         eventsPerTask = 0,
                         addFiles = True,
                         bkQuery = {},
                         rpc = None,
                         url = '',
                         timeout = 120 ):
    rpcClient = self._getRPC( rpc = rpc, url = url, timeout = timeout )
    res = rpcClient.addTransformation( transName, description, longDescription, transfType, plugin, agentType,
                                       fileMask, transformationGroup, groupSize, inheritedFrom, body,
                                       maxTasks, eventsPerTask, addFiles )
    if not res['OK']:
      return res
    transID = res['Value']
    if bkQuery:
      res = rpcClient.createTransformationQuery( transID, bkQuery )
      if not res['OK']:
        gLogger.error( "Failed to publish BKQuery for transformation", "%s %s" % ( transID, res['Message'] ) )
    return S_OK( transID )

  def setTransformationParameter( self, transID, paramName, paramValue ):
    """ just invokes the state machine check when needed
    """
    if paramName.lower() == 'status':
      return self.setStatus( transID, paramValue )
    else:
      return DIRACTransformationClient.setTransformationParameter( self, transID, paramName, paramValue )

  def setStatus( self, transID, status, originalStatus = '' ):
    """ Performs a state machine check for productions when asked to change the status
    """
    if not originalStatus:
      originalStatus = DIRACTransformationClient.getTransformationParameters( transID, 'Status' )
      if not originalStatus['OK']:
        return originalStatus
      originalStatus = originalStatus['Value']

    transformation = DIRACTransformationClient.getTransformation( self, transID )
    if not transformation['OK']:
      return transformation
    transformationType = transformation['Value']['Status']
    if transformationType in self.opsH.getValue( 'Transformations/DataProcessing', [] ):
      psm = ProductionsStateMachine( originalStatus )
      stateChange = psm.setState( status )
      if not stateChange['OK']:
        return stateChange

    return DIRACTransformationClient.setTransformationParameter( self, transID, 'Status', status )

  def _applyProductionFilesStateMachine( self, tsFilesAsDict, dictOfProposedLFNsStatus, force ):
    """ Apply LHCb state machine for transformation files
    """
    newStatuses = dict()
    for lfn, status in dictOfProposedLFNsStatus.items():
      if not tsFilesAsDict.has_key( lfn ):
        continue
      else:
        if force:
          newStatuses[lfn] = dictOfProposedLFNsStatus[lfn]
        else:
          tfsm = TransformationFilesStateMachine( tsFilesAsDict[lfn][0] )

          if tfsm.state.lower() == 'assigned' and status.lower() == 'unused':
            if ( tsFilesAsDict[lfn][1] % self.maxResetCounter ) == 0:
              status = 'MaxReset'

          stateChange = tfsm.setState( status )
          if not stateChange['OK']:
            newStatuses[lfn] = status
          else:
            newStatuses[lfn] = stateChange['Value']

    return newStatuses
