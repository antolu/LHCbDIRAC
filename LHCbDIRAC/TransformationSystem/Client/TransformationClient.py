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
          addBookkeepingQuery(transID,queryDict)
          getBookkeepingQuery(transName)
  """

  def __init__( self, **kwargs ):
    DIRACTransformationClient.__init__( self, **kwargs )
    self.dataProcessingTypes = Operations().getValue( 'Transformations/DataProcessing', [] )

  def addTransformation( self, transName, description, longDescription, transfType, plugin, agentType, fileMask,
                         transformationGroup = 'General',
                         groupSize = 1,
                         inheritedFrom = 0,
                         body = '',
                         maxTasks = 0,
                         eventsPerTask = 0,
                         addFiles = True,
                         bkQuery = None,
                         timeout = 1800 ):
    res = super( TransformationClient, self ).addTransformation( self, transName, description, longDescription,
                                                                 transfType, plugin, agentType, fileMask,
                                                                 transformationGroup = transformationGroup,
                                                                 groupSize = groupSize,
                                                                 inheritedFrom = inheritedFrom,
                                                                 body = body,
                                                                 maxTasks = maxTasks,
                                                                 eventsPerTask = eventsPerTask,
                                                                 addFiles = addFiles,
                                                                 timeout = timeout )
    if not res['OK']:
      return res
    transID = res['Value']
    if bkQuery:
      res = self._getRPC().addBookkeepingQuery( transID, bkQuery )
      if not res['OK']:
        gLogger.error( "Failed to publish BKQuery for transformation", "%s %s" % ( transID, res['Message'] ) )
        return res
    return S_OK( transID )

  def _applyTransformationStatusStateMachine( self, transIDAsDict, dictOfProposedstatus, force ):
    """ Performs a state machine check for productions when asked to change the status
    """
    originalStatus, transformationType = transIDAsDict.values()[0][0:2]
    proposedStatus = dictOfProposedstatus.values()[0]
    if force:
      return proposedStatus
    else:
      if transformationType in self.dataProcessingTypes:
        stateChange = ProductionsStateMachine( originalStatus ).setState( proposedStatus )
        if not stateChange['OK']:
          return originalStatus
        else:
          return stateChange['Value']
      else:
        return proposedStatus


  def _applyTransformationFilesStateMachine( self, tsFilesAsDict, dictOfProposedLFNsStatus, force ):
    """ Apply LHCb state machine for transformation files
    """
    newStatuses = dict()
    for lfn, newStatus in dictOfProposedLFNsStatus.iteritems():
      if lfn in tsFilesAsDict:
        if force:
          # We do whatever is requested
          newStatus = dictOfProposedLFNsStatus[lfn]
        else:
          currentStatus = tsFilesAsDict[lfn][0]
          # Special case for Assigned -> Unused
          if currentStatus.lower() == 'assigned' and newStatus.lower() == 'unused':
            errorCount = tsFilesAsDict[lfn][1]
            if errorCount and ( ( errorCount % self.maxResetCounter ) == 0 ):
              newStatus = 'MaxReset'

          tfsm = TransformationFilesStateMachine( currentStatus )
          stateChange = tfsm.setState( newStatus )
          if stateChange['OK']:
            newStatus = stateChange['Value']
        # Only worth setting the status if different from current one
        if newStatus.lower() != currentStatus.lower():
          newStatuses[lfn] = newStatus

    return newStatuses
