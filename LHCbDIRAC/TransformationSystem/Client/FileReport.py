""" Just an extension to include the StateMachine
"""
from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.TransformationSystem.Client.FileReport                 import FileReport as DIRACFileReport
from LHCbDIRAC.TransformationSystem.Client.TransformationClient   import TransformationClient
from LHCbDIRAC.TransformationSystem.Utilities.StateMachine        import TransformationFilesStateMachine

class FileReport( DIRACFileReport ):
  """ Replacing setFileStatus and commit to include the LHCb Transformation Client and Transformation Files SM
  """

  def __init__( self ):
    """ c'tor
    """
    super( FileReport, self ).__init__()
    self.transClient = TransformationClient()

  def setFileStatus( self, transformation, lfn, status, currentStatus = 'Assigned', sendFlag = False ):
    """ Set file status in the context of the given transformation - uses State Machine
    """
    if self.statusDict and self.statusDict.has_key( lfn ):
      currentStatus = self.statusDict[lfn]
    currentState = TransformationFilesStateMachine( currentStatus )
    res = currentState.setState( status )
    if not res['OK']:
      return res

    return super( FileReport, self ).setFileStatus( transformation, lfn, status, sendFlag )

  def commit( self ):
    """ Commit pending file status update records
    """
    if not self.statusDict:
      return S_OK()

    # create intermediate status dictionary
    sDict = {}
    for lfn, status in self.statusDict.items():
      if not sDict.has_key( status ):
        sDict[status] = []
      sDict[status].append( lfn )

    summaryDict = {}
    failedResults = []
    for status, lfns in sDict.items():
      res = self.transClient.setFileStatusForTransformation( self.transformation, status, lfns,
                                                      originalStatuses = dict( [( lfn, 'Assigned' ) for lfn in lfns] ) )
      if not res['OK']:
        failedResults.append( res )
        continue
      for lfn, error in res['Value']['Failed'].items():
        gLogger.error( "Failed to update file status", "%s %s" % ( lfn, error ) )
      if res['Value']['Successful']:
        summaryDict[status] = len( res['Value']['Successful'] )
        for lfn in res['Value']['Successful']:
          self.statusDict.pop( lfn )

    if not self.statusDict:
      return S_OK( summaryDict )
    result = S_ERROR( "Failed to update all file statuses" )
    result['FailedResults'] = failedResults
    return result

