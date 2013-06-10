""" :mod: LogUpload
    ====================

    .. module: LogUpload
    :synopsis: logUpload operation handler

    LogUpload operation handler
"""

# # imports
import os.path
from DIRAC                                                      import S_OK, S_ERROR, gMonitor
from DIRAC.RequestManagementSystem.private.OperationHandlerBase import OperationHandlerBase

class LogUpload( OperationHandlerBase ):
  """
  .. class:: LogUpload

  LogUpload operation handler
  """

  def __init__( self, operation = None, csPath = None ):
    """c'tor

    :param self: self reference
    :param Operation operation: Operation instance
    :param str csPath: CS path for this handler
    """
    # # base class ctor
    OperationHandlerBase.__init__( self, operation, csPath )
    # # gMonitor stuff
    gMonitor.registerActivity( "LogUploadAtt", "Log upload attempted",
                               "RequestExecutingAgent", "Files/min", gMonitor.OP_SUM )
    gMonitor.registerActivity( "LogUploadOK", "Replications successful",
                               "RequestExecutingAgent", "Files/min", gMonitor.OP_SUM )
    gMonitor.registerActivity( "LogUploadFail", "Replications failed",
                               "RequestExecutingAgent", "Files/min", gMonitor.OP_SUM )

  def __call__( self ):
    """ LogUpload operation processing """
    # # list of targetSEs

    targetSEs = self.operation.targetSEList

    if len( targetSEs ) != 1:
      self.log.error( "wrong value for TargetSE list = %s, should contain only one target!" % targetSEs )
      self.operation.Error = "Wrong parameters: TargetSE should contain only one targetSE"
      for opFile in self.operation:

        opFile.Status = "Failed"
        opFile.Error = "Wrong parameters: TargetSE should contain only one targetSE"

        gMonitor.addMark( "LogUploadAtt", 1 )
        gMonitor.addMark( "LogUploadFail", 1 )

      return S_ERROR( "TargetSE should contain only one target, got %s" % targetSEs )

    targetSE = targetSEs[0]
    targetWrite = self.rssSEStatus( targetSE, "WriteAccess" )
    if not targetWrite["OK"]:
      self.log.error( targetWrite["Message"] )
      for opFile in self.operation:
        opFile.Status = "Failed"
        opFile.Error = "Wrong parameters: %s" % targetWrite["Message"]
        gMonitor.addMark( "LogUploadAtt", 1 )
        gMonitor.addMark( "LogUploadFail", 1 )
      self.operation.Error = targetWrite["Message"]
      return S_OK()

    if not targetWrite["Value"]:
      self.operation.Error = "TargetSE %s is banned for writing"
      return S_ERROR( self.operation.Error )

    # # get waiting files
    waitingFiles = self.getWaitingFilesList()

    # # loop over files
    for opFile in waitingFiles:
      # # get LFN
      lfn = opFile.LFN
      self.log.info( "processing file %s" % lfn )
      gMonitor.addMark( "LogUploadAtt", 1 )

      destination = '/'.join( lfn.split( '/' )[0:-1] ) + '/' + ( os.path.basename( lfn ) ).split( '_' )[1].split( '.' )[0]
      logUpload = self.replicaManager().replicate( lfn, targetSE, destPath = destination )
      if not logUpload["OK"]:
        gMonitor.addMark( "LogUploadFail", 1 )
        self.dataLoggingClient().addFileRecord( lfn, "LogUploadFail", targetSE, "", "LogUpload" )
        self.log.error( "completely failed to upload log file: %s" % logUpload["Message"] )
        opFile.Error = str( logUpload["Message"] )
        self.operation.Error = str( logUpload["Message"] )
        continue

      logUpload = logUpload["Value"]

      if lfn in logUpload["Failed"]:
        gMonitor.addMark( "LogUploadFail", 1 )
        self.dataLoggingClient().addFileRecord( lfn, "LogUploadFail", targetSE, "", "LogUpload" )

        reason = logUpload["Failed"][lfn]
        self.log.error( "failed to replicate log file %s at %s: %s" % ( lfn, targetSE, reason ) )
        opFile.Error = str( reason )
        opFile.Attempt += 1
        self.operation.Error = str( reason )
        continue

      logUpload = logUpload["Successful"]

      if lfn in logUpload:
        gMonitor.addMark( "LogUploadOK", 1 )
        self.dataLoggingClient().addFileRecord( lfn, "LogUpload", targetSE, "", "LogUpload" )
        opFile.Status = "Done"
        self.log.info( "%s to %s took %s seconds" % ( lfn, targetSE, logUpload[lfn]['LogUpload'] ) )

    return S_OK()
