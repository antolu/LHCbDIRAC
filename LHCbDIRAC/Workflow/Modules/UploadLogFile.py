""" UploadLogFile module is used to upload the files present in the working
    directory.
"""

import os
import shutil
import glob
import random
import stat

from DIRAC                                              import S_OK, S_ERROR, gLogger, gConfig
from DIRAC.Core.Utilities.Subprocess                    import shellCall
from DIRAC.DataManagementSystem.Client.FailoverTransfer import FailoverTransfer
from DIRAC.RequestManagementSystem.Client.Operation     import Operation
from DIRAC.RequestManagementSystem.Client.File          import File
from DIRAC.Resources.Storage.StorageElement             import StorageElement
from DIRAC.Core.Utilities.ReturnValues                  import returnSingleResult

from LHCbDIRAC.Workflow.Modules.ModuleBase              import ModuleBase
from LHCbDIRAC.Workflow.Modules.ModulesUtilities        import tarFiles
from LHCbDIRAC.Core.Utilities.ProductionData            import getLogPath

__RCSID__ = "$Id$"

class UploadLogFile( ModuleBase ):
  """ Upload to LogSE
  """

  #############################################################################

  def __init__( self, bkClient = None, dm = None ):
    """Module initialization.
    """

    self.log = gLogger.getSubLogger( "UploadLogFile" )
    super( UploadLogFile, self ).__init__( self.log, bkClientIn = bkClient, dm = dm )

    self.version = __RCSID__

    self.logSE = self.opsH.getValue( 'LogStorage/LogSE', 'LogSE' )
    self.logSizeLimit = self.opsH.getValue( 'LogFiles/SizeLimit', 1 * 1024 * 1024 )
    self.logExtensions = self.opsH.getValue( 'LogFiles/Extensions', [] )
    self.failoverSEs = gConfig.getValue( '/Resources/StorageElementGroups/Tier1-Failover', [] )
    self.diracLogo = self.opsH.getValue( 'SAM/LogoURL',
                                         'https://lhcbweb.pic.es/DIRAC/images/logos/DIRAC-logo-transp.png' )
    self.logFilePath = ''
    self.logLFNPath = ''
    self.logdir = ''
    self.failoverTransfer = None

######################################################################

  def _resolveInputVariables( self ):

    super( UploadLogFile, self )._resolveInputVariables()

    if self.workflow_commons.has_key( 'LogTargetPath' ):
      self.logLFNPath = self.workflow_commons['LogTargetPath']
    else:
      self.log.info( 'LogFilePath parameter not found, creating on the fly' )
      result = getLogPath( self.workflow_commons, self.bkClient )
      if not result['OK']:
        self.log.error( 'Could not create LogFilePath', result['Message'] )
        return result
      self.logLFNPath = result['Value']['LogTargetPath'][0]

    if not isinstance( self.logLFNPath, str ):
      self.logLFNPath = self.logLFNPath[0]

######################################################################

  def execute( self, production_id = None, prod_job_id = None, wms_job_id = None,
               workflowStatus = None, stepStatus = None,
               wf_commons = None, step_commons = None,
               step_number = None, step_id = None ):
    """ Main executon method
    """

    try:

      super( UploadLogFile, self ).execute( self.version, production_id, prod_job_id, wms_job_id,
                                            workflowStatus, stepStatus,
                                            wf_commons, step_commons, step_number, step_id )

      self._resolveInputVariables()

      self.request.RequestName = 'job_%d_request.xml' % self.jobID
      self.request.JobID = self.jobID
      self.request.SourceComponent = "Job_%d" % self.jobID

      res = shellCall( 0, 'ls -al' )
      if res['OK'] and res['Value'][0] == 0:
        self.log.info( 'The contents of the working directory...' )
        self.log.info( str( res['Value'][1] ) )
      else:
        self.log.error( 'Failed to list the log directory', str( res['Value'][2] ) )

      self.log.info( 'Job root is found to be %s' % ( gConfig.getValue( '/LocalSite/Root', os.getcwd() ) ) )
      self.log.info( 'PRODUCTION_ID = %s, JOB_ID = %s ' % ( self.production_id, self.prod_job_id ) )
      self.logdir = os.path.realpath( './job/log/%s/%s' % ( self.production_id, self.prod_job_id ) )
      self.log.info( 'Selected log files will be temporarily stored in %s' % self.logdir )

      ##########################################
      # First determine the files which should be saved
      self.log.info( 'Determining the files to be saved in the logs.' )
      res = self._determineRelevantFiles()
      if not res['OK']:
        self.log.error( 'Completely failed to select relevant log files.', res['Message'] )
        return S_OK()
      selectedFiles = res['Value']
      self.log.info( 'The following %s files were selected to be saved:\n%s' % ( len( selectedFiles ),
                                                                                 '\n'.join( selectedFiles ) ) )

      #########################################
      # Create a temporary directory containing these files
      self.log.info( 'Populating a temporary directory for selected files.' )
      res = self.__populateLogDirectory( selectedFiles )
      if not res['OK']:
        self.log.error( 'Completely failed to populate temporary log file directory.', res['Message'] )
        self.setApplicationStatus( 'Failed To Populate Log Dir' )
        return S_OK()
      self.log.info( '%s populated with log files.' % self.logdir )

      #########################################
      # Create a tailored index page
      self.log.info( 'Creating an index page for the logs' )
      result = self.__createLogIndex( selectedFiles )
      if not result['OK']:
        self.log.error( 'Failed to create index page for logs', res['Message'] )

      #########################################
      # Make sure all the files in the log directory have the correct permissions
      result = self.__setLogFilePermissions( self.logdir )
      if not result['OK']:
        self.log.error( 'Could not set permissions of log files to 0755 with message:\n%s' % ( result['Message'] ) )


      # Instantiate the failover transfer client with the global request object
      if not self.failoverTransfer:
        self.failoverTransfer = FailoverTransfer( self.request )

      #########################################
      if not self._enableModule():
        self.log.info( "Would have attempted to upload log files, but there's not JobID" )
        return S_OK()

      # Attempt to uplaod logs to the LogSE
      self.log.info( 'Transferring log files to the %s' % self.logSE )
      res = S_ERROR()
      logURL = '<a href="http://lhcb-logs.cern.ch/storage%s">Log file directory</a>' % self.logFilePath
      self.log.info( 'Logs for this job may be retrieved from %s' % logURL )
      self.log.info( 'putDirectory %s %s %s' % ( self.logFilePath, os.path.realpath( self.logdir ), self.logSE ) )

      res = returnSingleResult( StorageElement( self.logSE ).putDirectory( {self.logFilePath:os.path.realpath( self.logdir )} ) )
      self.log.verbose( res )
      self.setJobParameter( 'Log URL', logURL )
      if res['OK']:
        self.log.info( 'Successfully upload log directory to %s' % self.logSE )
        # TODO: The logURL should be constructed using the LogSE and StorageElement()
        # FS: Tried, not available ATM in Dirac
        # storageElement = StorageElement(self.logSE)
        # pfn = storageElement.getPfnForLfn(self.logFilePath)['Value']
        # logURL = getPfnForProtocol(res['Value'],'http')['Value']

      else:
        self.log.error( "Failed to upload log files with message '%s', uploading to failover SE" % res['Message'] )
        # make a tar file
        tarFileName = os.path.basename( self.logLFNPath )
        try:
          res = tarFiles( tarFileName, selectedFiles, compression = 'gz' )
          if not res['OK']:
            self.log.error( 'Failed to create tar of log files: %s' % res['Message'] )
            self.setApplicationStatus( 'Failed to create tar of log files' )
            # We do not fail the job for this case
            return S_OK()
        except IOError:
          self.log.error( 'Failed to create tar of log files: %s' % res['Message'] )
          self.setApplicationStatus( 'Failed to create tar of log files' )
          # We do not fail the job for this case
          return S_OK()
        self._uploadLogToFailoverSE( tarFileName )

      self.workflow_commons['Request'] = self.request

      return S_OK( "Log Files uploaded" )

    except Exception as e:
      self.log.exception( e )
      self.setApplicationStatus( e )
      return S_ERROR( e )

    finally:
      super( UploadLogFile, self ).finalize( self.version )

  #############################################################################

  def _uploadLogToFailoverSE( self, tarFileName ):
    """  Recover the logs to a failover storage element
    """

    random.shuffle( self.failoverSEs )
    self.log.info( "Attempting to store file %s to the following SE(s):\n%s" % ( tarFileName,
                                                                                 ', '.join( self.failoverSEs ) ) )

    fileDict = { tarFileName: { 'lfn': self.logLFNPath,
                                'workflowSE': self.failoverSEs }}
    metadata = self.getFileMetadata( fileDict )
    fileMetaDict = { 'Size'         : metadata[tarFileName]['filedict']['Size'],
                     'LFN'          : metadata[tarFileName]['filedict']['LFN'],
                     'GUID'         : metadata[tarFileName]['filedict']['GUID'],
                     'Checksum'     : metadata[tarFileName]['filedict']['Checksum'],
                     'ChecksumType' : metadata[tarFileName]['filedict']['ChecksumType'] }

    result = self.failoverTransfer.transferAndRegisterFile( fileName = tarFileName,
                                                            localPath = '%s/%s' % ( os.getcwd(), tarFileName ),
                                                            lfn = self.logLFNPath,
                                                            destinationSEList = self.failoverSEs,
                                                            fileMetaDict = fileMetaDict,
                                                            masterCatalogOnly = True )

    if not result['OK']:
      self.log.error( "Failed to upload logs to all failover destinations (the job will not fail for this reason" )
      self.setApplicationStatus( 'Failed To Upload Logs' )
    else:
      uploadedSE = result['Value']['uploadedSE']
      self.log.info( "Uploaded logs to failover SE %s" % uploadedSE )

      self.request = self.failoverTransfer.request

      self.__createLogUploadRequest( self.logSE, self.logLFNPath, uploadedSE )
      self.log.info( "Successfully created failover request" )


  def _determineRelevantFiles( self ):
    """ The files which are below a configurable size will be stored in the logs.
        This will typically pick up everything in the working directory minus the output data files.
    """
    logFileExtensions = ['*.txt', '*.log', '*.out', '*.output',
                         '*.xml', '*.sh', '*.info', '*.err', 'prodConf*.py']  # '*.root',
    if self.logExtensions:
      self.log.info( 'Using list of log extensions from CS:\n%s' % ( ', '.join( self.logExtensions ) ) )
      logFileExtensions = self.logExtensions
    else:
      self.log.info( 'Using default list of log extensions:\n%s' % ( ', '.join( logFileExtensions ) ) )

    candidateFiles = []
    for ext in logFileExtensions:
      self.log.debug( 'Looking at log file wildcard: %s' % ext )
      globList = glob.glob( ext )
      for check in globList:
        if os.path.isfile( check ):
          self.log.debug( 'Found locally existing log file: %s' % check )
          candidateFiles.append( check )

    selectedFiles = []
    try:
      for candidate in candidateFiles:
        fileSize = os.stat( candidate )[6]
        if fileSize < self.logSizeLimit:
          selectedFiles.append( candidate )
        else:
          self.log.info( 'Log file found to be greater than maximum of %s bytes, compressing' % self.logSizeLimit )
          tarFileName = os.path.basename( candidate ) + '.gz'
          tarFiles( tarFileName, [candidate], compression = 'gz' )
          selectedFiles.append( tarFileName )
      return S_OK( selectedFiles )
    except OSError, x:
      self.log.exception( 'Exception while determining files to save.', '', str( x ) )
      return S_ERROR( 'Could not determine log files' )

  #############################################################################

  def __populateLogDirectory( self, selectedFiles ):
    """ A temporary directory is created for all the selected files.
        These files are then copied into this directory before being uploaded
    """
    # Create the temporary directory
    try:
      if not os.path.exists( self.logdir ):
        os.makedirs( self.logdir )
    except OSError, x:
      self.log.exception( 'Exception while trying to create directory.', self.logdir, str( x ) )
      return S_ERROR()
    # Set proper permissions
    self.log.info( 'Changing log directory permissions to 0755' )
    try:
      os.chmod( self.logdir, stat.S_IRWXU | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH + stat.S_IXOTH )
    except OSError, x:
      self.log.error( 'Could not set logdir permissions to 0755:', '%s (%s)' % ( self.logdir, str( x ) ) )
    # Populate the temporary directory
    try:
      for fileS in selectedFiles:
        destinationFile = '%s/%s' % ( self.logdir, os.path.basename( fileS ) )
        shutil.copy ( fileS, destinationFile )
    except shutil.Error:
      self.log.warn( 'scr and dst are the same' )
    except IOError, x:
      self.log.exception( 'Exception while trying to copy file.', fileS, str( x ) )
      self.log.info( 'File %s will be skipped and can be considered lost.' % fileS )

    # Now verify the contents of our target log dir
    successfulFiles = os.listdir( self.logdir )
    if len( successfulFiles ) == 0:
      self.log.info( 'Failed to copy any files to the target directory.' )
      return S_ERROR()
    else:
      self.log.info( 'Prepared %s files in the temporary directory.' % self.logdir )
      return S_OK()

  #############################################################################

  def __createLogUploadRequest( self, targetSE, logFileLFN, uploadedSE ):
    """ Set a request to upload job log files from the output sandbox
    """
    self.log.info( 'Setting log upload request for %s at %s' % ( logFileLFN, targetSE ) )

    logUpload = Operation()
    logUpload.Type = 'LogUpload'
    logUpload.TargetSE = targetSE

    logFile = File()
    logFile.LFN = logFileLFN

    logUpload.addFile( logFile )
    self.request.addOperation( logUpload )

    logRemoval = Operation()
    logRemoval.Type = 'RemoveFile'
    logRemoval.TargetSE = uploadedSE

    logRemoval.addFile( logFile )
    self.request.addOperation( logRemoval )

  #############################################################################

  def __setLogFilePermissions( self, logDir ):
    """ Sets the permissions of all the files in the log directory to ensure
        they are readable.
    """
    try:
      for toChange in os.listdir( logDir ):
        if not os.path.islink( '%s/%s' % ( logDir, toChange ) ):
          self.log.debug( 'Changing permissions of %s/%s to 0755' % ( logDir, toChange ) )
          os.chmod( '%s/%s' % ( logDir, toChange ), stat.S_IRWXU | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH + stat.S_IXOTH )
    except OSError, x:
      self.log.error( 'Problem changing shared area permissions', str( x ) )
      return S_ERROR( x )

    return S_OK()

  #############################################################################

  def __createLogIndex( self, selectedFiles ):
    """ Create a log index page for browsing the log files.
    """
    productionID = self.production_id
    prodJobID = self.prod_job_id
    wmsJobID = str( self.jobID )

    targetFile = '%s/index.html' % ( self.logdir )
    fopen = open( targetFile, 'w' )
    fopen.write( """
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN">
<html>
<head>\n""" )
    fopen.write( "<title>Logs for Job %s of Production %s (DIRAC WMS ID %s)</title>\n" % ( prodJobID,
                                                                                           productionID,
                                                                                           wmsJobID ) )
    fopen.write( """</head>
<body text="#000000" bgcolor="#33ffff" link="#000099" vlink="#990099"
 alink="#000099"> \n
""" )
    fopen.write( """<IMG SRC="%s" ALT="DIRAC" WIDTH="300" HEIGHT="120" ALIGN="right" BORDER="0">
<br>
""" % self.diracLogo )
    fopen.write( "<h3>Log files for  Job %s_%s </h3> \n<br>" % ( productionID, prodJobID ) )
    for fileName in selectedFiles:
      fopen.write( '<a href="%s">%s</a><br> \n' % ( fileName, fileName ) )

    fopen.write( "<p>Job %s_%s corresponds to WMS JobID %s executed at %s.</p><br>" % ( productionID,
                                                                                        prodJobID,
                                                                                        wmsJobID,
                                                                                        self.siteName ) )
    fopen.write( "<h3>Parameter summary for job %s_%s</h3> \n" % ( prodJobID, productionID ) )
    check = ['BannedSites', 'JobType', 'CPUTime', 'ProductionOutputData', 'LogFilePath', 'InputData', 'InputSandbox']
    params = {}
    for name, val in self.workflow_commons.items():
      for item in check:
        if name == item and val:
          params[name] = str( val )

    finalKeys = params.keys()
    finalKeys.sort()
    rows = ''
    for k in finalKeys:
      rows += """

<tr>
<td> %s </td>
<td> %s </td>
</tr>
      """ % ( k, params[k] )

    table = """<table border="1" bordercolor="#000000" width="50%" bgcolor="#BCCDFE">
<tr>
<td>Parameter Name</td>
<td>Parameter Value</td>
</tr>""" + rows + """
</table>
"""
    fopen.write( table )
    fopen.write( """</body>
</html>""" )
    fopen.close()
    return S_OK()

# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
