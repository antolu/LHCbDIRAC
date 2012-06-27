# $HeadURL$
''' LHCb SAM Job Finalization Module

  Removes the lock on the shared area (assuming the lock was placed
  there during the LockSharedArea test). Publishes test results to
  SAM DB and creates summary web page.  Uploads the logs to the LogSE.
'''

import glob
import os
import re
import shutil
import sys
import time

import DIRAC

from DIRAC                                               import S_OK, S_ERROR, gLogger, gConfig
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.Core.Utilities.Subprocess                     import pythonCall
from DIRAC.DataManagementSystem.Client.ReplicaManager    import ReplicaManager

import LHCbDIRAC
from LHCbDIRAC.SAMSystem.Modules.ModuleBaseSAM              import ModuleBaseSAM
from LHCbDIRAC.Core.Utilities.CombinedSoftwareInstallation  import SharedArea

__RCSID__ = '$Id$'

SAM_TEST_NAME = 'CE-lhcb-sam-publish'
SAM_LOG_FILE  = 'sam-publish.log'
SAM_LOCK_NAME = 'DIRAC-SAM-Test-Lock'

class SAMFinalization( ModuleBaseSAM ):
  """ SAM Finalization class """

  def __init__( self ):
    """ Standard constructor for SAM Module
    """
    ModuleBaseSAM.__init__( self )
    self.version     = __RCSID__
    self.runinfo     = {}
    self.logFile     = SAM_LOG_FILE
    self.testName    = SAM_TEST_NAME
    self.lockFile    = SAM_LOCK_NAME
    self.diracSetup  = gConfig.getValue( '/DIRAC/Setup', 'None' )
    self.log         = gLogger.getSubLogger( "SAMFinalization" )
    self.result      = S_ERROR()
    self.opsH        = Operations()
    self.diracLogo   = self.opsH.getValue( 'SAM/LogoURL', 
                                           'https://lhcbweb.pic.es/DIRAC/images/logos/DIRAC-logo-transp.png' )
    self.samVO       = self.opsH.getValue( 'SAM/VO', 'lhcb' )
    self.samLogFiles = self.opsH.getValue( 'SAM/LogFiles', [] )
    self.logURL      = 'http://lhcb-logs.cern.ch/storage/'
    self.siteRoot    = os.path.dirname( os.path.realpath( LHCbDIRAC.__path__[0] ) )
#    self.samPublishClient = '%s/LHCbDIRAC/SAMSystem/Distribution/sam.tar.gz' % ( self.siteRoot )
    self.samPublishClient = '%s/LHCbDIRAC/SAMSystem/Distribution/stomp.zip' % ( self.siteRoot )
#    self.samPublishScript = 'sam/bin/same-publish-tuples'
    self.logSE = 'LogSE'
    self.jobID = None
    if os.environ.has_key( 'JOBID' ):
      self.jobID = os.environ['JOBID']

    #Workflow parameters for the test
    self.enable             = True
    self.publishResultsFlag = False
    self.uploadLogsFlag     = False

  def _resolveInputVariables( self ):
    """ By convention the workflow parameters are resolved here.
    """
    if self.step_commons.has_key( 'enable' ):
      self.enable = self.step_commons['enable']
      if not type( self.enable ) == type( True ):
        self.log.warn( 'Enable flag set to non-boolean value %s, setting to False' % self.enable )
        self.enable = False

    if self.step_commons.has_key( 'publishResultsFlag' ):
      self.publishResultsFlag = self.step_commons['publishResultsFlag']
      if not type( self.publishResultsFlag ) == type( True ):
        self.log.warn( 'Publish results flag set to non-boolean value %s, setting to False' % self.publishResultsFlag )
        self.enable = False

    if self.step_commons.has_key( 'uploadLogsFlag' ):
      self.uploadLogsFlag = self.step_commons['uploadLogsFlag']
      if not type( self.uploadLogsFlag ) == type( True ):
        self.log.warn( 'Upload logs flag set to non-boolean value %s, setting to False' % self.uploadLogsFlag )
        self.enable = False

    self.log.verbose( 'Enable flag is set to %s' % self.enable )
    self.log.verbose( 'Publish results flag set to %s' % self.publishResultsFlag )
    self.log.verbose( 'Upload logs flag set to %s' % self.uploadLogsFlag )
    return S_OK()

  def execute( self ):
    """The main execution method of the SAMFinalization module.
    """
    self.log.info( 'Initializing ' + self.version )
    self._resolveInputVariables()
    self.runinfo = self.getRunInfo()
    sharedArea = SharedArea()
    if not sharedArea or not os.path.exists( sharedArea ):
      self.log.info( 'Could not determine sharedArea for site %s:\n%s' % ( DIRAC.siteName(), sharedArea ) )
      return S_ERROR( 'Could not determine shared area for site' )
    else:
      self.log.info( 'Software shared area for site %s is %s' % ( DIRAC.siteName(), sharedArea ) )

    failed = False
    if not self.workflowStatus['OK'] or not self.stepStatus['OK']:
      self.log.warn( 'A critical error was detected in a previous step, will attempt to publish SAM results.' )
      failed = True

    self.log.verbose( self.workflow_commons )
    if not self.workflow_commons.has_key( 'SAMResults' ):
      self.setApplicationStatus( 'No SAM Results Found' )
      return self.finalize( 'No SAM results found', 'Status ERROR (= 50)', 'error' )

    samResults = self.workflow_commons['SAMResults']
    #If the lock test has failed or is not OK, another job is running and the lock should not be removed
    if samResults.has_key( 'CE-lhcb-lock' ):
      if not int( samResults['CE-lhcb-lock'] ) > int( self.samStatus['info'] ):
        result = self.__removeLockFile( sharedArea )
        if not result['OK']:
          self.setApplicationStatus( 'Could Not Remove Lock File' )
          self.writeToLog( 'Failed to remove lock file with result:\n%s' % result )
          failed = True
      else:
        self.log.info( 'Another SAM job is running, leaving SAM lock file in shared area' )
        self.writeToLog( 'Another SAM job is running, leaving SAM lock file in shared area' )

    if not failed:
      self.setApplicationStatus( 'Starting %s Test' % self.testName )


    samNode = self.runinfo['CE']
    samLogs = {}
    if not self.workflow_commons.has_key( 'SAMLogs' ):
      self.setApplicationStatus( 'No SAM Logs Found' )
      self.log.warn( 'No SAM logs found' )
    else:
      samLogs = self.workflow_commons['SAMLogs']

    result = self.__publishSAMResultsNagios( samNode, samResults, samLogs )
    if not result['OK']:
      self.setApplicationStatus( 'SAM Reporting Error' )
      return self.finalize( 'Failure while publishing SAM results', result['Message'], 'error' )

    result = self.__uploadSAMLogs( samNode )
    if not result['OK']:
      self.setApplicationStatus( 'SAM Logs Not Uploaded' )
      return self.finalize( 'Failure while uploading SAM logs', result['Message'], 'error' )

    self.log.info( 'Test %s completed successfully' % self.testName )
    if not failed:
      self.finalize( '%s Test Successful' % self.testName, 'Status Success (= 10)', 'ok' )
      self.setApplicationStatus( 'SAM Job Successful' )
      return S_OK()
    else:
      return self.finalize( 'Failure During Execution', 'Status Error (= 50)', 'error' )

  def __removeLockFile( self, sharedArea ):
    """Method to remove the lock placed in the shared area if the earlier test was
       successful.
    """
    self.log.info( 'Checking SAM lock file: %s' % self.lockFile )

    #nasty fix but only way to resolve writeable volume at CERN
    if DIRAC.siteName() == 'LCG.CERN.ch' or DIRAC.siteName() == 'LCG.CERN5.ch':
      self.log.info( 'Changing shared area path to writeable volume at CERN' )
      if re.search( '.cern.ch', sharedArea ):
        newSharedArea = sharedArea.replace( 'cern.ch', '.cern.ch' )
        self.writeToLog( 'Changing path to shared area writeable volume at LCG.CERN.ch:\n%s => %s' % ( sharedArea, newSharedArea ) )
        sharedArea = newSharedArea

    if os.path.exists( '%s/%s' % ( sharedArea, self.lockFile ) ):
      self.log.info( 'Found SAM lock on the shared area at %s' % sharedArea )
      cmd = 'rm -fv %s/%s' % ( sharedArea, self.lockFile )
      result = self.runCommand( 'Current lock file will be removed', cmd )
      self.setJobParameter( 'SAMLock', 'Removed on %s' % ( time.asctime() ) )
      if not result['OK']:
        self.log.warn( result['Message'] )
        return S_ERROR( 'Could not remove %s' % self.lockFile )
    else:
      self.log.info( 'SAM lock file is not present in the site shared area' )

    return S_OK( 'Lock removed' )

  def __getSAMStatus( self, testStatus ):
    """ Returns SAM status string for test summary.
    """
    testStatus = str( testStatus )
    statusString = ''
    for status, localid in self.samStatus.items():
      if localid == testStatus:
        statusString = status
    return statusString

  def __getSoftwareReport( self, lfnPath, samResults, samLogs ):
    """Returns the list of software installed at the site organized by platform.
       If the test status is not successful, returns a link to the install test
       log.  Creates an html table for the results.
    """
    logURL = '%s%s/' % ( self.logURL, lfnPath )
    failedMessages = []
    for testName, testStatus in samResults.items():
      if samLogs.has_key( testName ):
        logName = samLogs[testName]
        if int( testStatus ) > int( self.samStatus['notice'] ):
          self.log.warn( '%s test status was %s, writing message to check %s test' % ( testName, testStatus, testName ) )
          failedMessages.append( '<LI>%s test status was %s = %s, please check <A HREF="%s%s">%s test log</A> for more details<br>' % ( testName, self.__getSAMStatus( testStatus ), testStatus, logURL, logName, testName ) )
        else:
          self.log.verbose( 'Test %s completed with status %s' % ( testName, testStatus ) )
      else:
        self.log.info( '%s Test completed with %s but no SAM log file available in workflow commons' % ( testName, testStatus ) )

    if failedMessages:
      failedLogs = """<UL><br>
"""
      for msg in failedMessages:
        failedLogs += msg + """
"""
      failedLogs += """
</UL><br>
"""
      return failedLogs

    #If software installation test was not run by this job e.g. is 'notice' status, do not add software report.
    if not samResults.has_key( 'CE-lhcb-install' ):
      self.log.info( 'Software install test was not run by this job.' )
      return 'Software installation test was disabled for this job (safe mode).'

    if int( samResults['CE-lhcb-install'] ) > int( self.samStatus['ok'] ):
      self.log.info( 'Software install test was not run by this job.' )
      return 'Software installation test was disabled for this job by the SAM lock file.'

    activeSw = self.opsH.getValue( 'SoftwareDistribution/Active', [] )
    self.log.debug( 'Active software is: %s' % ( ', '.join( activeSw ) ) )
    arch = gConfig.getValue( '/LocalSite/Architecture' )
    self.log.verbose( 'Current node system configuration is: %s' % ( arch ) )
    architectures = gConfig.getValue( '/Resources/Computing/OSCompatibility/%s' % arch, [] )
    self.log.verbose( 'Compatible system configurations are: %s' % ( ', '.join( architectures ) ) )
    softwareDict = {}
    for architecture in architectures:
      archSw = self.opsH.getValue( 'SoftwareDistribution/%s' % ( architecture ), [] )
      for sw in activeSw:
        if sw in archSw:
          if softwareDict.has_key( sw ):
            current = softwareDict[sw]
            current.append( architecture )
            softwareDict[sw] = current
          else:
            softwareDict[sw] = [architecture]

    self.log.verbose( softwareDict )
    rows = """
    <br><br><br>
    Software summary from job running on node with system configuration %s:
    <br><br><br>
    """ % ( arch )
    sortedKeys = softwareDict.keys()
    sortedKeys.sort()
    for appNameVersion in sortedKeys:
      archList = softwareDict[appNameVersion]
      name = appNameVersion.split( '.' )[0]
      version = appNameVersion.split( '.' )[1]
      sysConfigs = ', '.join( archList )
      rows += """

<tr>
<td> %s </td>
<td> %s </td>
<td> %s </td>
</tr>
      """ % ( name, version, sysConfigs )

    self.log.debug( rows )

    table = """<table border="1" bordercolor="#000000" width="50%" bgcolor="#BCCDFE">
<tr>
<td>Project Name</td>
<td>Project Version</td>
<td>System Configurations</td>
</tr>""" + rows + """
</table>
"""
    self.log.debug( table )
    return table

  def __getTestSummary( self, lfnPath, samResults, samLogs ):
    """Returns a test summary as an html table.
    """
    logURL = '%s%s/' % ( self.logURL, lfnPath )
    self.log.debug( samResults )
    rows = """
"""
    for testName, testStatus in samResults.items():
      rows += """

<tr>
<td> %s </td>
<td> %s </td>
<td> %s </td>
</tr>
      """ % ( '<A HREF="%s%s">%s</A>' % ( logURL, samLogs[testName], testName ), self.__getSAMStatus( testStatus ), testStatus )

    self.log.debug( str( rows ) )
    table = """
<table border="1" bordercolor="#000000" width="50%" bgcolor="#B8FDC2">
<tr>
<td>Test Name</td>
<td>Test Status</td>
<td>SAM Status</td>
</tr>""" + rows + """
</table>
"""
    self.log.debug( table )
    return table

  def __publishSAMResultsNagios( self, samNode, samResults, samLogs ):
    """Prepares SAM publishing files and reports results to the SAM DB.
    """

    self.log.info( 'Preparing SAM publishing files' )
    if not self.jobID:
      self.log.verbose( 'No jobID defined, setting to 12345 and samNode to test.node for test purposes' )
      self.jobID = 12345
      samNode = 'test.node'


    lfnPath = self.__getLFNPathString( samNode )
    testSummary = self.__getTestSummary( lfnPath, samResults, samLogs )
    softwareReport = self.__getSoftwareReport( lfnPath, samResults, samLogs )

    reports = []
    for testName, testStatus in samResults.items():

      testDetails = """
<br>
<IMG SRC="%s" ALT="DIRAC" WIDTH="300" HEIGHT="120" ALIGN="left" BORDER="0">
<br><br>
<br>DIRAC Site Name %s ( CE = %s )<br>
<br>Corresponding to JobID %s in DIRAC setup %s<br>
<br>Test Summary %s:<br>
<br>
%s
<br>
<br><br><br>
Link to log files: <br>
<UL><br>
<LI><A HREF='%s%s'>Log SE output</A><br>
<LI><A HREF='%s%s/test/sam/%s'>Previous tests for %s</A><br>
<LI><A HREF='%s%s/test/sam'>LHCb SAM Logs CE Directory</A><br>
</UL><br>
<br>
%s
<br>
EOT
""" % ( self.diracLogo, DIRAC.siteName(), samNode, self.jobID, self.diracSetup, time.strftime( '%Y-%m-%d' ), testSummary, self.logURL, lfnPath, self.logURL, self.samVO, samNode, samNode, self.logURL, self.samVO, softwareReport )

      report = {}
      report['testName'] = testName
      report['testStatus'] = testStatus
      report['testSummary'] = "%s %s" % ( testName, testStatus )
      report['testDetails'] = testDetails
      reports.append( report )

    sys.path.append( self.samPublishClient + "/stomp" )

    res = pythonCall( 600, self.__publisherNagios, samNode, reports )

    if not res['OK']:
      return res

    return S_OK()

  def __getLFNPathString( self, samNode ):
    """Returns LFN path string according to SAM convention.
    """
    date = time.strftime( '%Y-%m-%d' )
    return '/%s/test/sam/%s/%s/%s' % ( self.samVO, samNode, date, self.jobID )

  def __uploadSAMLogs( self, samNode ):
    """Saves SAM log files to the log SE.
    """
    self.log.info( 'Saving SAM log files with extension: %s' % ( ', '.join( self.samLogFiles ) ) )
    logFiles = []
    for extnType in self.samLogFiles:
      globList = glob.glob( extnType )
      for check in globList:
        if os.path.isfile( check ):
          logFiles.append( check )

    logDir = '%s/%s' % ( os.getcwd(), 'log' )
    self.log.verbose( 'Creating log directory %s' % logDir )
    try:
      os.mkdir( logDir )
    except SystemError:
      return S_ERROR( 'Could not create log directory %s' % logDir )

    self.log.info( 'Saving log files: %s' % ( ', '.join( logFiles ) ) )
    for toSave in logFiles:
      shutil.copy( toSave, logDir )

    if not self.enable or not self.uploadLogsFlag:
      self.log.info( 'Log file upload is disabled via testing flag' )
      return S_OK()

    lfnPath = self.__getLFNPathString( samNode )
    rm = ReplicaManager()
    self.log.verbose( 'Arguments for rm.putDirectory are: %s\n%s\n%s' % ( lfnPath, os.path.realpath( logDir ), self.logSE ) )
    result = rm.putStorageDirectory( {lfnPath:os.path.realpath( logDir )}, self.logSE, singleDirectory = True )
    self.log.verbose( result )
    if not result['OK']:
      return result
    logReference = '<a href="%s%s">Log file directory</a>' % ( self.logURL, lfnPath )
    self.log.verbose( 'Adding Log URL job parameter: %s' % logReference )
    self.setJobParameter( 'Log URL', logReference )
    return S_OK()

  def __getSAMClient( self ):
    """Locates the shipped SAM client tarball and unpacks it in the job working
       directory if the publishing and enable flags are set to True.
    """
    if not os.path.exists( self.samPublishClient ):
      return S_ERROR( '%s does not exist' % ( self.samPublishClient ) )
    shutil.copy( self.samPublishClient, os.getcwd() )
    #Untar not using tarfile so this is added to the sam logs
    #SAM -> LCG -> Linux
    return self.runCommand( 'Obtaining SAM client', 'tar -zxvf %s' % ( os.path.basename( self.samPublishClient ) ) )

  def __publisherNagios( self, ce, reports ):
    """
    Publish information to Nagious:

    hostName:           CE name for example ce06.pic.es
    metricName:         org.lhcb.WN-<test_name>-lhcb
    metricStatus:       0 is OK, 1 is WARNING, 2 is ERROR
    timeStamp:          2010-12-07T16:02:06Z (<-- please stick to this format)
    summaryData:        Few lines summarizing the test
    detailsData:        Free format text (this could be a link to the logSE if you prefer)
    """

    import stomp
    self.log.verbose ( "Nagios" )
    host = 'egi.msg.cern.ch'
    port = 6163
    queue = '/queue/grid.probe.metricOutput.EGEE.sam-lhcb_cern_ch'
    try:
      conn = stomp.Connection( [( host, port )] )
      conn.start()
      conn.connect()
    except Exception:
      return S_ERROR( 'Exception on connect to %s %d' % ( host, port ) )

    timeStamp = time.strftime( "%Y-%m-%dT%H:%M:%S", time.gmtime() )

    for report in reports:

      testStatus = int( report['testStatus'] )

      metricStatus = 0
      if 0 < testStatus < 50:
        metricStatus = 1
      if testStatus >= 50:
        metricStatus = 2

      message = ""
      message += "hostName: %s\n" % ce
      message += "metricName: org.lhcb.WN-%s-lhcb\n" % report['testName']
      message += "metricStatus: %d\n" % metricStatus
      message += "timestamp: %s\n" % timeStamp
      message += "summaryData: %s\n" % report['testSummary']
      message += "detailsData: %s\n" % report['testDetails']
      message += "EOT\n"

      try:
        conn.send( message, destination = queue, ack = 'auto', persistent = 'true' )
      except Exception:
        return S_ERROR( 'Exception on send to %s %d %s' % ( host, port, queue ) )

    try:
      conn.disconnect()
    except Exception:
      return S_ERROR( 'Exception on disconnect to %s %d ' % ( host, port ) )

    return S_OK()

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF