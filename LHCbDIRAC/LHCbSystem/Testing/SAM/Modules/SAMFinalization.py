########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/LHCbSystem/Testing/SAM/Modules/SAMFinalization.py,v 1.7 2008/07/23 17:38:39 paterson Exp $
# Author : Stuart Paterson
########################################################################

""" LHCb SAM Job Finalization Module

    Removes the lock on the shared area (assuming the lock was placed
    there during the LockSharedArea test). Publishes test results to
    SAM DB and creates summary web page.  Uploads the logs to the LogSE.

"""

__RCSID__ = "$Id: SAMFinalization.py,v 1.7 2008/07/23 17:38:39 paterson Exp $"

from DIRAC import S_OK, S_ERROR, gLogger, gConfig
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager
try:
  from DIRAC.LHCbSystem.Utilities.CombinedSoftwareInstallation  import SharedArea
  from DIRAC.LHCbSystem.Testing.SAM.Modules.ModuleBaseSAM import *
except Exception,x:
  from LHCbSystem.Utilities.CombinedSoftwareInstallation  import SharedArea
  from LHCbSystem.Testing.SAM.Modules.ModuleBaseSAM import *

import string, os, sys, re, glob, shutil

SAM_TEST_NAME='CE-lhcb-sam-publish'
SAM_LOG_FILE='sam-publish.log'
SAM_LOCK_NAME='DIRAC-SAM-Test-Lock'

class SAMFinalization(ModuleBaseSAM):

  #############################################################################
  def __init__(self):
    """ Standard constructor for SAM Module
    """
    ModuleBaseSAM.__init__(self)
    self.version = __RCSID__
    self.logFile = SAM_LOG_FILE
    self.testName = SAM_TEST_NAME
    self.lockFile = SAM_LOCK_NAME
    self.site = gConfig.getValue('/LocalSite/Site','LCG.Unknown.ch')
    self.log = gLogger.getSubLogger( "SAMFinalization" )
    self.result = S_ERROR()
    self.diracLogo = gConfig.getValue('/Operations/SAM/LogoURL','https://lhcbweb.pic.es/DIRAC/images/logos/DIRAC-logo-transp.png')
    self.samVO = gConfig.getValue('/Operations/SAM/VO','lhcb')
    self.samLogFiles = gConfig.getValue('/Operations/SAM/LogFiles',[])
    self.logURL = 'http://lhcb-logs.cern.ch/storage/'
    self.siteRoot = gConfig.getValue('LocalSite/Root','')
    self.samPublishClient = '%s/DIRAC/LHCbSystem/Testing/SAM/Distribution/sam.tar.gz' %(self.siteRoot)
    self.samPublishScript = 'sam/bin/same-publish-tuples'
    self.logSE = 'LogSE'
    self.jobID = None
    if os.environ.has_key('JOBID'):
      self.jobID = os.environ['JOBID']

    #Workflow parameters for the test
    self.enable = True
    self.publishResultsFlag = False
    self.uploadLogsFlag = False

  #############################################################################
  def resolveInputVariables(self):
    """ By convention the workflow parameters are resolved here.
    """
    if self.step_commons.has_key('enable'):
      self.enable=self.step_commons['enable']
      if not type(self.enable)==type(True):
        self.log.warn('Enable flag set to non-boolean value %s, setting to False' %self.enable)
        self.enable=False

    if self.step_commons.has_key('publishResultsFlag'):
      self.publishResultsFlag=self.step_commons['publishResultsFlag']
      if not type(self.publishResultsFlag)==type(True):
        self.log.warn('Publish results flag set to non-boolean value %s, setting to False' %self.publishResultsFlag)
        self.enable=False

    if self.step_commons.has_key('uploadLogsFlag'):
      self.uploadLogsFlag=self.step_commons['uploadLogsFlag']
      if not type(self.uploadLogsFlag)==type(True):
        self.log.warn('Upload logs flag set to non-boolean value %s, setting to False' %self.uploadLogsFlag)
        self.enable=False

    self.log.verbose('Enable flag is set to %s' %self.enable)
    self.log.verbose('Publish results flag set to %s' %self.publishResultsFlag)
    self.log.verbose('Upload logs flag set to %s' %self.uploadLogsFlag)
    return S_OK()

  #############################################################################
  def execute(self):
    """The main execution method of the SAMFinalization module.
    """
    self.log.info('Initializing '+self.version)
    self.resolveInputVariables()

    if not self.workflowStatus['OK'] or not self.stepStatus['OK']:
      self.log.warn('A critical error was detected in a previous step, exiting.')
      return self.finalize('Stopping execution of SAM Finalization','Workflow / Step Failure','critical')

    self.setApplicationStatus('Starting %s Test' %self.testName)
    sharedArea = SharedArea()
    if not sharedArea or not os.path.exists(sharedArea):
      self.log.info('Could not determine sharedArea for site %s:\n%s' %(self.site,sharedArea))
      return S_ERROR('Could not determine shared area for site')
    else:
      self.log.info('Software shared area for site %s is %s' %(self.site,sharedArea))

    result = self.__removeLockFile(sharedArea)
    if not result['OK']:
      self.setApplicationStatus('Could Not Remove Lock File')
      return self.finalize('Failed to remove lock file','Status ERROR (= 50)','error')

    result = gConfig.getValue('/LocalSite/GridCE')
    if not result['OK']:
      return self.finalize('Could not get current CE',result['Message'],'error')
    ceOutput = result['Value']
    self.log.info('Current CE is %s' %ceOutput)
    samNode = ceOutput

    self.log.verbose(self.workflow_commons)
    if not self.workflow_commons.has_key('SAMResults'):
      return self.finalize('No SAM results found','Status ERROR (= 50)','error')

    samResults = self.workflow_commons['SAMResults']
    result = self.__publishSAMResults(samNode,samResults)
    if not result['OK']:
      self.setApplicationStatus('SAM Reporting Error')
      return self.finalize('Failure while publishing SAM results',result['Message'],'error')

    result = self.__uploadSAMLogs(samNode)
    if not result['OK']:
      self.setApplicationStatus('SAM Logs Not Uploaded')
      return self.finalize('Failure while uploading SAM logs',result['Message'],'error')

    self.log.info('Test %s completed successfully' %self.testName)
    self.setApplicationStatus('SAM Job Successful')
    return self.finalize('%s Test Successful' %self.testName,'Status OK (= 10)','ok')

  #############################################################################
  def __removeLockFile(self,sharedArea):
    """Method to remove the lock placed in the shared area if the earlier test was
       successful.
    """
    self.log.info('Checking SAM lock file: %s' %self.lockFile)
    if os.path.exists('%s/%s' %(sharedArea,self.lockFile)):
      self.log.info('Found SAM lock on the shared area at %s' %sharedArea)
      cmd = 'rm -fv %s/%s' %(sharedArea,self.lockFile)
      result = self.runCommand('Current lock file will be removed',cmd)
      self.setJobParameter('SAMLock','Removed on %s' %(time.asctime()))
      if not result['OK']:
        self.log.warn(result['Message'])
        return S_ERROR('Could not remove %s' %self.lockFile)
    else:
      self.log.info('SAM lock file is not present in the site shared area')

    return S_OK('Lock removed')

  #############################################################################
  def __publishSAMResults(self,samNode,samResults):
    """Prepares SAM publishing files and reports results to the SAM DB.
    """
    self.log.info('Preparing SAM publishing files')
    if not self.jobID:
      self.log.verbose('No jobID defined, setting to 12345 and samNode to test.node for test purposes')
      self.jobID = 12345
      samNode = 'test.node'

    #Get SAM client
    self.log.info('Publish Flag: %s, Enable Flag: %s' %(self.publishResultsFlag,self.enable))
    if self.publishResultsFlag and self.enable:
      result = self.__getSAMClient()
      if not result['OK']:
        self.setJobParameter(testName,'Failed to locate SAM client with message %s' %(result['Message']))
        return result

    lfnPath = self.__getLFNPathString(samNode)
    testSummary = ''
    for testName,testStatus in samResults.items():
      testSummary += '<br> %s = %s' %(testName,testStatus)
    counter = 0
    publishFlag = True
    for testName,testStatus in samResults.items():
      counter += 1
      defFile = """testName: %s
testAbbr: LHCb %s
testTitle: LHCb SAM %s
EOT
""" %(testName,testName,testName)

      envFile = """envName: CE-%s%s
name: LHCbSAMTest
value: OK
""" %(self.jobID,counter)

      resultFile = """nodename: %s
testname: %s
envName: CE-%s%s
voname: lhcb
status: %s
detaileddata: EOT
<br>
<IMG SRC="%s" ALT="DIRAC" WIDTH="300" HEIGHT="120" ALIGN="left" BORDER="0">
<br><br><br><br><br><br><br>
<br>DIRAC Site %s ( CE = %s )<br>
<br>Test Summary %s:<br>
<br> %s <br>
<br><br><br>
Link to log files: <br>
<UL><br>
<LI><A HREF='%s%s'>Log SE output</A><br>
<LI><A HREF='%s%s/test/sam/%s'>Previous tests for %s</A><br>
</UL><br>

A summary of the SAM status codes is:
<UL><br>
<LI>ok=10<br>
<LI>info=20<br>
<LI>notice=30<br>
<LI>warning=40<br>
<LI>error=50<br>
<LI>critical=60<br>
<LI>maintenance=100<br>
</UL><br>
The LHCb SAM log files CE directory is <A HREF='%s%s/test/sam'>here</A>.<br>
<br>
EOT
""" %(samNode,testName,self.jobID,counter,testStatus,self.diracLogo,self.site,samNode,time.strftime('%Y-%m-%d'),testSummary,self.logURL,lfnPath,self.logURL,self.samVO,samNode,samNode,self.logURL,self.samVO)

      files = {}
      files[defFile]='.def'
      files[resultFile]='.result'
      files[envFile]='.env'

      for contents,ext in files.items():
        fopen = open(testName+ext,'w')
        fopen.write(contents)
        fopen.close()

      testDict = {'TestDef':'.def','TestData':'.result','TestEnvVars':'.env'}
      if not self.publishResultsFlag:
        self.log.info('Publish flag is disabled for %s %s' %(testName,testStatus))
      if self.publishResultsFlag and self.enable:
        self.log.info('Attempting to publish results for %s to SAM DB' %(testName))
        samHome = '%s/sam' %(os.getcwd())
        samPublish = '%s/%s' %(os.getcwd(),self.samPublishScript)
        os.environ['SAME_HOME'] = samHome
        sys.path.insert(0,samHome)
        if not os.path.exists(samPublish):
          return S_ERROR('%s does not exist' %(samPublish))
        for testTitle,testExtn in testDict.items():
          testFile = '%s%s' %(testName,testExtn)
          cmd = '%s %s %s' %(samPublish,testTitle,testFile)
          result = self.runCommand('Publishing %s %s' %(testName,testFile),cmd)
          if not result['OK']:
            self.setJobParameter(testName,'Publishing %s %s failed' %(testName,testFile))
            publishFlag = False

    if not publishFlag:
      return S_ERROR('Publishing of some results failed')

    return S_OK()

  #############################################################################
  def __getLFNPathString(self,samNode):
    """Returns LFN path string according to SAM convention.
    """
    date = time.strftime('%Y-%m-%d')
    return '/%s/test/sam/%s/%s/%s/log' %(self.samVO,samNode,date,self.jobID)

  #############################################################################
  def __uploadSAMLogs(self,samNode):
    """Saves SAM log files to the log SE.
    """
    self.log.info('Saving SAM log files with extension: %s' %(string.join(self.samLogFiles,', ')))
    logFiles = []
    for extnType in self.samLogFiles:
      globList = glob.glob(extnType)
      for check in globList:
        if os.path.isfile(check):
          logFiles.append(check)

    logDir = '%s/%s' %(os.getcwd(),'log')
    self.log.verbose('Creating log directory %s' %logDir)
    try:
      os.mkdir(logDir)
    except Exception, x:
      return S_ERROR('Could not create log directory %s' %logDir)

    self.log.info('Saving log files: %s' %(string.join(logFiles,', ')))
    for toSave in logFiles:
      shutil.copy(toSave,logDir)

    if not self.enable or not self.uploadLogsFlag:
      self.log.info('Log file upload is disabled via testing flag')
      return S_OK()

    lfnPath = self.__getLFNPathString(samNode)
    rm = ReplicaManager()
    result = rm.putDirectory(lfnPath,logDir,self.logSE)
    self.log.verbose(result)
    if not result['OK']:
      return result
    logReference = '<a href="%s%s">Log file directory</a>' % (self.logURL,lfnPath)
    self.log.verbose('Adding Log URL job parameter: %s' %logReference)
    self.setJobParameter('Log URL',logReference)
    return S_OK()

  #############################################################################
  def __getSAMClient(self):
    """Locates the shipped SAM client tarball and unpacks it in the job working
       directory if the publishing and enable flags are set to True.
    """
    if not os.path.exists(self.samPublishClient):
      return S_ERROR('%s does not exist' %(self.samPublishClient))
    shutil.copy(self.samPublishClient,os.getcwd())
    #Untar not using tarfile so this is added to the sam logs
    #SAM -> LCG -> Linux
    return self.runCommand('tar -zxvf %s' %(os.path.basename(self.samPublishClient)))

  #EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#