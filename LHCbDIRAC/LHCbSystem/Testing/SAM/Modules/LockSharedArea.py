########################################################################
# $HeadURL$
# Author : Stuart Paterson
########################################################################

""" LHCb LockSharedArea SAM Test Module
"""

__RCSID__ = "$Id$"

import DIRAC
from DIRAC import S_OK, S_ERROR, gLogger, gConfig
from DIRAC.Core.DISET.RPCClient import RPCClient
from LHCbDIRAC.LHCbSystem.Utilities.CombinedSoftwareInstallation  import SharedArea, CreateSharedArea
from LHCbDIRAC.LHCbSystem.Testing.SAM.Modules.ModuleBaseSAM import *

import string, os, sys, re, time

SAM_TEST_NAME='CE-lhcb-lock'
SAM_LOG_FILE='sam-lock.log'
SAM_LOCK_NAME='DIRAC-SAM-Test-Lock'

class LockSharedArea(ModuleBaseSAM):

  #############################################################################
  def __init__(self):
    """ Standard constructor for SAM Module
    """
    ModuleBaseSAM.__init__(self)
    self.version = __RCSID__
    self.runinfo = {}
    self.logFile = SAM_LOG_FILE
    self.testName = SAM_TEST_NAME
    self.lockFile = SAM_LOCK_NAME
    self.log = gLogger.getSubLogger( "LockSharedArea" )
    self.result = S_ERROR()

    self.jobID = None
    if os.environ.has_key('JOBID'):
      self.jobID = os.environ['JOBID']

    self.lockValidity = gConfig.getValue('/Operations/SAM/LockValidity',24*60*60)

    #Workflow parameters for the test
    self.enable = True
    self.forceLockRemoval = False

    #Global parameter affecting behaviour
    self.safeMode = False

  #############################################################################
  def resolveInputVariables(self):
    """ By convention the workflow parameters are resolved here.
    """
    if self.step_commons.has_key('enable'):
      self.enable=self.step_commons['enable']
      if not type(self.enable)==type(True):
        self.log.warn('Enable flag set to non-boolean value %s, setting to False' %self.enable)
        self.enable=False

    if self.step_commons.has_key('forceLockRemoval'):
      self.forceLockRemoval=self.step_commons['forceLockRemoval']
      if not type(self.forceLockRemoval)==type(True):
        self.log.warn('Force lock flag set to non-boolean value %s, setting to False' %self.forceLockRemoval)
        self.enable=False

    if self.workflow_commons.has_key('SoftwareInstallationTest'):
       safeFlag = self.workflow_commons['SoftwareInstallationTest']
       if safeFlag=='False':
         self.safeMode=True

    self.log.verbose('Enable flag is set to %s' %self.enable)
    self.log.verbose('Force lock flag is set to %s' %self.forceLockRemoval)
    return S_OK()

  #############################################################################
  def execute(self):
    """The main execution method of the LockSharedArea module.
    """
    self.log.info('Initializing '+self.version)
    self.resolveInputVariables()
    self.setSAMLogFile()
    self.result = S_OK()
    if not self.result['OK']:
      return self.result

    if not self.workflowStatus['OK'] or not self.stepStatus['OK']:
      self.log.info('An error was detected in a previous step, exiting with status error.')
      return self.finalize('Problem during execution','Failure detected in a previous step','error')

    self.setApplicationStatus('Starting %s Test' %self.testName)
    self.runinfo = self.getRunInfo()

    # Change the permissions on the shared area
    self.log.info('Current account: %s' %self.runinfo['identity'])
    if not re.search('\d',self.runinfo['identityShort']):
      self.log.info('%s uses static accounts' %DIRAC.siteName())
      isPoolAccount = False
    else:
      self.log.info('%s uses pool accounts' %DIRAC.siteName())
      isPoolAccount = True

    #If running in safe mode stop here and return S_OK()
    if self.safeMode:
      self.log.info('We are running in SAM safe mode so no lock file will be created')
      self.setApplicationStatus('%s Successful (Safe Mode)' %self.testName)
      return self.finalize('%s Test Successful (Safe Mode)' %self.testName,'Status OK (= 10)','ok')

    result = self.runCommand('Checking current umask','umask')
    if not result['OK']:
      return self.finalize('umask returned non-zero status',result['Message'],'error')

    self.log.info('Current umask: %s' %result['Value'])
    if isPoolAccount:
      if not result['Value'].count('0002'):
        self.log.info('Changing current umask to 0002')
        try:
          os.umask(0002)
        except Exception,x:
          return self.finalize('excepton during umask',x,'error')
    else:
      if not result['Value'].count('0022'):
        self.log.info('Changing current umask to 0022')
        try:
          os.umask(0022)
        except Exception,x:
          return self.finalize('excepton during umask',x,'error')

    sharedArea = SharedArea()
    if not sharedArea:
      self.log.info('Could not determine sharedArea for site %s:\n%s\n trying to create it' %(DIRAC.siteName(),sharedArea))
      createSharedArea = CreateSharedArea()
      if not createSharedArea:
        return self.finalize('Could not create sharedArea for site %s:' %(DIRAC.siteName()),sharedArea,'error')
      sharedArea = SharedArea()
    else:
      self.log.info('Software shared area for site %s is %s' %(DIRAC.siteName(),sharedArea))

    #nasty fix but only way to resolve writeable volume at CERN
    if DIRAC.siteName()=='LCG.CERN.ch' or DIRAC.siteName()=='LCG.CERN5.ch':
      self.log.info('Changing shared area path to writeable volume at CERN')
      if re.search('.cern.ch',sharedArea):
        newSharedArea = sharedArea.replace('cern.ch','.cern.ch')
        self.writeToLog('Changing path to shared area writeable volume at LCG.CERN.ch:\n%s => %s' %(sharedArea,newSharedArea))
        sharedArea = newSharedArea

    self.log.info('Checking shared area contents: %s' %(sharedArea))
    result = self.runCommand('Checking contents of shared area directory: %s' %sharedArea,'ls -al %s' %sharedArea)
    if not result['OK']:
      return self.finalize('Could not list contents of shared area',result['Message'],'error')

    self.log.verbose('Trying to resolve shared area link problem')
    if os.path.exists('%s/lib' %sharedArea):
      if os.path.islink('%s/lib' %sharedArea):
        self.log.info('Removing link %s/lib' %sharedArea)
        result = self.runCommand('Removing link in shared area','rm -fv %s/lib' %sharedArea,check=True)
        if not result['OK']:
          return self.finalize('Could not remove link in shared area',result['Message'],'error')
      else:
        self.log.info('%s/lib is not a link so will not be removed' %sharedArea)
    else:
      self.log.info('Link in shared area %s/lib does not exist' %sharedArea)

    if self.forceLockRemoval:
      self.log.info('Deliberately removing SAM lock file')
      cmd = 'rm -fv %s/%s' %(sharedArea,self.lockFile)
      result = self.runCommand('Flag enabled to forcefully remove current %s' %self.lockFile,cmd,check=True)
      if not result['OK']:
        self.setApplicationStatus('Could Not Remove Lock File')
        self.log.warn(result['Message'])
        return self.finalize('Could not remove existing lock file via flag',self.lockFile,'critical')

      self.setJobParameter('ExistingSAMLock','Deleted via SAM test flag on %s' %(time.asctime()))

    self.log.info('Checking SAM lock file: %s' %self.lockFile)
    if os.path.exists('%s/%s' %(sharedArea,self.lockFile)):
      self.log.info('Another SAM job has established a lock on the shared area at %s' %sharedArea)
      curtime = time.time()
      fileTime = os.stat('%s/%s' %(sharedArea,self.lockFile))[8]
      if curtime - fileTime > self.lockValidity:
        self.log.info('SAM lock file present for > %s secs, deleting' %self.lockValidity)
        cmd = 'rm -fv %s/%s' %(sharedArea,self.lockFile)
        result = self.runCommand('Current lock file %s present for longer than %s seconds' %(self.lockFile,self.lockValidity),cmd,check=True)
        self.setApplicationStatus('Could Not Remove Old Lock File')
        if not result['OK']:
          self.log.warn(result['Message'])
          return self.finalize('Could not remove existing lock file exceeding maximum validity',result['Message'],'critical')
        self.setJobParameter('ExistingSAMLock','Removed on %s after exceeding maximum validity' %(time.asctime()))
      else:
        #unique to this test, prevent execution of software installation via 'notice' status
        self.log.info('Another SAM job has been running at this site for less than %s seconds disabling software installation test' %self.lockValidity)
        self.writeToLog('Another SAM job has been running at this site for less than %s seconds, disabling software installation test' %self.lockValidity)
        self.setApplicationStatus('Shared Area Lock Exists')
        return self.finalize('%s test running at same time as another SAM job' %self.testName,'Status NOTICE (= 30)','notice')

    cmd = 'touch %s/%s' %(sharedArea,self.lockFile)
    result = self.runCommand('Creating SAM lock file',cmd,check=True)
    if not result['OK']:
      self.log.warn(result['Message'])
      self.log.info('Trying to change permissions: %s' %(sharedArea))
      try:
        os.chmod(sharedArea,0775)
      except Exception,x:
        self.setApplicationStatus('Could Not Create Lock File')
        return self.finalize('Could not change permissions','%s' %(sharedArea),'critical')
      cmd = 'touch %s/%s' %(sharedArea,self.lockFile)
      result = self.runCommand('Creating SAM lock file',cmd,check=True)
      if not result['OK']:
        self.setApplicationStatus('Could Not Create Lock File')
        return self.finalize('Could not create lock file','%s/%s' %(sharedArea,self.lockFile),'critical')

    if os.path.exists('%s/install_project.py' %(sharedArea)):
      self.log.info('Removing install_project from SharedArea')
      cmd = 'rm -fv %s/install_project.py' %(sharedArea)
      result = self.runCommand('Removing install_project from SharedArea',cmd,check=True)
      if not result['OK']:
        self.setApplicationStatus('Could Not Remove File')
        self.log.warn(result['Message'])
        return self.finalize('Could not remove install_project from SharedArea ',result['Message'],'critical')

    self.setJobParameter('NewSAMLock','Created on %s' %(time.asctime()))
    self.log.info('Test %s completed successfully' %self.testName)
    self.setApplicationStatus('%s Successful' %self.testName)
    return self.finalize('%s Test Successful' %self.testName,'Status OK (= 10)','ok')
#    return S_OK('Shared area is locked') #This result not published to SAM DB.

  #############################################################################
  def __changePermissions(self,sharedArea):
    """Change permissions for pool SGM account case in python.
    """
    self.log.verbose('Changing permissions to 0775 in shared area %s' %sharedArea)
    self.writeToLog('Changing permissions to 0775 in shared area %s' %sharedArea)

    userID = self.runinfo['identityShort']

    try:
      for dirName, subDirs, files in os.walk(sharedArea):
        self.log.debug('Changing file permissions in directory %s' %dirName)
        if os.stat('%s' %(dirName))[4] == userID and not os.path.islink('%s' %(dirName)):
          os.chmod('%s' %(dirName),0775)
        for toChange in files:
          if os.stat('%s/%s' %(dirName,toChange))[4] == userID and not os.path.islink('%s/%s' %(dirName,toChange)):
            os.chmod('%s/%s' %(dirName,toChange),0775)
    except Exception,x:
      self.log.error('Problem changing shared area permissions',str(x))
      return S_ERROR(x)

    self.log.info('Permissions in shared area %s updated successfully' %(sharedArea))
    self.writeToLog('Permissions in shared area %s updated successfully' %(sharedArea))
    return S_OK()

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
