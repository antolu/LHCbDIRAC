########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/LHCbSystem/Testing/SAM/Modules/LockSharedArea.py,v 1.25 2008/08/21 09:01:19 roma Exp $
# Author : Stuart Paterson
########################################################################

""" LHCb LockSharedArea SAM Test Module
"""

__RCSID__ = "$Id: LockSharedArea.py,v 1.25 2008/08/21 09:01:19 roma Exp $"

from DIRAC import S_OK, S_ERROR, gLogger, gConfig
from DIRAC.Core.DISET.RPCClient import RPCClient
try:
  from DIRAC.LHCbSystem.Utilities.CombinedSoftwareInstallation  import SharedArea
  from DIRAC.LHCbSystem.Testing.SAM.Modules.ModuleBaseSAM import *
except Exception,x:
  from LHCbSystem.Utilities.CombinedSoftwareInstallation  import SharedArea
  from LHCbSystem.Testing.SAM.Modules.ModuleBaseSAM import *

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
    self.logFile = SAM_LOG_FILE
    self.testName = SAM_TEST_NAME
    self.lockFile = SAM_LOCK_NAME
    self.site = gConfig.getValue('/LocalSite/Site','LCG.Unknown.ch')
    self.log = gLogger.getSubLogger( "LockSharedArea" )
    self.result = S_ERROR()

    self.jobID = None
    if os.environ.has_key('JOBID'):
      self.jobID = os.environ['JOBID']

    self.lockValidity = gConfig.getValue('/Operations/SAM/LockValidity',24*60*60)

    #Workflow parameters for the test
    self.enable = True
    self.forceLockRemoval = False

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
    sharedArea = SharedArea()
    if not sharedArea:
      self.log.info('Could not determine sharedArea for site %s:\n%s' %(self.site,sharedArea))
      return self.finalize('Could not determine sharedArea for site %s:' %(self.site),sharedArea,'error')
    else:
      self.log.info('Software shared area for site %s is %s' %(self.site,sharedArea))

    #nasty fix but only way to resolve writeable volume at CERN
    if self.site=='LCG.CERN.ch':
      self.log.info('Changing shared area path to writeable volume at CERN')
      if re.search('.cern.ch',sharedArea):
        newSharedArea = sharedArea.replace('cern.ch','.cern.ch')
        self.writeToLog('Changing path to shared area writeable volume at LCG.CERN.ch:\n%s => %s' %(sharedArea,newSharedArea))
        sharedArea = newSharedArea

    if not os.path.exists(sharedArea):
      try:
        os.mkdir(sharedArea)
        self.log.info('Path to %s did not exist, shared area lib directory created' %sharedArea)
      except Exception,x:
        self.log.error('Could not create directory in shared area',str(x))
        return self.finalize('Could not create shared area lib directory',str(x),'critical')

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

    # Change the permissions on the shared area (if a pool account is used)
    result = self.runCommand('Checking current user account mapping','id')
    if not result['OK']:
      return self.finalize('id returned non-zero status',result['Message'],'error')

    self.log.info('Current account: %s' %result['Value'])
    if not re.search('\d',result['Value']):
      self.log.info('%s uses static accounts' %self.site)
      isPoolAccount = False
    else:
      self.log.info('%s uses pool accounts' %self.site)
      isPoolAccount = True

    result = self.runCommand('Checking current umask','umask')
    if not result['OK']:
      return self.finalize('umask returned non-zero status',result['Message'],'error')

    self.log.info('Current umask: %s' %result['Value'])
    if isPoolAccount:
      if not result['Value']=='0002':
        return self.finalize('Wrong umask','For pool account umask: %s'%result['Value'],'critical')
    else:
      if not result['Value']=='0022':
        return self.finalize('Wrong umask','For static account umask: %s'%result['Value'],'critical')
        
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
      if fileTime - curtime > self.lockValidity:
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

    self.log.info('Removing install_project.py from SharedArea')
    cmd = 'rm -fv install_project.py'
    result = self.runCommand('Removing install_project.py from SharedArea',cmd,check=True)
    if not result['OK']:
      self.setApplicationStatus('Could Not Remove File')
      self.log.warn(result['Message'])
      return self.finalize('Could not remove install_project.py from SharedArea ',result['Message'],'critical')


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

    result = self.runCommand('Checking current user account mapping','id -u',check=True)
    if not result['OK']:
      return self.finalize('id -u',result['Message'],'error')
    userID = result['Value']

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
