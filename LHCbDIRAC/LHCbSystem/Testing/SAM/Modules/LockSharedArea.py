########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/LHCbSystem/Testing/SAM/Modules/LockSharedArea.py,v 1.7 2008/08/05 10:31:25 paterson Exp $
# Author : Stuart Paterson
########################################################################

""" LHCb LockSharedArea SAM Test Module
"""

__RCSID__ = "$Id: LockSharedArea.py,v 1.7 2008/08/05 10:31:25 paterson Exp $"

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
    if not sharedArea or not os.path.exists(sharedArea):
      self.log.info('Could not determine sharedArea for site %s:\n%s' %(self.site,sharedArea))
      return self.finalize('Could not determine sharedArea for site %s:' %(self.site),sharedArea,'error')
    else:
      self.log.info('Software shared area for site %s is %s' %(self.site,sharedArea))

    # Change the permissions on the shared area (if a pool account is used)
    result = self.runCommand('Checking current user account mapping','id -nu',check=True)
    if not result['OK']:
      return self.finalize('id -nu',result['Message'],'error')

    self.log.info('Current account: %s' %result['Value'])
    if not re.search('\d',result['Value']):
      self.log.info('%s uses static accounts' %self.site)
    else:
      self.log.info('%s uses pool accounts' %self.site)
      cmd = 'chmod -R 775 %s/lib' %sharedArea
      result = self.runCommand('Recursively changing shared area permissions',cmd,check=True)
      if not result['OK']:
        return self.finalize('Failed To Change Shared Area Permissions',result['Message'],'error')

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
      self.setApplicationStatus('Could Not Create Lock File')
      return self.finalize('Could not create lock file','%s/%s' %(sharedArea,self.lockFile),'critical')

    self.setJobParameter('NewSAMLock','Created on %s' %(time.asctime()))
    self.log.info('Test %s completed successfully' %self.testName)
    self.setApplicationStatus('%s Successful' %self.testName)
    return self.finalize('%s Test Successful' %self.testName,'Status OK (= 10)','ok')
#    return S_OK('Shared area is locked') #This result not published to SAM DB.

  #EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#