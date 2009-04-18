########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/LHCbSystem/Testing/SAM/Modules/RunTestScript.py,v 1.2 2009/04/18 18:26:56 rgracian Exp $
# Author : Stuart Paterson
########################################################################

""" LHCb RunTestScript SAM Test Module
"""

__RCSID__ = "$Id: RunTestScript.py,v 1.2 2009/04/18 18:26:56 rgracian Exp $"

from DIRAC import S_OK, S_ERROR, gLogger, gConfig
from DIRAC.Core.DISET.RPCClient import RPCClient
try:
  from DIRAC.LHCbSystem.Testing.SAM.Modules.ModuleBaseSAM import *
except Exception,x:
  from LHCbSystem.Testing.SAM.Modules.ModuleBaseSAM import *

import string, os, sys, re, time

SAM_TEST_NAME='CE-lhcb-test-script'
SAM_LOG_FILE='sam-run-test-script.log'
SAM_LOCK_NAME='DIRAC-SAM-Test-Script'

class RunTestScript(ModuleBaseSAM):

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
    self.site = gConfig.getValue('/LocalSite/Site','LCG.Unknown.ch')
    self.log = gLogger.getSubLogger( "RunTestScript" )
    self.result = S_ERROR()

    self.jobID = None
    if os.environ.has_key('JOBID'):
      self.jobID = os.environ['JOBID']

    #Workflow parameters for the test
    self.enable = True
    self.scriptName = ''

  #############################################################################
  def resolveInputVariables(self):
    """ By convention the workflow parameters are resolved here.
    """
    if self.step_commons.has_key('enable'):
      self.enable=self.step_commons['enable']
      if not type(self.enable)==type(True):
        self.log.warn('Enable flag set to non-boolean value %s, setting to False' %self.enable)
        self.enable=False

    if self.step_commons.has_key('scriptName'):
      self.scriptName=self.step_commons['scriptName']
      if not type(self.scriptName)==type(" "):
        self.log.warn('Script name parameter set to non-string value %s, setting enable to False' %self.scriptName)
        self.enable=False
    else:
      self.log.warn('Script name not set, setting enable flag to False')

    self.log.verbose('Enable flag is set to %s' %self.enable)
    self.log.verbose('Script name is set to %s' %self.scriptName)
    return S_OK()

  #############################################################################
  def execute(self):
    """The main execution method of the RunTestScript module.
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

    #Should fail the test in the case where the script is not locally available on the WN
    if not os.path.exists('%s/%s' %(os.getcwd(),self.scriptName)):
      return self.finalize('Script not found','%s' %(self.scriptName),'notice')

    #Assume any status code is ok but report a non-zero status to the logs and report SAM notice status
    cmd = '%s %s' %(sys.executable,self.scriptName)
    self.log.info('Prepended DIRAC python to script, execution command will be "%s"' %(cmd))
    result = self.runCommand('Executing script with commmand "%s"' %cmd,cmd,check=True)
    if not result['OK']:
      self.log.warn('%s returned non-zero status' %(self.scriptName))
      return self.finalize('Script not found','%s' %(self.scriptName),'info')

    self.setApplicationStatus('%s Successful' %self.testName)
    return self.finalize('%s Test Successful' %self.testName,'Status OK (= 10)','ok')

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
