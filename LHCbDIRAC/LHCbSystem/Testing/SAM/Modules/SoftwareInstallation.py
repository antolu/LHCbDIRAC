########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/LHCbSystem/Testing/SAM/Modules/SoftwareInstallation.py,v 1.3 2008/07/23 18:28:47 paterson Exp $
# Author : Stuart Paterson
########################################################################

""" LHCb SoftwareInstallation SAM Test Module

    Corresponds to SAM test CE-lhcb-install, utilizes the SoftwareManagementAgent
    to perform the installation of LHCb software in site shared areas. Deprecated
    software is also removed during this phase.

"""

__RCSID__ = "$Id: SoftwareInstallation.py,v 1.3 2008/07/23 18:28:47 paterson Exp $"

from DIRAC import S_OK, S_ERROR, gLogger, gConfig
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.ConfigurationSystem.Client.LocalConfiguration import LocalConfiguration
from DIRAC.Core.Base.Agent import createAgent
try:
  from DIRAC.LHCbSystem.Utilities.CombinedSoftwareInstallation  import SharedArea
  from DIRAC.LHCbSystem.Testing.SAM.Modules.ModuleBaseSAM import *
except Exception,x:
  from LHCbSystem.Utilities.CombinedSoftwareInstallation  import SharedArea
  from LHCbSystem.Testing.SAM.Modules.ModuleBaseSAM import *

import string, os, sys, re

SAM_TEST_NAME='CE-lhcb-install'
SAM_LOG_FILE='sam-install.log'

class SoftwareInstallation(ModuleBaseSAM):

  #############################################################################
  def __init__(self):
    """ Standard constructor for SAM Module
    """
    ModuleBaseSAM.__init__(self)
    self.version = __RCSID__
    self.logFile = SAM_LOG_FILE
    self.testName = SAM_TEST_NAME
    self.site = gConfig.getValue('/LocalSite/Site','LCG.Unknown.ch')
    self.log = gLogger.getSubLogger( "SoftwareInstallation" )
    self.softwareAgentName = gConfig.getValue('/Operations/SAM/SoftwareAgent','LHCb/SoftwareManagementAgent')
    self.result = S_ERROR()

    self.jobID = None
    if os.environ.has_key('JOBID'):
      self.jobID = os.environ['JOBID']

    #Workflow parameters for the test
    self.enable = True
    self.purgeSharedArea = False

  #############################################################################
  def resolveInputVariables(self):
    """ By convention the workflow parameters are resolved here.
    """
    if self.step_commons.has_key('enable'):
      self.enable=self.step_commons['enable']
      if not type(self.enable)==type(True):
        self.log.warn('Enable flag set to non-boolean value %s, setting to False' %self.enable)
        self.enable=False

    if self.step_commons.has_key('purgeSharedAreaFlag'):
      self.purgeSharedArea=self.step_commons['purgeSharedAreaFlag']
      if not type(self.purgeSharedArea)==type(True):
        self.log.warn('Purge shared area flag set to non-boolean value %s, setting to False' %self.purgeSharedArea)
        self.enable=False

    self.log.verbose('Enable flag is set to %s' %self.enable)
    self.log.verbose('Purge shared area flag set to %s' %self.purgeSharedArea)
    return S_OK()

  #############################################################################
  def execute(self):
    """The main execution method of the SoftwareInstallation module.
    """
    self.log.info('Initializing '+self.version)
    self.resolveInputVariables()

    self.result = S_OK()
    if not self.result['OK']:
      return self.result

    if not self.workflowStatus['OK'] or not self.stepStatus['OK']:
      self.log.info('An error was detected in a previous step, exiting.')
      return self.finalize('Problem during execution','Failure detected in a previous step','error')

    self.setApplicationStatus('Starting %s Test' %self.testName)
    sharedArea = SharedArea()
    if not sharedArea or not os.path.exists(sharedArea):
      self.log.info('Could not determine sharedArea for site %s:\n%s' %(self.site,sharedArea))
      return self.finalize('Could not determine shared area for site',sharedArea,'critical')
    else:
      self.log.info('Software shared area for site %s is %s' %(self.site,sharedArea))

    # Change the permissions on the shared area (if a pool account is used)
    result = self.runCommand('Checking current user account mapping','id -nu')
    if not result['OK']:
      return self.finalize('id -nu',result['Message'],'error')

    self.log.info('Current account: %s' %result['Value'])
    if not re.search('\d',result['Value']):
      self.log.info('%s uses static accounts' %self.site)
    else:
      self.log.info('%s uses pool accounts' %self.site)
      cmd = 'chmod -R 775 %s/lib' %sharedArea
      result = self.runCommand('Recursively changing shared area permissions',cmd)
      if not result['OK']:
        return self.finalize(cmd,result['Message'],'error')

    # Purge shared area if requested.
    if self.purgeSharedArea:
      self.log.info('Flag to purge the site shared area at %s is enabled' %sharedArea)
      if self.enable:
        self.log.verbose('Enable flag is True, starting shared area deletion')
        result = self.runCommand('Shared area deletion flag is enabled','rm -rf %s/*' %sharedArea)
        if not result['OK']:
          return self.finalize('Could not delete software in shared area',result['Message'],'critical')
      else:
        self.log.info('Enable flag is False so shared area will not be cleaned')

    #Install the software now
    if self.enable:
      try:
        agentName = self.softwareAgentName
        localCfg = LocalConfiguration()
        localCfg.addDefaultEntry('/LocalSite/SharedArea',sharedArea)
        localCfg.setConfigurationForAgent(agentName)
        resultDict = localCfg.loadUserData()
        if not resultDict[ 'OK' ]:
          return self.finalize("Errors when loading configuration", resultDict['Message'],'critical')

        agent = createAgent(agentName)
        result = agent.run_once()
        if not result['OK']['AgentResult']: #Will always return another boolean for the status due to the S_ERROR Agent framework issue
          return self.finalize('Software not successfully installed',result['Value'],'critical')
      except Exception,x:
        return self.finalize('Could not start %s with exception' %self.softwareAgentName,str(x),'critical')
    else:
      self.log.info('Software installation is disabled via enable flag')

    self.log.info('Test %s completed successfully' %self.testName)
    self.setApplicationStatus('%s Successful' %self.testName)
    return self.finalize('%s Test Successful' %self.testName,'Status OK (= 10)','ok')

  #EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#