########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/LHCbSystem/Testing/SAM/Modules/SystemConfiguration.py,v 1.8 2008/07/31 10:56:22 paterson Exp $
# Author : Stuart Paterson
########################################################################

""" LHCb System Configuration SAM Test Module

    Corresponds to SAM test CE-lhcb-os.
"""

__RCSID__ = "$Id: SystemConfiguration.py,v 1.8 2008/07/31 10:56:22 paterson Exp $"

from DIRAC import S_OK, S_ERROR, gLogger, gConfig
from DIRAC.Core.DISET.RPCClient import RPCClient
try:
  from DIRAC.LHCbSystem.Utilities.CombinedSoftwareInstallation  import SharedArea
  from DIRAC.LHCbSystem.Testing.SAM.Modules.ModuleBaseSAM import *
except Exception,x:
  from LHCbSystem.Utilities.CombinedSoftwareInstallation  import SharedArea
  from LHCbSystem.Testing.SAM.Modules.ModuleBaseSAM import *

import string, os, sys, re

SAM_TEST_NAME='CE-lhcb-os'
SAM_LOG_FILE='sam-os.log'

class SystemConfiguration(ModuleBaseSAM):

  #############################################################################
  def __init__(self):
    """ Standard constructor for SAM Module
    """
    ModuleBaseSAM.__init__(self)
    self.version = __RCSID__
    self.logFile = SAM_LOG_FILE
    self.testName = SAM_TEST_NAME
    self.site = gConfig.getValue('/LocalSite/Site','LCG.Unknown.ch')
    self.log = gLogger.getSubLogger( "SystemConfiguration" )
    self.result = S_ERROR()

    self.jobID = None
    if os.environ.has_key('JOBID'):
      self.jobID = os.environ['JOBID']

    #Workflow parameters for the test
    self.enable = True

  #############################################################################
  def resolveInputVariables(self):
    """ By convention the workflow parameters are resolved here.
    """
    if self.step_commons.has_key('enable'):
      self.enable=self.step_commons['enable']
      if not type(self.enable)==type(True):
        self.log.warn('Enable flag set to non-boolean value %s, setting to False' %self.enable)
        self.enable=False

    self.log.verbose('Enable flag is set to %s' %self.enable)
    return S_OK()

  #############################################################################
  def execute(self):
    """The main execution method of the SystemConfiguration module.
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

    self.cwd  = os.getcwd()
    localRoot = gConfig.getValue( '/LocalSite/Root', self.cwd )
    site = gConfig.getValue( '/LocalSite/Site', 'Unknown' )
    self.log.info( "Root directory for job is %s" % ( localRoot ) )

    sharedArea = SharedArea()
    if not sharedArea or not os.path.exists(sharedArea):
      self.log.info('Could not determine sharedArea for site %s:\n%s' %(self.site,sharedArea))
      return self.finalize('Could not determine shared area for site',sharedArea,'critical')
    else:
      self.log.info('Software shared area for site %s is %s' %(self.site,sharedArea))

    result = self.runCommand('Checking current proxy', 'voms-proxy-info -all')
    if not result['OK']:
      return self.finalize('voms-proxy-info -all',result,'error')

    result = self.runCommand('Checking current user account mapping','id -nu',check=True)
    if not result['OK']:
      return self.finalize('id -nu',result['Message'],'error')

    self.log.info('Current account: %s' %result['Value'])
    if not re.search('\d',result['Value']):
      self.log.info('%s uses static accounts' %self.site)
    else:
      self.log.info('%s uses pool accounts' %self.site)

    cmd = 'chmod -R 775 %s/lib/lcg/external/dcache_client' %sharedArea
    result = self.runCommand('Changing dCache client permissions',cmd,check=True)
    if not result['OK']:
      self.setApplicationStatus('Shared Area Permissions Problem')
      return self.finalize(cmd,result['Message'],'error')

    cmd = 'rpm -qa | grep lcg_util | cut -f 2 -d "-"'
    result = self.runCommand('Checking RPM for LCG Utilities',cmd)
    if not result['OK']:
      return self.finalize('Could not get RPM version',result['Message'],'error')

    rpmOutput = result['Value']
    if rpmOutput.split('.')[0]=='1':
      if int(rpmOutput.split('.')[1]) < 6:
        return self.finalize('RPM version not correct',rpmOutput,'critical')

    systemConfigs = gConfig.getValue('/LocalSite/Architecture',[])
    self.log.info('Current system configurations are: %s ' %(string.join(systemConfigs,', ')))
    compatiblePlatforms = gConfig.getOptionsDict('/Resources/Computing/OSCompatibility')
    if not compatiblePlatforms['OK']:
      return self.finalize('Could not establish compatible platforms',compatiblePlatforms['Message'],'error')
    cPlats = compatiblePlatforms['Value'].keys()
    compatible = False
    for sc in systemConfigs:
      if sc in cPlats:
        compatible = True
    if not compatible:
      return self.finalize('Site does not have an officially compatible platform',string.join(systemConfigs,', '),'critical')

    result = self.getSAMNode()
    if not result['OK']:
      return self.finalize('Could not get current CE',result['Message'],'error')
    else:
      self.log.info('Current CE is %s' %result['Value'])

    libsToRemove = ['tls/libc.so.6','libgcc_s.so.1','tls/libm.so.6','tls/libpthread.so.0']
    for arch in systemConfigs:
      for lib in libsToRemove:
        libPath = '%s/%s/%s' %(sharedArea,arch,lib)
        if os.path.exists('%s' %libPath):
          self.log.info('Removing %s' %(libPath))
          cmd = 'rm -rf %s' %libPath
          result = self.runCommand('Removing incorrect compatibility library',cmd)
          if not result['OK']:
            return self.finalize('Failed to remove incorrect compatibility library',result['Message'],'error')

    for arch in systemConfigs:
      libPath = '%s/%s/' %(sharedArea,arch)
      cmd = 'ls -alR %s' %libPath
      result = self.runCommand('Checking compatibility libraries for system configuration %s' %(arch),cmd)
      if not result['OK']:
        return self.finalize('Failed to check compatibility library directory %s' %libPath,result['Message'],'error')

    self.log.info('Test %s completed successfully' %self.testName)
    self.setApplicationStatus('%s Successful' %self.testName)
    return self.finalize('%s Test Successful' %self.testName,'Status OK (= 10)','ok')

  #EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#