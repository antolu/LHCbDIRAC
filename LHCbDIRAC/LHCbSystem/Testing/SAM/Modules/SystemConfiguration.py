########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/LHCbSystem/Testing/SAM/Modules/SystemConfiguration.py,v 1.25 2009/06/03 14:11:58 joel Exp $
# Author : Stuart Paterson
########################################################################

""" LHCb System Configuration SAM Test Module

    Corresponds to SAM test CE-lhcb-os.
"""

__RCSID__ = "$Id: SystemConfiguration.py,v 1.25 2009/06/03 14:11:58 joel Exp $"

from DIRAC import S_OK, S_ERROR, gLogger, gConfig
from DIRAC.Core.DISET.RPCClient import RPCClient
try:
  from DIRAC.LHCbSystem.Utilities.CombinedSoftwareInstallation  import SharedArea
  from DIRAC.LHCbSystem.Testing.SAM.Modules.ModuleBaseSAM import *
except Exception,x:
  from LHCbSystem.Utilities.CombinedSoftwareInstallation  import SharedArea
  from LHCbSystem.Testing.SAM.Modules.ModuleBaseSAM import *

import string, os, sys, re, glob

SAM_TEST_NAME='CE-lhcb-os'
SAM_LOG_FILE='sam-os.log'

class SystemConfiguration(ModuleBaseSAM):

  #############################################################################
  def __init__(self):
    """ Standard constructor for SAM Module
    """
    ModuleBaseSAM.__init__(self)
    self.version = __RCSID__
    self.runinfo = {}
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

    self.runinfo = self.getRunInfo()
    self.setApplicationStatus('Starting %s Test' %self.testName)
    result = self.__checkMapping(self.runinfo['Proxy'],self.runinfo['identityShort'])
    if not result['OK']:
      return self.finalize('Potentiel problem in the mapping',self.runinfo['identityShort'],'warning')

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

    #nasty fix but only way to resolve writeable volume at CERN
    if self.site=='LCG.CERN.ch':
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

   # result = self.runCommand('Removing *_parameters.txt files from shared area','rm -fv %s/*_parameters.txt' %(sharedArea))
    result = self.__deleteSharedAreaFiles(sharedArea,'*_parameters.txt')
    if not result['OK']:
      return self.finalize('Could not remove shared area parameters files',result['Message'],'error')

 #   result = self.runCommand('Removing lcg-ManageVOTag.* files from shared area','rm -fv %s/lcg-ManageVOTag.*' %(sharedArea))
    result = self.__deleteSharedAreaFiles(sharedArea,'lcg-ManageVOTag.*')
    if not result['OK']:
      return self.finalize('Could not remove shared area VO tag files',result['Message'],'error')

    result = self.__deleteSharedAreaFiles(sharedArea,'lhcb.*')
    if not result['OK']:
      return self.finalize('Could not remove shared area lhcb.* files',result['Message'],'error')

    self.log.info('Checking shared area contents: %s' %(sharedArea))
    result = self.runCommand('Checking contents of shared area directory: %s' %sharedArea,'ls -al %s' %sharedArea)
    if not result['OK']:
      return self.finalize('Could not list contents of shared area',result['Message'],'error')

    result = self.runCommand('Checking current proxy', 'voms-proxy-info -all')
    if not result['OK']:
      return self.finalize('voms-proxy-info -all',result,'error')

    self.log.info('Current account: %s' %self.runinfo['identity'])
    if not re.search('\d',self.runinfo['identityShort']):
      self.log.info('%s uses static accounts' %self.site)
    else:
      self.log.info('%s uses pool accounts' %self.site)

    # FIXME: this prints lots of errors if not running with write privileges
    # R.G I have removed the -R to reduce the amount of errors.
    if os.path.exists('%s/lcg/external/dcache_client' %sharedArea):
      cmd = 'chmod 775 %s/lcg/external/dcache_client' %sharedArea
      result = self.runCommand('Changing dCache client permissions',cmd)
      if not result['OK']:
        self.setApplicationStatus('Shared Area Permissions Problem')
        return self.finalize(cmd,result['Message'],'error')
    else:
      self.log.info('%s/lcg/external/dcache_client does not exist' %sharedArea)

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

    cmd = 'rpm -qa | grep lcg_util | cut -f 2 -d "-"'
    result = self.runCommand('Checking RPM for LCG Utilities',cmd)
    if not result['OK']:
      return self.finalize('Could not get RPM version',result['Message'],'error')

    rpmOutput = result['Value']
    if rpmOutput.split('.')[0]=='1':
      if int(rpmOutput.split('.')[1]) < 6:
        return self.finalize('RPM version not correct',rpmOutput,'warning')

    self.log.info('Test %s completed successfully' %self.testName)
    self.setApplicationStatus('%s Successful' %self.testName)
    return self.finalize('%s Test Successful' %self.testName,'Status OK (= 10)','ok')

  #############################################################################
  def __checkMapping(self,proxy,map_name):
    """Return warning if the mapping is not the on expected..
    """

    self.log.info(' Check mapping')
    if proxy.find('lcgadmin') != -1:
      self.log.info("my username2 is %s" % (map_name))
      if map_name.find('s') != -1 or map_name.find('g') != -1 or map_name.find('m') != -1:
        self.log.info('correct mapping')
        return S_OK()
      else:
        self.log.warn('potentiel problem in the mapping')
        return S_ERROR('potentiel problem in the mapping')
    elif proxy.lower().find('production') != -1:
      if map_name.find('p') != -1 or map_name.find('r') != -1 or map_name.find('d') != -1:
        self.log.info('correct mapping')
        return S_OK()
      else:
        self.log.warn('potentiel problem in the mapping')
        return S_ERROR('potentiel problem in the mapping')
      

  #############################################################################
  def __deleteSharedAreaFiles(self,sharedArea,filePattern):
    """Remove all files in shared area.
    """
    self.log.verbose('Removing all files with name %s in shared area %s' %(filePattern,sharedArea))
    self.writeToLog('Removing all files with name %s shared area %s' %(filePattern,sharedArea))
    count = 0
    try:
      globList = glob.glob('%s/%s' %(sharedArea,filePattern))
      for check in globList:
        if os.path.isfile(check):
          os.remove(check)
          count += 1
    except Exception,x:
      self.log.error('Problem deleting shared area ',str(x))
      return S_ERROR(x)

    if count:
      self.log.info('Removed %s files with pattern %s from shared area' %(count,filePattern))
      self.writeToLog('Removed %s files with pattern %s from shared area' %(count,filePattern))
    else:
      self.log.info('No %s files to remove' %filePattern)
      self.writeToLog('No %s files to remove' %filePattern)

    self.log.info('Shared area %s successfully purged of %s files' %(sharedArea,filePattern))
    self.writeToLog('Shared area %s successfully purged of %s files' %(sharedArea,filePattern))
    return S_OK()

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
