########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/LHCbSystem/Testing/SAM/Modules/SoftwareInstallation.py,v 1.16 2008/08/08 12:03:55 rgracian Exp $
# Author : Stuart Paterson
########################################################################

""" LHCb SoftwareInstallation SAM Test Module

    Corresponds to SAM test CE-lhcb-install, utilizes the SoftwareManagementAgent
    to perform the installation of LHCb software in site shared areas. Deprecated
    software is also removed during this phase.

"""

__RCSID__ = "$Id: SoftwareInstallation.py,v 1.16 2008/08/08 12:03:55 rgracian Exp $"

from DIRAC import S_OK, S_ERROR, gLogger, gConfig
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.ConfigurationSystem.Client.LocalConfiguration import LocalConfiguration
try:
  from DIRAC.LHCbSystem.Utilities.CombinedSoftwareInstallation  import SharedArea,InstallApplication,RemoveApplication
  from DIRAC.LHCbSystem.Testing.SAM.Modules.ModuleBaseSAM import *
except Exception,x:
  from LHCbSystem.Utilities.CombinedSoftwareInstallation  import SharedArea,InstallApplication,RemoveApplication
  from LHCbSystem.Testing.SAM.Modules.ModuleBaseSAM import *

import string, os, sys, re, shutil

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
    self.setSAMLogFile()
    self.result = S_OK()
    if not self.result['OK']:
      return self.result

    if not self.workflowStatus['OK'] or not self.stepStatus['OK']:
      self.log.info('An error was detected in a previous step, exiting with status error.')
      return self.finalize('Problem during execution','Failure detected in a previous step','error')

    if not self.workflow_commons.has_key('SAMResults'):
      return self.finalize('Problem determining CE-lhcb-lock test result','No SAMResults key in workflow commons','error')

    if int(self.workflow_commons['SAMResults']['CE-lhcb-lock']) > int(self.samStatus['ok']):
      self.writeToLog('Another SAM job is running at this site, disabling software installation test for this CE job')
      return self.finalize('%s test will be disabled' %self.testName,'Status INFO (= 20)','info')

    self.setApplicationStatus('Starting %s Test' %self.testName)
    sharedArea = SharedArea()
    if not sharedArea or not os.path.exists(sharedArea):
      self.log.info('Could not determine sharedArea for site %s:\n%s' %(self.site,sharedArea))
      return self.finalize('Could not determine shared area for site',sharedArea,'critical')
    else:
      self.log.info('Software shared area for site %s is %s' %(self.site,sharedArea))

    # Change the permissions on the shared area (if a pool account is used)
    result = self.runCommand('Checking current user account mapping','id -nu',check=True)
    if not result['OK']:
      return self.finalize('id -nu',result['Message'],'error')
    # Change the permissions on the shared area (if a pool account is used)

    if not re.search('\d',result['Value']):
      isPoolAccount = False
    else:
      isPoolAccount = True

    # Purge shared area if requested.
    if self.purgeSharedArea:
      self.log.info('Flag to purge the site shared area at %s is enabled' %sharedArea)
      if self.enable:
        self.log.verbose('Enable flag is True, starting shared area deletion')
        result = self.__deleteSharedArea(sharedArea)
#        result = self.runCommand('Shared area deletion flag is enabled','rm -rf %s/*' %sharedArea,check=True)
        if not result['OK']:
          return self.finalize('Could not delete software in shared area',result['Message'],'critical')
      else:
        self.log.info('Enable flag is False so shared area will not be cleaned')

    #Install the software now
    if self.enable:
      activeSoftware = '/Operations/SoftwareDistribution/Active'
      installList = gConfig.getValue(activeSoftware,[])
      if not installList:
        return self.finalize('The active list of software could not be retreived from',activeSoftware,'error')

      deprecatedSoftware = '/Operations/SoftwareDistribution/Deprecated'
      removeList = gConfig.getValue(deprecatedSoftware,[])

      localArch = gConfig.getValue('/LocalSite/Architecture','')
      if not localArch:
        return self.finalize('/LocalSite/Architecture is not defined in the local configuration','Could not get /LocalSite/Architecture','error')

      #must get the list of compatible platforms for this architecture
      localPlatforms = gConfig.getValue('/Resources/Computing/OSCompatibility/%s' %localArch,[])
      if not localPlatforms:
        return self.finalize('Could not obtain compatible platforms for %s' %localArch,'/Resources/Computing/OSCompatibility/%s' %localArch,'error')

      if not os.path.exists(sharedArea):
        try:
          os.mkdir(sharedArea)
        except Exception,x:
          return self.finalize('Could not create proposed shared area directory:',sharedArea,'critical')

      for systemConfig in localPlatforms:
        self.log.info('The following software packages will be installed:\n%s\nfor system configuration %s' %(string.join(installList,'\n'),systemConfig))
        packageList = gConfig.getValue('/Operations/SoftwareDistribution/%s' %(systemConfig),[])

        for installPackage in installList:
          appNameVersion = string.split(installPackage,'.')
          if not len(appNameVersion)==2:
            return self.finalize('Could not determine name and version of package:',installPackage,'error')
          #Must check that package to install is supported by LHCb for requested system configuration

          if installPackage in packageList:
            self.log.info('Attempting to install %s %s for system configuration %s' %(appNameVersion[0],appNameVersion[1],systemConfig))
            orig = sys.stdout
            catch = open(self.logFile,'a')
            sys.stdout=catch
            result = InstallApplication(appNameVersion, systemConfig, sharedArea )
            sys.stdout=orig
            catch.close()
            #result = True
            if not result: #or not result['OK']:
              return self.finalize('Problem during execution, result is stopping.',result,'error')
            else:
              self.log.info('Installation of %s %s for %s successful' %(appNameVersion[0],appNameVersion[1],systemConfig))
          else:
            self.log.info('%s is not supported for system configuration %s, nothing to install.' %(installPackage,systemConfig))

        for removePackage in removeList:
          appNameVersion = string.split(removePackage,'.')
          if not len(appNameVersion)==2:
            return self.finalize('Could not determine name and version of package:',installPackage,'error')

          if removePackage in packageList:
            self.log.info('Attempting to remove %s %s for system configuration %s' %(appNameVersion[0],appNameVersion[1],systemConfig))
            orig = sys.stdout
            catch = open(self.logFile,'a')
            sys.stdout=catch
            result = RemoveApplication(appNameVersion, systemConfig, sharedArea )
            sys.stdout=orig
            catch.close()
            result = True
            if not result: # or not result['OK']:
              return self.finalize('Problem during execution, stopping.',result,'error')
            else:
              self.log.info('Removal of %s %s for %s successful' %(appNameVersion[0],appNameVersion[1],systemConfig))
          else:
            self.log.info('%s is not supported for system configuration %s, nothing to remove.' %(removePackage,systemConfig))
    else:
      self.log.info('Software installation is disabled via enable flag')

    if isPoolAccount:
      result = self.__changePermissions(sharedArea)
      if not result['OK']:
        return self.finalize('Failed To Change Shared Area Permissions',result['Message'],'error')
      

    self.log.info('Test %s completed successfully' %self.testName)
    self.setApplicationStatus('%s Successful' %self.testName)
    return self.finalize('%s Test Successful' %self.testName,'Status OK (= 10)','ok')

  #############################################################################
  def __deleteSharedArea(self,sharedArea):
    """Remove all files in shared area.
    """
    self.log.verbose('Removing all files in shared area %s' %sharedArea)
    self.writeToLog('Removing all files in shared area %s' %sharedArea)
    try:
      for fdir in os.listdir(sharedArea):
        if os.path.isfile('%s/%s' %(sharedArea,fdir)):
          os.remove('%s/%s' %(sharedArea,fdir))
        elif os.path.isdir('%s/%s' %(sharedArea,fdir)):
          self.log.verbose('Removing directory %s/%s' %(sharedArea,fdir))
          self.writeToLog('Removing directory %s/%s' %(sharedArea,fdir))
          shutil.rmtree('%s/%s' %(sharedArea,fdir))
    except Exception,x:
      self.log.error('Problem deleting shared area ',str(x))
      return S_ERROR(x)

    self.log.info('Shared area %s successfully purged' %(sharedArea))
    self.writeToLog('Shared area %s successfully purged' %(sharedArea))
    return S_OK()

  #############################################################################
  def __changePermissions(self,sharedArea):
    """Change permissions for pool SGM account case in python.
    """
    self.log.verbose('Changing permissions to 0775 in shared area %s' %sharedArea)
    self.writeToLog('Changing permissions to 0775 in shared area %s' %sharedArea)
    try:
      for dirName, subDirs, files in os.walk(sharedArea):
        self.log.verbose('Changing file permissions in directory %s' %dirName)
        self.writeToLog('Changing file permissions in directory %s' %dirName)
        try:
          os.chmod('%s' %(dirName),0775)
        except:
          pass
        for toChange in files:
          try:
            os.chmod('%s/%s' %(dirName,toChange),0775)
          except:
            pass
    except Exception,x:
      self.log.error('Problem changing shared area permissions',str(x))
      return S_ERROR(x)

    self.log.info('Permissions in shared area %s updated successfully' %(sharedArea))
    self.writeToLog('Permissions in shared area %s updated successfully' %(sharedArea))
    return S_OK()

  #EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#