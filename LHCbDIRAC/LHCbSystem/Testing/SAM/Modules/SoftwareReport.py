########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/LHCbSystem/Testing/SAM/Modules/SoftwareReport.py,v 1.1 2009/07/09 16:00:35 joel Exp $
# Author : Stuart Paterson
########################################################################

""" LHCb SoftwareReport SAM Test Module

    Corresponds to SAM test CE-lhcb-softreport, utilizes the SoftwareManagementAgent
    to report the installation of LHCb software in site shared areas.

"""

__RCSID__ = "$Id: SoftwareReport.py,v 1.1 2009/07/09 16:00:35 joel Exp $"

from DIRAC import S_OK, S_ERROR, gLogger, gConfig, systemCall
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.ConfigurationSystem.Client.LocalConfiguration import LocalConfiguration
try:
  from DIRAC.LHCbSystem.Utilities.CombinedSoftwareInstallation  import SharedArea, CreateSharedArea
  from DIRAC.LHCbSystem.Testing.SAM.Modules.ModuleBaseSAM import *
except Exception,x:
  from LHCbSystem.Utilities.CombinedSoftwareInstallation  import SharedArea, CreateSharedArea
  from LHCbSystem.Testing.SAM.Modules.ModuleBaseSAM import *

import string, os, sys, re, shutil, urllib

SAM_TEST_NAME='CE-lhcb-softreport'
SAM_LOG_FILE='sam-softreport.log'
InstallProject = 'install_project.py'
InstallProjectURL = 'http://cern.ch/lhcbproject/dist/'

class SoftwareReport(ModuleBaseSAM):

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
    self.log = gLogger.getSubLogger( "SoftwareReport" )
    self.result = S_ERROR()

    self.jobID = None
    if os.environ.has_key('JOBID'):
      self.jobID = os.environ['JOBID']

    #Workflow parameters for the test
    self.enable = True
    self.installProjectURL = None

  #############################################################################
  def resolveInputVariables(self):
    """ By convention the workflow parameters are resolved here.
    """
    if self.step_commons.has_key('enable'):
      self.enable=self.step_commons['enable']
      if not type(self.enable)==type(True):
        self.log.warn('Enable flag set to non-boolean value %s, setting to False' %self.enable)
        self.enable=False

    if self.step_commons.has_key('installProjectURL'):
      self.installProjectURL=self.step_commons['installProjectURL']
      if not type(self.installProjectURL)==type(" ") or not self.installProjectURL:
        self.log.warn('Install project URL not set to non-zero string parameter, setting to None')
        self.installProjectURL = None

    self.log.verbose('Enable flag is set to %s' %self.enable)
    self.log.verbose('Install project URL set to %s' %(self.installProjectURL))
    return S_OK()

  #############################################################################
  def execute(self):
    """The main execution method of the SoftwareReport module.
    """
    self.log.info('Initializing '+self.version)
    self.resolveInputVariables()
    self.setSAMLogFile()
    self.result = S_OK()

    soft_present = []
    if not self.result['OK']:
      return self.result

    if not self.workflowStatus['OK'] or not self.stepStatus['OK']:
      self.log.info('An error was detected in a previous step, exiting with status error.')
      return self.finalize('Problem during execution','Failure detected in a previous step','error')

    if not self.workflow_commons.has_key('SAMResults'):
      return self.finalize('Problem determining CE-lhcb-lock test result','No SAMResults key in workflow commons','error')

    self.runinfo = self.getRunInfo()
    if not self.enable:
      return self.finalize('%s test is disabled via control flag' %self.testName,'Status INFO (= 20)','info')

    self.setApplicationStatus('Starting %s Test' %self.testName)
    if not CreateSharedArea():
      self.log.info( 'Can not get access to Shared Area for SW installation' )
      return self.finalize('Could not determine shared area for site', 'Status ERROR (=50)','error' )
    sharedArea = SharedArea()
    if not sharedArea or not os.path.exists(sharedArea):
      # After previous check this error should never occur
      self.log.info('Could not determine sharedArea for site %s:\n%s' %(self.site,sharedArea))
      return self.finalize('Could not determine shared area for site',sharedArea,'critical')
    else:
      self.log.info('Software shared area for site %s is %s' %(self.site,sharedArea))

    #Check for optional install project URL
    if self.installProjectURL:
      self.writeToLog('Found specified install_project URL %s' %(self.installProjectURL))
      installProjectName = 'install_project.py'
      if os.path.exists('%s/%s' %(os.getcwd(),installProjectName)):
        self.writeToLog('Removing previous install project script from local area')
        os.remove('%s/%s' %(os.getcwd(),installProjectName))
      installProjectFile = os.path.basename(self.installProjectURL)
      localname,headers = urllib.urlretrieve(self.installProjectURL,installProjectFile)
      if not os.path.exists('%s/%s' %(os.getcwd(),installProjectFile)):
        return self.finalize('%s could not be downloaded to local area' %(self.installProjectURL))
      else:
        self.writeToLog('install_project downloaded from %s to local area' %(self.installProjectURL))
      self.writeToLog('Copying downloaded install_project to sharedArea %s' %sharedArea)
      if not installProjectFile==installProjectName:
        shutil.copy('%s/%s' %(os.getcwd(),installProjectFile),'%s/%s' %(os.getcwd(),installProjectName))
      shutil.copy('%s/%s' %(os.getcwd(),installProjectName),'%s/%s' %(sharedArea,installProjectName))

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

#
#to be remove...
#
      sharedArea = '/afs/.cern.ch/project/gd/apps/lhcb/lib'
      for systemConfig in localPlatforms:
        self.log.info('The following software packages will be checked:\n%s\nfor system configuration %s' %(string.join(installList,'\n'),systemConfig))
        packageList = gConfig.getValue('/Operations/SoftwareDistribution/%s' %(systemConfig),[])

        for installPackage in installList:
          appNameVersion = string.split(installPackage,'.')
          if not len(appNameVersion)==2:
            return self.finalize('Could not determine name and version of package:',installPackage,'error')
          #Must check that package to install is supported by LHCb for requested system configuration

          if installPackage in packageList:
            self.log.info('Attempting to check %s %s for system configuration %s' %(appNameVersion[0],appNameVersion[1],systemConfig))
#            orig = sys.stdout
#            catch = open(self.logFile,'a')
#            sys.stdout=catch
            result = CheckPackage(self, appNameVersion, systemConfig, sharedArea )
#            sys.stdout=orig
#            catch.close()
            #result = True
            if not result: #or not result['OK']:
              return self.finalize('Problem during execution, result is stopping.',result,'error')
            else:
#              self.log.info('Installation of %s %s for %s successful' %(appNameVersion[0],appNameVersion[1],systemConfig))
              soft_present.append((appNameVersion[0], appNameVersion[1] , systemConfig))
          else:
            self.log.info('%s is not supported for system configuration %s, nothing to install.' %(installPackage,systemConfig))

        self.log.info(soft_present)

        for removePackage in removeList:
          appNameVersion = string.split(removePackage,'.')
          if not len(appNameVersion)==2:
            return self.finalize('Could not determine name and version of package:',installPackage,'error')

          if removePackage in packageList:
            self.log.info('Attempting to remove %s %s for system configuration %s' %(appNameVersion[0],appNameVersion[1],systemConfig))
            orig = sys.stdout
            catch = open(self.logFile,'a')
            sys.stdout=catch
            result = CheckPackage(self,appNameVersion, systemConfig, sharedArea )
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


    self.log.info('Test %s completed successfully' %self.testName)
    self.setApplicationStatus('%s Successful' %self.testName)
    return self.finalize('%s Test Successful' %self.testName,'Status OK (= 10)','ok')


def CheckPackage(self, app, config, area):
  """
   check if given application is available in the given area
  """
  if not os.path.exists('%s/%s' %(os.getcwd(),InstallProject)):
    localname,headers = urllib.urlretrieve('%s%s' %(InstallProjectURL,InstallProject),InstallProject)
    if not os.path.exists('%s/%s' %(os.getcwd(),InstallProject)):
      self.log.error('%s/%s could not be downloaded' %(InstallProjectURL,InstallProject))
      return False

  if not area:
    return False

  localArea = area
  if re.search(':',area):
    localArea = string.split(area,':')[0]

  appName    = app[0]
  appVersion = app[1]

  installProject = os.path.join( localArea, InstallProject )
  if not os.path.exists( installProject ):
    try:
      shutil.copy( InstallProject, localArea )
    except:
      self.log.error( 'Failed to get:', installProject )
      return False

  # Now run the installation
  curDir = os.getcwd()
  #NOTE: must cd to LOCAL area directory (install_project requirement)
  os.chdir(localArea)

  cmtEnv = dict(os.environ)
  cmtEnv['MYSITEROOT'] = area
  self.log.info( 'Defining MYSITEROOT = %s' % area )
  cmtEnv['CMTCONFIG']  = config
  self.log.info( 'Defining CMTCONFIG = %s' % config )

  cmdTuple =  [sys.executable]
  cmdTuple += [InstallProject]
  cmdTuple += ['-d']
  cmdTuple += [ '-p', appName ]
  cmdTuple += [ '-v', appVersion ]
  cmdTuple += [ '--check' ]

  self.log.info( 'Executing %s' % ' '.join(cmdTuple) )
  timeout = 300
  ret = systemCall( timeout, cmdTuple, env=cmtEnv )
#  self.log.debug(ret)
  os.chdir(curDir)
  if not ret['OK']:
    self.log.error('Software check failed, missing software', '%s %s:\n%s' %(appName,appVersion,ret['Value'][2]))
    return False
  if ret['Value'][0]: # != 0
    self.log.error('Software check failed with non-zero status', '%s %s:\n%s' %(appName,appVersion,ret['Value'][2]))
    return False

  if ret['Value'][2]:
    self.log.debug('Error reported with ok status for install_project check:\n%s' %ret['Value'][2])

  return True

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
