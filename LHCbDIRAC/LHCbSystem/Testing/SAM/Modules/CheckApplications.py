########################################################################
# $HeadURL$
# Author : Stuart Paterson
########################################################################

""" LHCb CheckApplications SAM Test Module

    Corresponds to SAM test CE-lhcb-checkapp, utilizes the SoftwareManagementAgent
    to report the installation of LHCb software in site shared areas.

"""

__RCSID__ = "$Id$"

from DIRAC import S_OK, S_ERROR, gLogger, gConfig, systemCall
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.ConfigurationSystem.Client.LocalConfiguration import LocalConfiguration

from LHCbDIRAC.LHCbSystem.Utilities.CombinedSoftwareInstallation  import SharedArea, CreateSharedArea, MySiteRoot
from LHCbDIRAC.LHCbSystem.Testing.SAM.Modules.ModuleBaseSAM import *

import string, os, sys, re, shutil, urllib

SAM_TEST_NAME='CE-lhcb-checkapp'
SAM_LOG_FILE='sam-checkapp.log'

class CheckApplications(ModuleBaseSAM):

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
    self.log = gLogger.getSubLogger( "CheckApplications" )
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

    self.log.verbose('Enable flag is set to %s' %self.enable)
    return S_OK()

  #############################################################################
  def execute(self):
    """The main execution method of the SoftwareReport module.
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

    #Install the software now
    if self.enable:
      activeSoftware = '/Operations/SoftwareDistribution/Active'
      installList = gConfig.getValue(activeSoftware,[])
      if not installList:
        return self.finalize('The active list of software could not be retreived from',activeSoftware,'error')

      localArch = gConfig.getValue('/LocalSite/Architecture','')
      if not localArch:
        return self.finalize('/LocalSite/Architecture is not defined in the local configuration','Could not get /LocalSite/Architecture','error')

      #must get the list of compatible platforms for this architecture
      localPlatforms = gConfig.getOptionsDict('/Resources/Computing/OSCompatibility')
      if not localPlatforms:
        return self.finalize('Could not obtain compatible platforms for /Resources/Computing/OSCompatibility/','error')

#
#to be remove...
#
      sharedArea = MySiteRoot()
      if sharedArea == '':
        self.log.error( 'MySiteRoot Not found' )
        return S_ERROR(' MySiteRoot Not Found')

      mySiteRoot=sharedArea
      self.log.info('MYSITEROOT is %s' %mySiteRoot)
      localArea = sharedArea
      if re.search(':',sharedArea):
        localArea = string.split(sharedArea,':')[0]
        self.log.info('Setting local software area to %s' %localArea)

      for systemConfig in localPlatforms['Value'].keys():
#        self.log.info('The following software packages will be checked:\n%s\nfor system configuration %s' %(string.join(installList,'\n'),systemConfig))
        packageList = gConfig.getValue('/Operations/SoftwareDistribution/%s' %(systemConfig),[])

        for installPackage in installList:
          appNameVersion = string.split(installPackage,'.')
          if not len(appNameVersion)==2:
            return self.finalize('Could not determine name and version of package:',installPackage,'error')
          #Must check that package to install is supported by LHCb for requested system configuration

          if installPackage in packageList:
            self.log.info('Attempting to check %s %s for system configuration %s' %(appNameVersion[0],appNameVersion[1],systemConfig))
            orig = sys.stdout
            catch = open(self.logFile,'a')
            sys.stdout=catch
            result = CheckPackage(self, appNameVersion, systemConfig, sharedArea )
            sys.stdout=orig
            catch.close()
            #result = True
            if not result: #or not result['OK']:
              soft_present_pb.append((appNameVersion[0], appNameVersion[1] , systemConfig))
              if softwareDictPb.has_key(installPackage):
                current = softwareDictPb[installPackage]
                current.append(systemConfig)
                softwareDictPb[installPackage]=current
              else:
                softwareDictPb[installPackage]=[systemConfig]
            else:
#              self.log.info('Installation of %s %s for %s successful' %(appNameVersion[0],appNameVersion[1],systemConfig))
              if softwareDict.has_key(installPackage):
                current = softwareDict[installPackage]
                current.append(systemConfig)
                softwareDict[installPackage]=current
              else:
                softwareDict[installPackage]=[systemConfig]
              soft_present.append((appNameVersion[0], appNameVersion[1] , systemConfig))
#          else:
#            self.log.info('%s is not supported for system configuration %s, nothing to check.' %(installPackage,systemConfig))


    else:
      self.log.info('Software installation is disabled via enable flag')


    self.log.info('Test %s completed successfully' %self.testName)
    self.setApplicationStatus('%s Successful' %self.testName)
    return self.finalize('%s Test Successful' %self.testName,'Status OK (= 10)','ok')

  #############################################################################
  def getSoftwareReport(self,softwareDict):
    """Returns the list of software installed at the site organized by platform.
       If the test status is not successful, returns a link to the install test
       log.  Creates an html table for the results.
    """

    #If software installation test was not run by this job e.g. is 'notice' status, do not add software report.

    self.log.verbose(softwareDict)
    rows = """
    <br><br><br>
    Software summary from job running on node with system configuration :
    <br><br><br>
    """
    sortedKeys = softwareDict.keys()
    sortedKeys.sort()
    for appNameVersion in sortedKeys:
      archList = softwareDict[appNameVersion]
      name = appNameVersion.split('.')[0]
      version = appNameVersion.split('.')[1]
      sysConfigs = string.join(archList,', ')
      rows += """

<tr>
<td> %s </td>
<td> %s </td>
<td> %s </td>
</tr>
      """ %(name,version,sysConfigs)

    self.log.debug(rows)

    table = """<table border="1" bordercolor="#000000" width="50%" bgcolor="#BCCDFE">
<tr>
<td>Project Name</td>
<td>Project Version</td>
<td>System Configurations</td>
</tr>"""+rows+"""
</table>
"""
    self.log.debug(table)
    return table




def CheckPackage(self, app, config, area):
  """
   check if given application is available in the given area
  """

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

def CheckSharedArea(self, area):
  """
   check if all application  in the  area are used or not
  """
  if not os.path.exists(os.environ['LBSCRIPTS_HOME']+'/InstallArea/scripts/usedProjects'):
      self.log.error('UsedProjects is not in the path')
      return False

  if not area:
    return False

  localArea = area
  if re.search(':',area):
    localArea = string.split(area,':')[0]

  # Now run the installation
  curDir = os.getcwd()
  #NOTE: must cd to LOCAL area directory (install_project requirement)
  os.chdir(localArea)
  software_remove = {}

  cmdTuple = ['usedProjects']
  cmdTuple += ['-r']
  cmdTuple += ['-v']

  self.log.info( 'Executing %s' % ' '.join(cmdTuple) )
  timeout = 300
  ret = systemCall( timeout, cmdTuple )
#  self.log.info(ret)
  os.chdir(curDir)
  if not ret['OK']:
#    self.log.error('Software check failed, missing software', '%s %s:\n%s' %(appName,appVersion,ret['Value'][2]))
    return False
  if ret['Value'][0]: # != 0
#    self.log.error('Software check failed with non-zero status', '%s %s:\n%s' %(appName,appVersion,ret['Value'][2]))
    return False

  if ret['Value'][2]:
    self.log.debug('Error reported with ok status for install_project check:\n%s' %ret['Value'][2])

  for line in ret['Value'][1].split('\n'):
      if line.find('remove') != -1:
          line = line.split()
          if software_remove.has_key(line[1]):
            current = software_remove[line[1]]
            current.append(line[3])
            software_remove[line[1]]=current
          else:
            software_remove[line[1]]=[line[3]]
  self.log.info('Applications that could be remove')
  self.log.info(software_remove)
  return True

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
