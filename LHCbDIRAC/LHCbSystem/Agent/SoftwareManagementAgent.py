########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/LHCbSystem/Agent/SoftwareManagementAgent.py,v 1.7 2009/04/18 18:26:58 rgracian Exp $
# File :   SoftwareManagementAgent.py
# Author : Stuart Paterson
########################################################################

"""  The LHCb SoftwareManagementAgent is designed to manage software caches
     for DIRAC or Grid sites.  This could be integrated as part of e.g. a
     SAM framework or can simply be used 'by hand' to install all VO software.

     This depends on a VO software installation plugin module and expects the
     list of software to be in the DIRAC CS.  A detailed report of any errors is
     given after all software is attempted to be installed.

"""

__RCSID__ = "$Id: SoftwareManagementAgent.py,v 1.7 2009/04/18 18:26:58 rgracian Exp $"

from DIRAC.Core.Base.Agent                                      import Agent
from DIRAC.Core.Utilities.ModuleFactory                         import ModuleFactory
from DIRAC                                                      import S_OK, S_ERROR, gConfig, gLogger

import os, sys, re, string, time, shutil

AGENT_NAME = 'LHCb/SoftwareManagementAgent'

class SoftwareManagementAgent(Agent):

  #############################################################################
  def __init__(self):
    """ Standard constructor for Agent
    """
    Agent.__init__(self,AGENT_NAME)

  #############################################################################
  def initialize(self):
    """Sets defaults
    """
    result = Agent.initialize(self)
    self.pollingTime = gConfig.getValue(self.section+'/PollingTime',10)
    #DEBUGGING OPTIONS
    self.pollingTime = 5
    self.maxcount = 1 #AGent.py base class must be fixed for run_once use-case
    return result

  #############################################################################
  def execute(self):
    """The SoftwareManagementAgent execution method.
    """
    softwareModule = gConfig.getValue(self.section+'/ModulePath','DIRAC.LHCbSystem.Utilities.CombinedSoftwareInstallation')
    self.log.info('LHCb Software Distribution module: %s' %(softwareModule))
    try:
      exec 'from %s import InstallApplication,RemoveApplication' %softwareModule
    except Exception,x:
      return self.__finish('Could not import InstallApplication,RemoveApplication from %s' %(softwareModule))

    activeSoftware = gConfig.getValue(self.section+'/ActiveSoftwareSection','/Operations/SoftwareDistribution/Active')
    installList = gConfig.getValue(activeSoftware,[])
    if not installList:
      return self.__finish('The active list of software could not be retreived from %s or is null' %(activeSoftware))

    deprecatedSoftware = gConfig.getValue(self.section+'/DeprecatedSoftwareSection','/Operations/SoftwareDistribution/Deprecated')
    removeList = gConfig.getValue(deprecatedSoftware,[])

    localPlatforms = gConfig.getValue('/LocalSite/Architecture',[])
    if not localPlatforms:
      return self.__finish('/LocalSite/Architecture is not defined in the local configuration')

    sharedArea = gConfig.getValue('/LocalSite/SharedArea')
    if not sharedArea:
      return self.__finish('/LocalSite/SharedArea is not found, exiting')

    purgeFlag =  gConfig.getValue(self.section+'/PurgeFlag','Disabled')
    if purgeFlag.lower()=='enabled':
      self.log.info('Purge flag is ENABLED, removing all software from shared area')
      for item in os.listdir(sharedArea):
        self.log.verbose('Removing %s from shared area %s' %(item,sharedArea))
        if os.path.isdir('%s/%s' %(sharedArea,item)):
          shutil.rmtree('%s/%s' %(sharedArea,item))
        else:
          os.remove('%s/%s' %(sharedArea,item))

    if not os.path.exists(sharedArea):
      try:
        os.mkdir(sharedArea)
      except Exception,x:
        return self.__finish('Could not create proposed shared area directory:\n%s' %(sharedArea))

    for systemConfig in localPlatforms:
      self.log.info('The following software packages will be installed by %s:\n%s\nfor system configuration %s' %(AGENT_NAME,string.join(installList,'\n'),systemConfig))
      packageList = gConfig.getValue('/Operations/SoftwareDistribution/%s' %(systemConfig),[])

      for installPackage in installList:
        appNameVersion = string.split(installPackage,'.')
        if not len(appNameVersion)==2:
          return self.__finish('Could not determine name and version of package: %s' %installPackage)
        #Must check that package to install is supported by LHCb for requested system configuration

        if installPackage in packageList:
          self.log.info('Attempting to install %s %s for system configuration %s' %(appNameVersion[0],appNameVersion[1],systemConfig))
          result = InstallApplication(appNameVersion, systemConfig, sharedArea )
          #result = True
          if not result: #or not result['OK']:
            return self.__finish('Problem during execution:\n %s\n agent is stopped.' %(result))
          else:
            self.log.info('Installation of %s %s for %s successful' %(appNameVersion[0],appNameVersion[1],systemConfig))
        else:
          self.log.info('%s is not supported for system configuration %s, nothing to install.' %(installPackage,systemConfig))

      for removePackage in removeList:
        appNameVersion = string.split(removePackage,'.')
        if not len(appNameVersion)==2:
          return self.__finish('Could not determine name and version of package: %s' %installPackage)

        if removePackage in packageList:
          self.log.info('Attempting to remove %s %s for system configuration %s' %(appNameVersion[0],appNameVersion[1],systemConfig))
          result = RemoveApplication(appNameVersion, systemConfig, sharedArea )
          result = True
          if not result: # or not result['OK']:
            return self.__finish('Problem during execution:\n %s\n agent is stopped.' %(result))
          else:
            self.log.info('Removal of %s %s for %s successful' %(appNameVersion[0],appNameVersion[1],systemConfig))
        else:
          self.log.info('%s is not supported for system configuration %s, nothing to remove.' %(removePackage,systemConfig))

    return self.__finish('Successful',failed=False)

  #############################################################################
  def __finish(self,message,failed=True):
    """Force the agent to complete gracefully.
    """
    self.log.info('%s will stop with message: \n%s' %(AGENT_NAME,message))
    if failed:
      fd = open(self.controlDir+'/stop_agent','w')
      fd.write('%s Stopped at %s [UTC]' % (AGENT_NAME,time.asctime(time.gmtime())))
      fd.close()
      self.log.info('%s Stopped at %s [UTC] using %s/stop_agent control flag.' %(AGENT_NAME,time.asctime(time.gmtime()),self.controlDir))
    else:
      self.log.info('%s Stopped at %s [UTC] after one loop.' %(AGENT_NAME,time.asctime(time.gmtime())))
    result =  S_OK(message)
    result['AgentResult']=failed
    return result

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
