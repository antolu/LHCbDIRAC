"""  SAM Agent submits SAM jobs
"""
from DIRAC  import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.Base.Agent import Agent
from DIRAC.LHCbSystem.Testing.SAM.Client.DiracSAM import DiracSAM
from DIRAC.Core.Utilities.Shifter import setupShifterProxyInEnv

from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC  import gMonitor

import os,time

AGENT_NAME = "LHCb/SAMAgent"

class SAMAgent(Agent):

  def __init__(self):
    """ Standard constructor
    """
    Agent.__init__(self,AGENT_NAME)

  def initialize(self):
    result = Agent.initialize(self)

    self.pollingTime = gConfig.getValue(self.section+'/PollingTime',3600*6) # Every 6 hours
    gLogger.info("PollingTime %d hours" %(int(self.pollingTime)/3600))

    self.useProxies = gConfig.getValue(self.section+'/UseProxies','True').lower() in ( "y", "yes", "true" )
    self.proxyLocation = gConfig.getValue( self.section+'/ProxyLocation', '' )
    if not self.proxyLocation:
      self.proxyLocation = False

    gMonitor.registerActivity("TotalSites","Total Sites","SAMAgent","Sites",gMonitor.OP_SUM,3600)
    gMonitor.registerActivity("ActiveSites","Active Sites","SAMAgent","Sites",gMonitor.OP_SUM,3600)
    gMonitor.registerActivity("BannedSites","Banned Sites","SAMAgent","Sites",gMonitor.OP_SUM,3600)

    return result

  def execute(self):

    gLogger.debug("Executing %s"%(self.name))

    if self.useProxies:
      result = setupShifterProxyInEnv( "SAMManager", self.proxyLocation )
      if not result[ 'OK' ]:
        self.log.error( "Can't get shifter's proxy: %s" % result[ 'Message' ] )
        return result

    self._siteAccount()

    diracSAM = DiracSAM()

    result = diracSAM.submitAllSAMJobs()
     if not result['OK']:
      gLogger.error(result['Message'])
   
    return result

  def _siteAccount(self):
  
    wmsAdmin = RPCClient('WorkloadManagement/WMSAdministrator')
    result = wmsAdmin.getSiteMask()
    if not result[ 'OK' ]:
      self.log.error( "Can't get SiteMask: %s" % result[ 'Message' ] )
      return result
    sitesmask = result['Value']
    numsitesmask = len(sitesmask)

    result = gConfig.getSections('/Resources/Sites/LCG')
    if not result[ 'OK' ]:
      self.log.error( "Can't get Sites: %s" % result[ 'Message' ] )
      return result
    sites = result['Value']
    numsites = len(sites)
    
    gMonitor.addMark("TotalSites", numsites)
    gMonitor.addMark("ActiveSites", numsitesmask)
    gMonitor.addMark("BannedSites", numsites - numsitesmask)
   
    return S_OK()
