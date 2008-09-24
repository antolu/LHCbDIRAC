"""  SAM Agent submits SAM jobs
"""
from DIRAC  import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.Base.Agent import Agent
from DIRAC.LHCbSystem.Testing.SAM.Client.DiracSAM import DiracSAM
from DIRAC.Core.Utilities.Shifter import setupShifterProxyInEnv

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
    return result

  def execute(self):

    gLogger.debug("Executing %s"%(self.name))
    if self.useProxies:
      result = setupShifterProxyInEnv( "SAMManager", self.proxyLocation )
      if not result[ 'OK' ]:
        self.log.error( "Can't get shifter's proxy: %s" % result[ 'Message' ] )
        return result

    diracSAM = DiracSAM()

    result = diracSAM.submitAllSAMJobs()
    if not result['OK']:
      gLogger.error(result['Message'])
   
    return result
