########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/ProductionManagementSystem/Agent/ProductionJobAgent.py,v 1.1 2008/02/20 11:57:07 paterson Exp $
########################################################################

"""  The Production Job Agent automatically submits production jobs after
     they have been created.  This interacts with WMS Admin interface to
     setup the current shift production manager credential and uses the
     Dirac Production interface to submit the jobs.
"""

__RCSID__ = "$Id: ProductionJobAgent.py,v 1.1 2008/02/20 11:57:07 paterson Exp $"

from DIRAC.Core.Base.Agent                                import Agent
from DIRAC.Core.DISET.RPCClient                           import RPCClient
from DIRAC.Interfaces.API.DiracProduction                 import DiracProduction
from DIRAC.Core.Utilities.GridCredentials                 import setupProxy,restoreProxy,setDIRACGroup, getProxyTimeLeft
from DIRAC                                                import S_OK, S_ERROR, gConfig, gLogger, gMonitor

import os, time

AGENT_NAME = 'ProductionManagement/ProductionJobAgent'

class ProductionJobAgent(Agent):

  #############################################################################
  def __init__(self):
    """ Standard constructor for Agent
    """
    Agent.__init__(self,AGENT_NAME)
    self.diracProd=DiracProduction()
    self.wmsAdmin = RPCClient('WorkloadManagement/WMSAdministrator')

  #############################################################################
  def initialize(self):
    """Sets defaults
    """
    result = Agent.initialize(self)
    self.pollingTime = gConfig.getValue(self.section+'/PollingTime',120)
    self.proxyLength = gConfig.getValue(self.section+'/DefaultProxyLength',12) # hours
    self.minProxyValidity = gConfig.getValue(self.section+'/MinimumProxyValidity',30*60) # seconds
    self.proxyLocation = gConfig.getValue(self.section+'/ProxyLocation','/opt/dirac/work/ProductionJobAgent/shiftProdProxy')
    self.jobToSubmitPerProduction = gConfig.getValue(self.section+'/JobsToSubmitPerProduction',1)

  #############################################################################
  def execute(self):
    """The ProductionJobAgent execution method.
    """
    prodGroup = gConfig.getValue(self.section+'/ProductionGroup','lhcb_prod')
    prodDN = gConfig.getValue('Operations/Production/ShiftManager','') #to check
    if not prodDN:
      return S_ERROR('Production shift manager DN is not defined')

    self.log.verbose('Checking proxy for %s %s' %(self.prodGroup,self.prodDN))
    result = self.__getProdProxy(self.prodDN,self.prodGroup)
    if not result['OK']:
      self.log.warn('Could not set up proxy for %s %s' %(self.prodGroup,self.prodDN))
      return result

    result = self.diracProd.getActiveProductions(self)
    if not result['OK']:
      self.log.warn('Failed to get list of active productions')
      self.log.warn(result)
      return result
    activeProductions = result['Value']
    if not activeProductions:
      self.log.info('No active productions discovered, no work to do')
      return S_OK('Loop completed')

    for production,status in activeProductions.items():
      if status.lower=='active':
        self.log.info('Attempting to submit %s jobs for production %s' %(self.jobToSubmitPerProduction,production))
        start = time.time()
        result = self.diracProd.submitProduction(production,self.jobToSubmitPerProduction,None)
        timing = time.time() - start
        if not result['OK']:
          self.log.warn(result['Message'])
        else:
          self.log.info('Production %s submission time: %.2f seconds for %s jobs' % (production,timing,self.jobToSubmitPerProduction))

    return S_OK('Productions submitted')

  #############################################################################
  def __getProdProxy(self,prodDN,prodGroup):
    """This method sets up the proxy for immediate use if not available, and checks the existing
       proxy if this is available.
    """
    self.log.info("Determining the length of proxy for DN %s" %prodDN)
    obtainProxy = False
    if not os.path.exists(self.proxyLocation):
      self.log.info("No proxy found")
      obtainProxy = True
    else:
      currentProxy = open(self.proxyLocation,'r')
      oldProxyStr = currentProxy.read()
      res = getProxyTimeLeft(oldProxyStr)
      if not res["OK"]:
        self.log.error("Could not determine the time left for proxy", res['Message'])
        return S_OK()
      proxyValidity = int(res['Value'])
      self.log.debug('Current proxy found to be valid for %s seconds' %proxyValidity)
      self.log.info('%s proxy found to be valid for %s seconds' %(prodDN,proxyValidity))
      if proxyValidity <= self.minProxyValidity:
        obtainProxy = True

    if obtainProxy:
      self.log.info('Attempting to renew %s proxy' %prodDN)
      res = self.wmsAdmin.getProxy(prodDN,prodGroup,self.proxyLength)
      if not res['OK']:
        self.log.error('Could not retrieve proxy from WMS Administrator', res['Message'])
        return S_OK()
      proxyStr = res['Value']
      if not os.path.exists(os.path.dirname(self.proxyLocation)):
        os.makedirs(os.path.dirname(self.proxyLocation))
      res = setupProxy(proxyStr,self.proxyLocation)
      if not res['OK']:
        self.log.error('Could not create environment for proxy.', res['Message'])
        return S_OK()
      setDIRACGroup(prodGroup)
      self.log.info('Successfully renewed %s proxy' %prodDN)

    self.log.verbose('voms-proxy-info -all')
    return S_OK('Active proxy available')

  #EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
