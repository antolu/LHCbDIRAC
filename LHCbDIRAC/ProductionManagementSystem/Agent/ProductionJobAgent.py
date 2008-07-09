########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/ProductionManagementSystem/Agent/ProductionJobAgent.py,v 1.11 2008/07/09 10:16:49 paterson Exp $
########################################################################

"""  The Production Job Agent automatically submits production jobs after
     they have been created.  This interacts with WMS Admin interface to
     setup the current shift production manager credential and uses the
     Dirac Production interface to submit the jobs.
"""

__RCSID__ = "$Id: ProductionJobAgent.py,v 1.11 2008/07/09 10:16:49 paterson Exp $"

from DIRAC.Core.Base.Agent                                import Agent
from DIRAC.Core.DISET.RPCClient                           import RPCClient
from DIRAC.Interfaces.API.DiracProduction                 import DiracProduction
from DIRAC.Core.Utilities.GridCredentials                 import setupProxyFile,restoreProxy,setDIRACGroup,getProxyTimeLeft,setupProxy
from DIRAC                                                import S_OK, S_ERROR, gConfig, gMonitor

import os, time, string

AGENT_NAME = 'ProductionManagement/ProductionJobAgent'

class ProductionJobAgent(Agent):

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
    self.pollingTime = gConfig.getValue(self.section+'/PollingTime',120)
    self.proxyLength = gConfig.getValue(self.section+'/DefaultProxyLength',12) # hours
    self.minProxyValidity = gConfig.getValue(self.section+'/MinimumProxyValidity',30*60) # seconds
    self.proxyLocation = gConfig.getValue(self.section+'/ProxyLocation','/opt/dirac/work/ProductionJobAgent/shiftProdProxy')
    self.jobsToSubmitPerProduction = gConfig.getValue(self.section+'/JobsToSubmitPerProduction',50)
    self.productionStatus = gConfig.getValue(self.section+'/SubmitStatus','automatic')
    self.enableFlag = None
    gMonitor.registerActivity("SubmittedJobs","Automatically submitted jobs","Production Monitoring","Jobs", gMonitor.OP_ACUM)
    return result

  #############################################################################
  def execute(self):
    """The ProductionJobAgent execution method.
    """
    self.enableFlag = gConfig.getValue(self.section+'EnableFlag','True')
    if not self.enableFlag == 'True':
      self.log.info('ProductionJobAgent is disabled by configuration option %s/EnableFlag' %(self.section))
      return S_OK('Disabled via CS flag')

    prodGroup = gConfig.getValue(self.section+'/ProductionGroup','lhcb_prod')
    if not prodGroup:
      return S_ERROR('No production group for DIRAC defined')
    prodDN = gConfig.getValue('Operations/Production/ShiftManager','') #to check
    if not prodDN:
      return S_ERROR('Production shift manager DN is not defined')

    self.log.verbose('Checking proxy for %s %s' %(prodGroup,prodDN))
    result = self.__getProdProxy(prodDN,prodGroup)
    if not result['OK']:
      self.log.warn('Could not set up proxy for %s %s' %(prodGroup,prodDN))
      return result

    diracProd=DiracProduction()
    result = diracProd.getActiveProductions()
    self.log.verbose(result)
    if not result['OK']:
      self.log.warn('Failed to get list of active productions')
      self.log.warn(result)
      return result
    activeProductions = result['Value']
    if not activeProductions:
      self.log.info('No active productions discovered, no work to do')
      return S_OK('Loop completed')

    for production,status in activeProductions.items():
      if status.lower()==self.productionStatus:
        self.log.info('Attempting to submit %s jobs for production %s' %(self.jobsToSubmitPerProduction,production))
        start = time.time()
        result = diracProd.submitProduction(production,self.jobsToSubmitPerProduction)
        timing = time.time() - start
        if not result['OK']:
          self.log.warn(result['Message'])
        else:
          jobsDict = result['Value']
          if jobsDict.has_key('Successful'):
            jobsList = jobsDict['Successful']
            submittedJobs = len(jobsList)
            gMonitor.addMark("SubmittedJobs",int(submittedJobs))
            if submittedJobs:
              self.log.info('Production %s submission time: %.2f seconds for %s jobs' % (production,timing,submittedJobs))
            else:
              self.log.info('No jobs to submit for production %s' %(production))
      else:
        self.log.verbose('Nothing to do for productionID %s with status %s' %(production,status))

    return S_OK('Productions submitted')

  #############################################################################
  def __getProdProxy(self,prodDN,prodGroup):
    """This method sets up the proxy for immediate use if not available, and checks the existing
       proxy if this is available.
    """
    self.log.verbose("Determining the length of proxy for DN %s" %prodDN)
    obtainProxy = False
    if not os.path.exists(self.proxyLocation):
      self.log.info("No proxy found")
      obtainProxy = True
    else:
      res = setupProxyFile(self.proxyLocation)
      if not res["OK"]:
        self.log.error("Could not determine the time left for proxy", res['Message'])
        res = S_OK(0) # force update of proxy

      proxyValidity = int(res['Value'])
      self.log.info('%s proxy found to be valid for %s seconds' %(prodDN,proxyValidity))
      if proxyValidity <= self.minProxyValidity:
        obtainProxy = True

    if obtainProxy:
      self.log.info('Attempting to renew %s proxy' %prodDN)
      wmsAdmin = RPCClient('WorkloadManagement/WMSAdministrator')
      res = wmsAdmin.getProxy(prodDN,prodGroup,self.proxyLength)
      if not res['OK']:
        self.log.error('Could not retrieve proxy from WMS Administrator', res['Message'])
        return S_ERROR('Could not retrieve proxy from WMS Administrator')
      proxyStr = res['Value']
      if not os.path.exists(os.path.dirname(self.proxyLocation)):
        os.makedirs(os.path.dirname(self.proxyLocation))
      res = setupProxy(proxyStr,self.proxyLocation)
      if not res['OK']:
        self.log.error('Could not create environment for proxy', res['Message'])
        return S_ERROR('Could not create environment for proxy')

      setDIRACGroup(prodGroup)
      self.log.info('Successfully renewed %s proxy' %prodDN)

    #os.system('voms-proxy-info -all')
    return S_OK('Active proxy available')

  #EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
