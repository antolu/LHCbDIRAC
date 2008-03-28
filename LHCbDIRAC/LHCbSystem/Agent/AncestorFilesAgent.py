########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/LHCbSystem/Agent/AncestorFilesAgent.py,v 1.1 2008/03/28 18:44:58 paterson Exp $
# File :   AncestorFilesAgent.py
# Author : Stuart Paterson
########################################################################

"""   The LHCb AncestorFilesAgent queries the Bookkeeping catalogue for ancestor
      files if the JDL parameter AncestorDepth is specified.  The ancestor files
      are subsequently added to the existing input data requirement of the job.

      Initially the Ancestor Files Agent uses the previous Bookkeeping
      'genCatalog' utility but this will be updated in due course.
"""

__RCSID__ = "$Id: AncestorFilesAgent.py,v 1.1 2008/03/28 18:44:58 paterson Exp $"

from DIRAC.WorkloadManagementSystem.Agent.Optimizer        import Optimizer
from DIRAC.Core.Utilities.ClassAd.ClassAdLight             import ClassAd
from DIRAC.BookkeepingSystem.Client.genCatalogOld          import getAncestors
from DIRAC.Core.DISET.RPCClient                            import RPCClient
from DIRAC.Core.Utilities.GridCredentials                  import setupProxy,restoreProxy,setDIRACGroup, getProxyTimeLeft
from DIRAC                                                 import gConfig, S_OK, S_ERROR

import os, re, time, string

OPTIMIZER_NAME = 'AncestorFiles'

class AncestorFilesAgent(Optimizer):

  #############################################################################
  def __init__(self):
    """ Constructor, takes system flag as argument.
    """
    Optimizer.__init__(self,OPTIMIZER_NAME,enableFlag=True,system='LHCb')

  #############################################################################
  def initialize(self):
    """Initialize specific parameters for AncestorFilesAgent.
    """
    result = Optimizer.initialize(self)
    self.pollingTime = gConfig.getValue(self.section+'/PollingTime',60) #seconds
    self.proxyLength = gConfig.getValue(self.section+'/DefaultProxyLength',24) # hours
    self.minProxyValidity = gConfig.getValue(self.section+'/MinimumProxyValidity',30*60) # seconds
    self.proxyLocation = gConfig.getValue(self.section+'/ProxyLocation','/opt/dirac/work/AncestorFilesAgent/shiftProdProxy')
    self.failedMinorStatus    = gConfig.getValue(self.section+'/FailedJobStatus','genCatalog Error')
    self.wmsAdmin = RPCClient('WorkloadManagement/WMSAdministrator')
    return result

  #############################################################################
  def checkJob(self,job):
    """ The main agent execution method
    """
    prodDN = gConfig.getValue('Operations/Production/ShiftManager','')
    if not prodDN:
      self.log.warn('Production shift manager DN not defined (/Operations/Production/ShiftManager)')
      return S_OK('Production shift manager DN is not defined')

    self.log.verbose('Checking proxy for %s' %(prodDN))
    result = self.__getProdProxy(prodDN)
    if not result['OK']:
      self.log.warn('Could not set up proxy for shift manager %s %s' %(prodDN))
      return S_OK('Production shift manager proxy could not be set up')

    self.log.verbose("Checking JDL for job: %s" %(job))
    retVal = self.jobDB.getJobJDL(job)
    if not retVal['OK']:
      self.log.warn("Missing JDL for job %s, will be marked problematic" % (job))
      return S_ERROR('JDL not found in JobDB')

    jdl = retVal['Value']
    if not jdl:
      self.log.warn("Null JDL for job %s, will be marked problematic" % (job))
      return S_ERROR('Null JDL returned from JobDB')

    result = self.__checkAncestorDepth(job,jdl)
    if not result['OK']:
      self.log.warn(result['Message'])
      return result

    result = self.setNextOptimizer(job)
    if not result['OK']:
      self.log.warn(result['Message'])
    return result

  #############################################################################
  def __checkAncestorDepth(self,job,jdl):
    """This method checks the input data with ancestors.
    """
    result = self.jobDB.getInputData(job)
    if not result['OK'] or not result['Value']:
      return S_ERROR('Input Data Not Found')

    inputData = result['Value']
    classadJob = ClassAd(jdl)
    if not classadJob.isOK():
      self.log.warn("Illegal JDL for job %s, will be marked problematic" % (job))
      return S_ERROR('Illegal Job JDL')

    if not classadJob.lookupAttribute('AncestorDepth'):
      self.log.warn('No AncestorDepth requirement found for job %s' %(job))
      return S_ERROR('AncestorDepth Not Found')

    ancestorDepth = classadJob.get_expression('AncestorDepth').replace('"','').replace('Unknown','')
    if not type(ancestorDepth)==type(1):
      try:
        ancestorDepth = int(ancestorDepth)
      except Exception,x:
        return S_ERROR('Illegal AncestorDepth Requirement')

    if ancestorDepth==0:
      return S_OK('Null AncestorDepth specified')

    self.log.info('Job %s has %s input data files and specified ancestor depth of %s' % (job,len(inputData),ancestorDepth))
    result = self.__getInputDataWithAncestors(job,inputData,ancestorDepth)
    if not result['OK']:
      self.log.warn(result['Message'])
      return S_ERROR(self.failedMinorStatus)

    newInputData = result['Value']
    classadJob.insertAttributeVectorString('InputData',newInputData)
    newJDL = classadJob.asJDL()
    result = self.__setJobInputData(job,newJDL,newInputData)
    return result

  ############################################################################
  def __getInputDataWithAncestors(self,job,inputData,ancestorDepth):
    """Extend the list of LFNs with the LFNs for their ancestor files
       for the generation depth specified in the job JDL.
    """
    inputData = [ i.replace('LFN:','') for i in inputData]
    start = time.time()
    try:
      result = getAncestors(inputData,ancestorDepth)
    except Exception,x:
      return S_ERROR('getAncestors failed with exception:\n%s' %x)

    self.log.info('genCatalog.getAncestors lookup time %.2f s' %(time.time()-start))
    self.log.verbose(result)
    if not result:
      return S_ERROR('Null result from genCatalog utility')
    if not type(result)==type({}):
      return S_ERROR('Non-dict object returned from genCatalog utility')
    if not result.has_key('PFNs'):
      self.log.warn('----------BK-Result------------')
      self.log.warn(result)
      self.log.warn('--------End-BK-Result----------')
      return S_ERROR('Missing key PFNs from genCatalog utility')

    newInputData = result['PFNs']

    missingFiles = []
    for i in inputData:
      if not i in newInputData:
        missingFiles.append(i)

    if missingFiles:
      param = '%s input data files missing from genCatalog utility result:\n%s' %(len(missingFiles),string.join(missingFiles,',\n'))
      report = self.setJobParam(job,self.optimizerName,param)
      if not report['OK']:
        self.log.warn(report['Message'])
      return S_ERROR('genCatalog did not return all of original input data requirement')

    ancestorFiles = []
    for i in newInputData:
      if not i in inputData:
        ancestorFiles.append(i)

    param = '%s ancestor files retrieved from genCatalog utility' %(len(ancestorFiles))
    report = self.setJobParam(job,self.optimizerName,param)
    if not report['OK']:
      self.log.warn(report['Message'])

    return S_OK(newInputData)

  #############################################################################
  def __setJobInputData(self,job,jdl,inputData):
    """Sets the new job input data requirement including ancestor files.
    """
    inputData = [ i.replace('LFN:','') for i in inputData]
    inputData = map( lambda x: 'LFN:'+x, inputData)

    result = self.jobDB.setInputData(job,inputData)
    if not result['OK']:
      self.log.warn(result['Message'])
      return S_ERROR('Setting New Input Data')

    result = self.jobDB.setJobJDL(int(job),jdl)
    if not result['OK']:
      self.log.warn(result['Message'])
      return S_ERROR('Setting New JDL')

    return S_OK('Job updated')

  #############################################################################
  def __getProdProxy(self,prodDN):
    """This method sets up the proxy for immediate use if not available, and checks the existing
       proxy if this is available.
    """
    prodGroup = gConfig.getValue(self.section+'/ProductionGroup','lhcb_prod')
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
        res = S_OK(0) # force update of proxy

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

    #os.system('voms-proxy-info -all')
    return S_OK('Active proxy available')

  #EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
