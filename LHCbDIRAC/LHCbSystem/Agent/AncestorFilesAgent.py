########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/LHCbSystem/Agent/AncestorFilesAgent.py,v 1.7 2008/07/22 15:21:51 acasajus Exp $
# File :   AncestorFilesAgent.py
# Author : Stuart Paterson
########################################################################

"""   The LHCb AncestorFilesAgent queries the Bookkeeping catalogue for ancestor
      files if the JDL parameter AncestorDepth is specified.  The ancestor files
      are subsequently added to the existing input data requirement of the job.

      Initially the Ancestor Files Agent uses the previous Bookkeeping
      'genCatalog' utility but this will be updated in due course.
"""

__RCSID__ = "$Id: AncestorFilesAgent.py,v 1.7 2008/07/22 15:21:51 acasajus Exp $"

from DIRAC.WorkloadManagementSystem.Agent.Optimizer        import Optimizer
from DIRAC.Core.Utilities.ClassAd.ClassAdLight             import ClassAd
from DIRAC.BookkeepingSystem.Client.genCatalogOld          import getAncestors
from DIRAC.Core.DISET.RPCClient                            import RPCClient
from DIRAC.Core.Utilities.Shifter                          import setupShifterProxyInEnv
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
    self.pollingTime = gConfig.getValue(self.section+'/PollingTime',30) #seconds
    self.proxyLength = gConfig.getValue(self.section+'/DefaultProxyLength',24) # hours
    self.minProxyValidity = gConfig.getValue(self.section+'/MinimumProxyValidity',30*60) # seconds
    self.proxyLocation = gConfig.getValue(self.section+'/ProxyLocation','/opt/dirac/work/AncestorFilesAgent/shiftProdProxy')
    self.failedMinorStatus    = gConfig.getValue(self.section+'/FailedJobStatus','genCatalog Error')
    self.wmsAdmin = RPCClient('WorkloadManagement/WMSAdministrator')
    self.proxyLocation = gConfig.getValue( self.section+'/ProxyLocation', '' )
    if not self.proxyLocation:
      self.proxyLocation = False
    return result

  #############################################################################
  def checkJob(self,job):
    """ The main agent execution method
    """
    prodDN = gConfig.getValue('Operations/Production/ShiftManager','')
    if not prodDN:
      self.log.warn('Production shift manager DN not defined (/Operations/Production/ShiftManager)')
      return S_OK('Production shift manager DN is not defined')

    result = setupShifterProxyInEnv( "ProductionManager", self.proxyLocation )
    if not result[ 'OK' ]:
      self.log.error( "Can't get shifter's proxy: %s" % result[ 'Message' ] )
      return result

    self.log.verbose("Checking original JDL for job: %s" %(job))
    retVal = self.jobDB.getJobJDL(job,original=True)
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
    """This method checks the input data with ancestors. The original job JDL
       is always extracted to obtain the input data, therefore rescheduled jobs
       will not recursively search for ancestors of ancestors etc.
    """
    classadJob = ClassAd(jdl)
    if not classadJob.isOK():
      self.log.warn("Illegal JDL for job %s, will be marked problematic" % (job))
      return S_ERROR('Illegal Job JDL')

    inputData = classadJob.get_expression('InputData').replace('{','').replace('}','').replace('"','').split()

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
      return result

    newInputData = result['Value']
    self.log.verbose("Retrieving current JDL for job: %s" %(job))
    retVal = self.jobDB.getJobJDL(job)
    if not retVal['OK']:
      self.log.warn("Missing JDL for job %s, will be marked problematic" % (job))
      return S_ERROR('JDL not found in JobDB')

    currentJDL = retVal['Value']
    classadJobNew = ClassAd(currentJDL)
    if not classadJobNew.isOK():
      self.log.warn("Illegal JDL for job %s, will be marked problematic" % (job))
      return S_ERROR('Illegal Job JDL')

    classadJobNew.insertAttributeVectorString('InputData',newInputData)
    newJDL = classadJobNew.asJDL()
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
      self.log.warn('getAncestors failed with exception:\n%s' %x)
      return S_ERROR(self.failedMinorStatus)

    self.log.info('genCatalog.getAncestors lookup time %.2f s' %(time.time()-start))
    self.log.verbose(result)
    if not result:
      self.log.warn('Null result from genCatalog utility')
      return S_ERROR(self.failedMinorStatus)
    if not type(result)==type({}):
      self.log.warn('Non-dict object returned from genCatalog utility')
      return S_ERROR(self.failedMinorStatus)
    if not result.has_key('PFNs'):
      self.log.warn('----------BK-Result------------')
      self.log.warn(result)
      self.log.warn('--------End-BK-Result----------')
      self.log.warn('Missing key PFNs from genCatalog utility')
      return S_ERROR(self.failedMinorStatus)

    newInputData = result['PFNs']

    missingFiles = []
    for i in inputData:
      if not i in newInputData:
        missingFiles.append(i)

    #If no missing files and ancestor depth is 1 can return
    if ancestorDepth==1 and not missingFiles:
      return S_OK(newInputData)

    if missingFiles:
      param = '%s input data files missing from genCatalog utility result:\n%s' %(len(missingFiles),string.join(missingFiles,',\n'))
      report = self.setJobParam(job,self.optimizerName,param)
      if not report['OK']:
        self.log.warn(report['Message'])
      self.log.warn('genCatalog did not return all of original input data requirement')
      return S_ERROR(self.failedMinorStatus)
    ancestorFiles = []
    for i in newInputData:
      if not i in inputData:
        ancestorFiles.append(i)

    param = '%s ancestor files retrieved from genCatalog utility' %(len(ancestorFiles))
    report = self.setJobParam(job,self.optimizerName,param)
    if not report['OK']:
      self.log.warn(report['Message'])

    if not ancestorFiles:
      self.log.warn('Zero ancestor files returned from genCatalog')
      return S_ERROR('No Ancestor Files Found')

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

  #EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
