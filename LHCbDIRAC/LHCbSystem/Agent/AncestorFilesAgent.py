########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/LHCbSystem/Agent/AncestorFilesAgent.py,v 1.8 2008/08/13 15:53:19 rgracian Exp $
# File :   AncestorFilesAgent.py
# Author : Stuart Paterson
########################################################################

"""   The LHCb AncestorFilesAgent queries the Bookkeeping catalogue for ancestor
      files if the JDL parameter AncestorDepth is specified.  The ancestor files
      are subsequently added to the existing input data requirement of the job.

      Initially the Ancestor Files Agent uses the previous Bookkeeping
      'genCatalog' utility but this will be updated in due course.
"""

__RCSID__ = "$Id: AncestorFilesAgent.py,v 1.8 2008/08/13 15:53:19 rgracian Exp $"

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
  def checkJob( self, job, jdl = None, classad = None ):
    """ The main agent execution method
    """
    self.log.verbose('Job %s will be processed' % (job))

    result = self.getJDLandClassad( job, jdl, classad )
    if not result['OK']:
      return result

    JDL = result['JDL']
    classadJob = result['Classad']

    # FIXME: why do we need the Shifter proxy in here?
    result = setupShifterProxyInEnv( "ProductionManager", self.proxyLocation )
    if not result[ 'OK' ]:
      self.log.error( "Can't get shifter's proxy: %s" % result[ 'Message' ] )
      return S_ERROR( 'Can not get shifter proxy' )

    result = self.__checkAncestorDepth(job,classadJob)
    if not result['OK']:
      return result

    return self.setNextOptimizer(job)

  #############################################################################
  def __checkAncestorDepth(self,job,classadJob):
    """This method checks the input data with ancestors. The original job JDL
       is always extracted to obtain the input data, therefore rescheduled jobs
       will not recursively search for ancestors of ancestors etc.
    """

    #Now check whether the job has an input data requirement
    retVal = self.jobDB.getJobJDL(job,original=True)
    if not retVal['OK']:
      self.log.warn("Missing JDL for job %s, will be marked problematic" % (job))
      return S_ERROR('JDL not found in JobDB')

    JDL = retVal['Value']
    classadJobOrg = ClassAd( '[%s]' % JDL )
    if not classadJobOrg.isOK():
      self.log.warn("Illegal JDL for job %s, will be marked problematic" % (job))
      return S_ERROR('Error in JDL syntax')

    inputData = []
    if classAddJob.lookupAttribute('InputData'):
      inputData = classAddJob.getListFromExpression('InputData')

    if not classadJob.lookupAttribute('AncestorDepth'):
      self.log.warn('No AncestorDepth requirement found for job %s' %(job))
      return S_ERROR('AncestorDepth Not Found')

    ancestorDepth = classadJob.intFromClassAd( 'AncestorDepth' )

    if ancestorDepth==0:
      return S_OK('Null AncestorDepth specified')

    self.log.info('Job %s has %s input data files and specified ancestor depth of %s' % (job,len(inputData),ancestorDepth))
    result = self.__getInputDataWithAncestors(job,inputData,ancestorDepth)
    if not result['OK']:
      return result

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
      self.log.warn('getAncestors failed with exception:\n%s' %x)
      return S_ERROR(self.failedMinorStatus)

    self.log.info('genCatalog.getAncestors lookup time %.2f s' %(time.time()-start))
    self.log.verbose(result)
    if not result:
      return S_ERROR('Null result from genCatalog utility')
    if not type(result)==type({}):
      return S_ERROR('Non-dict object returned from genCatalog utility')
    if not result.has_key('PFNs'):
      self.log.error('----------BK-Result------------')
      self.log.error(result)
      self.log.error('--------End-BK-Result----------')
      return S_ERROR('Missing key PFNs from genCatalog utility')

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
      return S_ERROR('genCatalog did not return all of original input data requirement')
    ancestorFiles = []
    for i in newInputData:
      if not i in inputData:
        ancestorFiles.append(i)

    param = '%s ancestor files retrieved from genCatalog utility' %(len(ancestorFiles))
    report = self.setJobParam(job,self.optimizerName,param)
    if not report['OK']:
      self.log.warn(report['Message'])

    if not ancestorFiles:
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

    result = self.jobDB.setJobJDL(job,jdl)
    if not result['OK']:
      self.log.warn(result['Message'])
      return S_ERROR('Setting New JDL')

    return S_OK('Job updated')

  #EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
