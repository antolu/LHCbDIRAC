########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/LHCbSystem/Agent/BKInputDataAgent.py,v 1.2 2009/03/13 11:29:59 acsmith Exp $
# File :   InputDataAgent.py
# Author : Stuart Paterson
########################################################################

"""   The BK Input Data Agent queries the bookkeeping for specified job input data and adds the
      file GUID to the job optimizer parameters.

"""

__RCSID__ = "$Id: BKInputDataAgent.py,v 1.2 2009/03/13 11:29:59 acsmith Exp $"

from DIRAC.WorkloadManagementSystem.Agent.OptimizerModule  import OptimizerModule
from DIRAC.Core.DISET.RPCClient                            import RPCClient
from DIRAC                                                 import gConfig, S_OK, S_ERROR

import os, re, time, string

class BKInputDataAgent(OptimizerModule):

  #############################################################################
  def initializeOptimizer(self):
    """Initialize specific parameters for BKInputDataAgent.
    """
    self.failedMinorStatus = self.am_getOption( '/FailedJobStatus', 'BK Input Data Not Available' )

    #Define the shifter proxy needed
    self.am_setModuleParam( "shifterProxy", "ProductionManager" )
    
    self.bkClient = RPCClient('Bookkeeping/BookkeepingManager')
    return S_OK()

  #############################################################################
  def checkJob(self,job,classAdJob):
    """This method controls the checking of the job.
    """

    result = self.jobDB.getInputData(job)
    if not result['OK']:
      self.log.warn('Failed to get input data from JobdB for %s' % (job) )
      self.log.warn(result['Message'])
      return result
    if not result['Value']:
      self.log.verbose('Job %s has no input data requirement' % (job) )
      return self.setNextOptimizer(job)

    self.log.verbose('Job %s has an input data requirement and will be processed' % (job))
    inputData = result['Value']
    result = self.__resolveInputData(job,inputData)
    if not result['OK']:
      self.log.warn( result['Message'] )
      return result
    resolvedData = result['Value']
    result = self.setOptimizerJobInfo(job,self.am_getModuleParam( 'optimizerName' ),resolvedData)
    if not result['OK']:
      self.log.warn( result['Message'] )
      return result
    return self.setNextOptimizer(job)

  #############################################################################
  def __resolveInputData( self, job, inputData ):
    """This method checks the file catalogue for replica information.
    """
    lfns = [string.replace(fname,'LFN:','') for fname in inputData]
    start = time.time()
    res = self.bkClient.getFileMetadata(lfns) 
    timing = time.time() - start
    self.log.info('BK Lookup Time: %.2f seconds ' % (timing) )
    if not res['OK']:
      self.log.warn(res['Message'])
      return res

    bkFileMetadata = res['Value']
    badLFNCount = 0
    badLFNs = []
    bkGuidDict = {}
    for lfn in lfns:
      if not bkFileMetadata.has_key(lfn):
        badLFNCount+=1
        badLFNs.append('BK:%s Problem: %s' %(lfn,'File does not exist in the BK'))
     
    if badLFNCount:
      self.log.info('Found %s problematic LFN(s) for job %s' % (badLFNCount,job) )
      param = string.join(badLFNs,'\n')
      self.log.info(param)
      result = self.setJobParam(job,self.am_getModuleParam( 'optimizerName' ),param)
      if not result['OK']:
        self.log.warn(result['Message'])
      return S_ERROR(self.failedMinorStatus)

    result = {}
    result['Value'] = bkFileMetadata
    result['SiteCandidates'] = {}
    return S_OK(result)

  #EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
