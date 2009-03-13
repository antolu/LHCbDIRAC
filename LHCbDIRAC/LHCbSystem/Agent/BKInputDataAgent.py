########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/LHCbSystem/Agent/BKInputDataAgent.py,v 1.3 2009/03/13 13:45:03 acsmith Exp $
# File :   InputDataAgent.py
# Author : Stuart Paterson
########################################################################

"""   The BK Input Data Agent queries the bookkeeping for specified job input data and adds the
      file GUID to the job optimizer parameters.

"""

__RCSID__ = "$Id: BKInputDataAgent.py,v 1.3 2009/03/13 13:45:03 acsmith Exp $"

from DIRAC.WorkloadManagementSystem.Agent.OptimizerModule  import OptimizerModule
from DIRAC.Core.DISET.RPCClient                            import RPCClient
from DIRAC                                                 import gConfig, S_OK, S_ERROR

import os, re, time, string

class BKInputDataAgent(OptimizerModule):

  #############################################################################
  def initializeOptimizer(self):
    """Initialize specific parameters for BKInputDataAgent.
    """
    self.dataAgentName        = self.am_getOption('InputDataAgent','InputData')

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
    result = self.__determineInputDataIntegrity(job,inputData)
    if not result['OK']:
      self.log.warn( result['Message'] )
      return result
    return self.setNextOptimizer(job)

  #############################################################################
  def __determineInputDataIntegrity( self, job, inputData ):
    """This method checks the mutual consistency of the file catalog and bookkeeping information.
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
    badLFNs = []
    bkGuidDict = {}
    for lfn in lfns:
      if not bkFileMetadata.has_key(lfn):
        badLFNs.append('BK:%s Problem: %s' %(lfn,'File does not exist in the BK'))
     
    if badLFNs:
      self.log.info('Found %s problematic LFN(s) for job %s' % (len(badLFNs),job) )
      param = string.join(badLFNs,'\n')
      self.log.info(param)
      result = self.setJobParam(job,self.am_getModuleParam( 'optimizerName' ),param)
      if not result['OK']:
        self.log.warn(result['Message'])
      return S_ERROR('BK Input Data Not Available')

    res = self.getOptimizerJobInfo(job,self.dataAgentName)
    if not res['OK']:
      self.log.warn(res['Message'])
      return S_ERROR('Failed To Get LFC Metadata')
    lfcMetadataResult = res['Value']
    if not lfcMetadataResult['Value']:
      errStr = 'LFC Metadata Not Available'
      self.log.warn(errStr)
      return S_ERROR(errStr)
    lfcMetadata = res['Value']
    
    badFileCount = 0
    for lfn,lfcMeta in lfcMetadata['Value']['Successful'].items():
      bkMeta = bkFileMetadata[lfn]
      badFile=False
      if lfcMeta['GUID'].upper() != bkMeta['GUID'].upper(): 
        badLFNs.append('BK:%s Problem: %s' %(lfn,'LFC-BK GUID Mismatch'))
        badFile=True
      if int(lfcMeta['Size']) != int(bkMeta['FileSize']):
        badLFNs.append('BK:%s Problem: %s' %(lfn,'LFC-BK File Size Mismatch'))
        badFile=True
      if (lfcMeta['CheckSumType'] == 'AD') and bkMeta['ADLER32']:
        if lfcMeta['CheckSumValue'].upper() != bkMeta['ADLER32'].upper():
          badLFNs.append('BK:%s Problem: %s' %(lfn,'LFC-BK Checksum Mismatch'))
          badFile=True
      if badFile:
        badFileCount +=1
 
    if badLFNs:
      self.log.info('Found %s problematic LFN(s) for job %s' % (badFileCount,job) )
      param = string.join(badLFNs,'\n')
      self.log.info(param)
      result = self.setJobParam(job,self.am_getModuleParam( 'optimizerName' ),param)
      if not result['OK']:
        self.log.warn(result['Message'])
      return S_ERROR('BK-LFC Integrity Check Failed')

    return S_OK()
