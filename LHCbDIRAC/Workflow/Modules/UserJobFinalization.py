########################################################################
# $Id: UserJobFinalization.py,v 1.17 2009/07/29 14:06:24 paterson Exp $
########################################################################
""" Module to upload specified job output files according to the parameters
    defined in the user workflow.
     
    UNDER DEVELOPMENT
"""

__RCSID__ = "$Id: UserJobFinalization.py,v 1.17 2009/07/29 14:06:24 paterson Exp $"

from WorkflowLib.Module.ModuleBase                         import *
from DIRAC.DataManagementSystem.Client.FailoverTransfer    import FailoverTransfer
from DIRAC.RequestManagementSystem.Client.RequestContainer import RequestContainer
from DIRAC                                                 import S_OK, S_ERROR, gLogger, gConfig
import DIRAC

try:
  from LHCbDIRAC.Core.Utilities.ProductionData  import constructUserLFNs
except Exception,x:
  from DIRAC.LHCbSystem.Utilities.ProductionData  import constructUserLFNs

import string,os,random

class UserJobFinalization(ModuleBase):

  #############################################################################
  def __init__(self):
    """Module initialization.
    """
    ModuleBase.__init__(self)
    self.version = __RCSID__
    self.log = gLogger.getSubLogger( "UserJobFinalization" )
    self.commandTimeOut = 30*60
    self.rm = ReplicaManager()
    self.jobID = ''
    self.enable=True
    self.failoverTest=False #flag to put file to failover SE by default
    self.failoverSEs = gConfig.getValue('/Resources/StorageElementGroups/Tier1-Failover',[])
    #List all parameters here
    self.userFileCatalog='LcgFileCatalogCombined'
    self.request = None

  #############################################################################
  def resolveInputVariables(self):
    """ By convention the module parameters are resolved here.
    """
    self.log.verbose(self.workflow_commons)
    self.log.verbose(self.step_commons)

    if self.step_commons.has_key('Enable'):
      self.enable=self.step_commons['Enable']
      if not type(self.enable)==type(True):
        self.log.warn('Enable flag set to non-boolean value %s, setting to False' %self.enable)
        self.enable=False

    if self.step_commons.has_key('TestFailover'):
      self.enable=self.step_commons['TestFailover']
      if not type(self.failoverTest)==type(True):
        self.log.warn('Test failover flag set to non-boolean value %s, setting to False' %self.failoverTest)
        self.failoverTest=False

    if os.environ.has_key('JOBID'):
      self.jobID = os.environ['JOBID']
      self.log.verbose('Found WMS JobID = %s' %self.jobID)
    else:
      self.log.info('No WMS JobID found, disabling module via control flag')
      self.enable=False

    if self.workflow_commons.has_key('Request'):
      self.request = self.workflow_commons['Request']
    else:
      self.request = RequestContainer()
      self.request.setRequestName('job_%s_request.xml' % self.jobID)
      self.request.setJobID(self.jobID)
      self.request.setSourceComponent("Job_%s" % self.jobID)

    #Use LHCb utility for local running via dirac-jobexec
    if self.workflow_commons.has_key('UserOutputData'):
        self.userOutputLFNs = self.workflow_commons['UserOutputData']
        if not type(self.userOutputLFNs)==type([]):
          self.userOutputLFNs = [i.strip() for i in self.userOutputLFNs.split(';')]
    else:
      self.log.info('UserOutputData parameter not found, creating on the fly')
      result = constructUserLFNs(self.workflow_commons)
      if not result['OK']:
        self.log.error('Could not create production LFNs',result['Message'])
        return result
      self.userOutputLFNs=result['Value']['UserOutputData']

    return S_OK('Parameters resolved')

  #############################################################################
  def execute(self):
    """ Main execution function.
    """
    self.log.info('Initializing %s' %self.version)

    result = self.resolveInputVariables()
    if not result['OK']:
      self.log.error(result['Message'])
      return result

    if not self.workflowStatus['OK'] or not self.stepStatus['OK']:
      self.log.verbose('Workflow status = %s, step status = %s' %(self.workflowStatus['OK'],self.stepStatus['OK']))
      return S_OK('No output data upload attempted')
    
    #TODO: resolve the behaviour for user stuff
    
    #Determine the final list of possible output files for the
    #workflow and all the parameters needed to upload them.
    result = self.getCandidateFiles(self.outputList,self.userOutputLFNs,self.outputDataFileMask)
    if not result['OK']:
      self.setApplicationStatus(result['Message'])
      return result
    
    fileDict = result['Value']      
    result = self.getFileMetadata(fileDict)
    if not result['OK']:
      self.setApplicationStatus(result['Message'])
      return result

    if not result['Value']:
      self.log.info('No output data files were determined to be uploaded for this workflow')
      return S_OK()

    fileMetadata = result['Value']

    #Get final, resolved SE list for files
    final = {}
    for fileName,metadata in fileMetadata.items():
      result = getDestinationSEList(metadata['workflowSE'],DIRAC.siteName(),self.outputMode)
      if not result['OK']:
        self.log.error('Could not resolve output data SE',result['Message'])
        self.setApplicationStatus('Failed To Resolve OutputSE')
        return result
      
      final[fileName]=metadata
      final[fileName]['resolvedSE']=result['Value']

    #At this point can exit and see exactly what the module will upload
    if not self.enable:
      self.log.info('Module is disabled by control flag, would have attempted to upload the following files %s' %string.join(final.keys(),', '))
      for fileName,metadata in final.items():
        self.log.info('--------%s--------' %fileName)
        for n,v in metadata.items():
          self.log.info('%s = %s' %(n,v))

      return S_OK('Module is disabled by control flag')

    #Instantiate the failover transfer client with the global request object
    failoverTransfer = FailoverTransfer(self.request)

    #One by one upload the files with failover if necessary
    failover = {}
    if not self.failoverTest:
      for fileName,metadata in final.items():
        self.log.info("Attempting to store file %s to the following SE(s):\n%s" % (fileName, string.join(metadata['resolvedSE'],', ')))
        result = failoverTransfer.transferAndRegisterFile(fileName,metadata['localpath'],metadata['lfn'],metadata['resolvedSE'],fileGUID=metadata['guid'],fileCatalog=None)
        if not result['OK']:
          self.log.error('Could not transfer and register %s with metadata:\n %s' %(fileName,metadata))
          failover[fileName]=metadata 
    else:
      failover = final

    cleanUp = False
    for fileName,metadata in failover.items():
      self.log.info('Setting default catalog for failover transfer to LcgFileCatalogCombined')
      random.shuffle(self.failoverSEs)
      targetSE = metadata['resolvedSE'][0]
      metadata['resolvedSE']=self.failoverSEs
      result = failoverTransfer.transferAndRegisterFileFailover(fileName,metadata['localpath'],metadata['lfn'],targetSE,metadata['resolvedSE'],fileGUID=metadata['guid'],fileCatalog='LcgFileCatalogCombined')
      if not result['OK']:
        self.log.error('Could not transfer and register %s with metadata:\n %s' %(fileName,metadata))
        cleanUp = True
        break #no point continuing if one completely fails

    #Now after all operations, retrieve potentially modified request object
    result = failoverTransfer.getRequestObject()
    if not result['OK']:
      self.log.error(result)
      return S_ERROR('Could not retrieve modified request')

    self.request = result['Value']

    #If some or all of the files failed to be saved to failover
    if cleanUp:
      lfns = []
      for fileName,metadata in final.items():
         lfns.append(metadata['lfn'])

      result = self.__cleanUp(lfns)
      self.workflow_commons['Request']=self.request
      return S_ERROR('Failed to upload output data')

    self.workflow_commons['Request']=self.request
    return S_OK('Output data uploaded')

  #############################################################################
  def __cleanUp(self,lfnList):
    """ Clean up uploaded data for the LFNs in the list
    """
    # Clean up the current request
    for req_type in ['transfer','register']:
      for lfn in lfnList:
        result = self.request.getNumSubRequests(req_type)
        if result['OK']:
          nreq = result['Value']
          if nreq:
            # Go through subrequests in reverse order in order not to spoil the numbering
            ind_range = [0]
            if nreq > 1:
              ind_range = range(nreq-1,-1,-1)
            for i in ind_range:
              result = self.request.getSubRequestFiles(i,req_type)
              if result['OK']:
                fileList = result['Value']
                if fileList[0]['LFN'] == lfn:
                  result = self.request.removeSubRequest(i,req_type)

    # Set removal requests just in case
    for lfn in lfnList:
      result = self.__setFileRemovalRequest(lfn)

    return S_OK()

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#