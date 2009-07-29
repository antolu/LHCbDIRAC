########################################################################
# $Id: UploadOutputData.py,v 1.17 2009/07/29 14:06:24 paterson Exp $
########################################################################
""" Module to upload specified job output files according to the parameters
    defined in the production workflow.
"""

__RCSID__ = "$Id: UploadOutputData.py,v 1.17 2009/07/29 14:06:24 paterson Exp $"

from WorkflowLib.Module.ModuleBase                         import *
from DIRAC.DataManagementSystem.Client.ReplicaManager      import ReplicaManager
from DIRAC.RequestManagementSystem.Client.RequestContainer import RequestContainer
from DIRAC.DataManagementSystem.Client.PoolXMLFile         import getGUID
from DIRAC.Core.Utilities.File                             import fileAdler
from DIRAC                                                 import S_OK, S_ERROR, gLogger, gConfig
import DIRAC

try:
  from LHCbSystem.Utilities.ProductionData  import constructProductionLFNs
  from LHCbSystem.Utilities.ResolveSE  import getDestinationSEList
except Exception,x:
  from DIRAC.LHCbSystem.Utilities.ProductionData  import constructProductionLFNs
  from DIRAC.LHCbSystem.Utilities.ResolveSE  import getDestinationSEList

import string,os,random

class UploadOutputData(ModuleBase):

  #############################################################################
  def __init__(self):
    """Module initialization.
    """
    ModuleBase.__init__(self)
    self.version = __RCSID__
    self.log = gLogger.getSubLogger( "UploadOutputData" )
    self.commandTimeOut = 10*60
    self.rm = ReplicaManager()
    self.jobID = ''
    self.enable=True
    self.failoverTest=False #flag to put file to failover SE by default
    self.failoverSEs = gConfig.getValue('/Resources/StorageElementGroups/Tier1-Failover',[])
    self.existingCatalogs = []
    result = gConfig.getSections('/Resources/FileCatalogs')
    if result['OK']:
      self.existingCatalogs = result['Value']

    #List all parameters here
    self.outputDataFileMask = ''
    self.outputMode='Any' #or 'Local' for reco case
    self.outputList = []
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

    if self.workflow_commons.has_key('outputList'):
       self.outputList = self.workflow_commons['outputList']

    if self.workflow_commons.has_key('outputMode'):
      self.outputMode = self.workflow_commons['outputMode']

    if self.workflow_commons.has_key('outputDataFileMask'):
        self.outputDataFileMask = self.workflow_commons['outputDataFileMask']
        if not type(self.outputDataFileMask)==type([]):
          self.outputDataFileMask = [i.lower().strip() for i in self.outputDataFileMask.split(';')]

    #Use LHCb utility for local running via jobexec
    if self.workflow_commons.has_key('ProductionOutputData'):
        self.prodOutputLFNs = self.workflow_commons['ProductionOutputData']
        if not type(self.prodOutputLFNs)==type([]):
          self.prodOutputLFNs = [i.strip() for i in self.prodOutputLFNs.split(';')]
    else:
      self.log.info('ProductionOutputData parameter not found, creating on the fly')
      result = constructProductionLFNs(self.workflow_commons)
      if not result['OK']:
        self.log.error('Could not create production LFNs',result['Message'])
        return result
      self.prodOutputLFNs=result['Value']['ProductionOutputData']

    return S_OK('Parameters resolved')

  #############################################################################
  def execute(self):
    """ Main execution function.
    """
    self.log.info('Initializing %s' %self.version)
    if DIRAC.siteName() == 'DIRAC.ONLINE-FARM.ch':
      return S_OK()
    result = self.resolveInputVariables()
    if not result['OK']:
      self.log.error(result['Message'])
      return result

    if not self.workflowStatus['OK'] or not self.stepStatus['OK']:
      self.log.verbose('Workflow status = %s, step status = %s' %(self.workflowStatus['OK'],self.stepStatus['OK']))
      return S_OK('No output data upload attempted')

    #Determine the final list of possible output files for the
    #workflow and all the parameters needed to upload them.
    result = self.__getFileMetadata()
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

    #One by one upload the files with failover if necessary
    failover = {}
    if not self.failoverTest:
      for fileName,metadata in final.items():
        result = self.__transferAndRegisterFile(fileName,metadata)
        if not result['OK']:
          self.log.error('Could not transfer and register %s with metadata:\n %s' %(fileName,metadata))
          failover[fileName]=metadata
    else:
      failover = final

    cleanUp = False
    for fileName,metadata in failover.items():
      result = self.__transferAndRegisterFileFailover(fileName,metadata)
      if not result['OK']:
        self.log.error('Could not transfer and register %s with metadata:\n %s' %(fileName,metadata))
        cleanUp = True
        break #no point continuing if one completely fails

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
  def __transferAndRegisterFile(self,fileName,metadata,failover=False):
    """Performs the transfer and register operation given all necessary file
       metadata.
    """
    fileCatalog=None
    if failover:
      fileCatalog='LcgFileCatalogCombined'
      self.log.info('Setting default catalog for failover transfer to LcgFileCatalogCombined')

    outputSEList = metadata['resolvedSE']
    self.log.info("Attempting to store file %s to the following SE(s):\n%s" % (fileName, string.join(outputSEList,', ')))
    localPath = '%s/%s' %(os.getcwd(),fileName)
    lfn = metadata['lfn']
    errorList = []

    for se in outputSEList:
      self.log.info('Attempting rm.putAndRegister("%s","%s","%s",guid="%s",catalog="%s")' %(lfn,localPath,se,metadata['guid'],fileCatalog))
      result = self.rm.putAndRegister(lfn,localPath,se,guid=metadata['guid'],catalog=fileCatalog)
      self.log.verbose(result)
      if not result['OK']:
        self.log.error('rm.putAndRegister failed with message',result['Message'])
        errorList.append(result['Message'])
        continue

      if not result['Value']['Failed']:
        self.log.info('rm.putAndRegister successfully uploaded %s to %s' %(fileName,se))
        metadata['uploadedSE']=se
        return S_OK(metadata)
      #Now we know something went wrong
      errorDict = result['Value']['Failed'][lfn]
      if not errorDict.has_key('register'):
        self.log.error('rm.putAndRegister failed with unknown error',str(errorDict))
        errorList.append('Unknown error while attempting upload to %s' %se)
        continue

      fileDict = errorDict['register']
      #Therefore the registration failed but the upload was successful
      result = self.__setRegistrationRequest(se,'',fileDict)
      if not result['OK']:
        self.log.error('Failed to set registration request for: SE %s and metadata: \n%s' %(se,fileDict))
        errorList.append('Failed to set registration request for: SE %s and metadata: \n%s' %(se,fileDict))
        continue
      else:
        self.log.info('Successfully set registration request for: SE %s and metadata: \n%s' %(se,fileDict))
        metadata['filedict']=fileDict
        metadata['uploadedSE']=se
        return S_OK(metadata)

    self.log.error('Encountered %s errors during attempts to upload output data' %len(errorList))
    return S_ERROR('Failed to upload output data file')

  #############################################################################
  def __transferAndRegisterFileFailover(self,fileName,metadata):
    """Performs the transfer and register operation to failover storage and sets the
       necessary replication and removal requests to recover.
    """
    random.shuffle(self.failoverSEs)
    targetSE = metadata['resolvedSE'][0]
    metadata['resolvedSE']=self.failoverSEs
    failover = self.__transferAndRegisterFile(fileName,metadata,failover=True)
    if not failover['OK']:
      self.log.error('Could not upload file to failover SEs',failover['Message'])
      return failover

    #set removal requests and replication requests
    result = self.__setFileReplicationRequest(failover['Value']['lfn'],targetSE,failover['Value']['filedict'])
    if not result['OK']:
      self.log.error('Could not set file replication request',result['Message'])
      return result

    lfn = failover['Value']['lfn']
    failoverSE = failover['Value']['uploadedSE']
    self.log.info('Attempting to set replica removal request for LFN %s at failover SE %s' %(lfn,failoverSE))
    result = self.__setReplicaRemovalRequest(lfn,failoverSE)
    if not result['OK']:
      self.log.error('Could not set removal request',result['Message'])
      return result

    return S_OK(metadata)

  #############################################################################
  def __setFileReplicationRequest(self,lfn,se,fileDict):
    """ Sets a registration request.
    """
    self.log.info('Setting replication request for %s to %s' % (lfn,se))
    result = self.request.addSubRequest({'Attributes':{'Operation':'replicateAndRegister',
                                                       'TargetSE':se,'ExecutionOrder':0}},
                                         'transfer')
    if not result['OK']:
      return result

    index = result['Value']
    fileDict['Status'] = 'Waiting'
    result = self.request.setSubRequestFiles(index,'transfer',[fileDict])

    return S_OK()

  #############################################################################
  def __setRegistrationRequest(self,se,catalog,fileDict):
    """ Sets a registration request.
    """
    lfn = fileDict['LFN']
    self.log.info('Setting registration request for %s at %s for metadata:\n%s' % (lfn,se,fileDict))
    result = self.request.addSubRequest({'Attributes':{'Operation':'registerFile','ExecutionOrder':0,
                                                       'TargetSE':se,'Catalogue':catalog}},'register')
    if not result['OK']:
      return result

    index = result['Value']
    result = self.request.setSubRequestFiles(index,'register',[fileDict])

    return S_OK()

  #############################################################################
  def __setReplicaRemovalRequest(self,lfn,se):
    """ Sets a removal request for a replica.
    """
    result = self.request.addSubRequest({'Attributes':{'Operation':'replicaRemoval',
                                                       'TargetSE':se,'ExecutionOrder':1}},
                                         'removal')
    index = result['Value']
    fileDict = {'LFN':lfn,'Status':'Waiting'}
    result = self.request.setSubRequestFiles(index,'removal',[fileDict])
    return S_OK()

  #############################################################################
  def __setFileRemovalRequest(self,lfn,se='',pfn=''):
    """ Sets a removal request for a file including all replicas.
    """
    result = self.request.addSubRequest({'Attributes':{'Operation':'removeFile',
                                                       'TargetSE':se,'ExecutionOrder':1}},
                                         'removal')
    index = result['Value']
    fileDict = {'LFN':lfn,'PFN':pfn,'Status':'Waiting'}
    result = self.request.setSubRequestFiles(index,'removal',[fileDict])
    return S_OK()

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

  #############################################################################
  def __getFileMetadata(self):
    """ Get the list of local Storage Element names
    """
    fileInfo = {}
    for outputFile in self.outputList:
      if outputFile.has_key('outputDataType') and outputFile.has_key('outputDataSE') and outputFile.has_key('outputDataName'):
        fname = outputFile['outputDataName']
        fileSE = outputFile['outputDataSE']
        fileType= outputFile['outputDataType']
        fileInfo[fname] = {'type':fileType,'workflowSE':fileSE}
      else:
        self.log.error('Ignoring malformed output data specification',str(outputFile))

    for lfn in self.prodOutputLFNs:
      if os.path.basename(lfn) in fileInfo.keys():
        fileInfo[os.path.basename(lfn)]['lfn']=lfn
        self.log.verbose('Found LFN %s for file %s' %(lfn,os.path.basename(lfn)))

    #Check that the list of output files were produced
    for fileName,metadata in fileInfo.items():
      if not os.path.exists(fileName):
        self.log.error('Output data file %s does not exist locally' %fileName)
        return S_ERROR('Output Data Not Found')

    #Check the list of files against the output file mask (if it exists)
    candidateFiles = {}
    if self.outputDataFileMask:
      for fileName,metadata in fileInfo.items():
        if metadata['type'].lower() in self.outputDataFileMask or fileName.split('.')[-1] in self.outputDataFileMask:
          candidateFiles[fileName]=metadata
        else:
          self.log.info('Output file %s was produced but will not be treated (outputDataFileMask is %s)' %(fileName,string.join(self.outputDataFileMask,', ')))

      if not candidateFiles.keys():
        return S_OK({}) #nothing to do

    #Retrieve the POOL File GUID(s) for any final output files
    self.log.info('Will search for POOL GUIDs for: %s' %(string.join(candidateFiles.keys(),', ')))
    pfnGUID = getGUID(candidateFiles.keys())
    if not pfnGUID['OK']:
      self.log.error('PoolXMLFile failed to determine POOL GUID(s) for output file list, these will be generated by the ReplicaManager',pfnGUID['Message'])
      for fileName in candidateFiles.keys():
        candidateFiles[fileName]['guid']=''
    elif pfnGUID['generated']:
      self.log.error('PoolXMLFile generated GUID(s) for the following files ',string.join(pfnGUID['generated'],', '))
    else:
      self.log.info('GUIDs found for all specified POOL files: %s' %(string.join(candidateFiles.keys(),', ')))

    for pfn,guid in pfnGUID['Value'].items():
      candidateFiles[pfn]['guid']=guid

    #Get all additional metadata about the file necessary for requests
    final = {}
    for fileName,metadata in candidateFiles.items():
      fileDict = {}
      fileDict['LFN'] = metadata['lfn']
      fileDict['Size'] = os.path.getsize(fileName)
      fileDict['Addler'] = fileAdler(fileName)
      fileDict['GUID'] = metadata['guid']
      fileDict['Status'] = "Waiting"
      final[fileName]=metadata
      final[fileName]['filedict']=fileDict

    #Sanity check all final candidate metadata keys are present (return S_ERROR if not)
    mandatoryKeys = ['type','workflowSE','lfn','guid','filedict'] #filedict is used for requests
    for fileName,metadata in final.items():
      for key in mandatoryKeys:
        if not metadata.has_key(key):
          return S_ERROR('File %s has missing %s' %(fileName,key))

    return S_OK(final)

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#