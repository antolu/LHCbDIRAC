########################################################################
# $Id: UploadLogFile.py,v 1.1 2008/11/08 11:56:43 acsmith Exp $
########################################################################

""" UploadLogFile module is used to upload the files present in the workingg directory
"""

__RCSID__ = "$Id: UploadLogFile.py,v 1.1 2008/11/08 11:56:43 acsmith Exp $"

from DIRAC.RequestManagementSystem.Client.RequestContainer import RequestContainer
from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager
from DIRAC.DataManagementSystem.Client.StorageElement import StorageElement
from DIRAC import S_OK, S_ERROR, gLogger, gConfig
from WorkflowLib.Module.ModuleBase import ModuleBase
import os,shutil

class UploadLogFile(ModuleBase):

  def __init__(self):

    #####################################################
    # Variables supposed to be set by the workflow

    self.version = __RCSID__

    self.PRODUCTION_ID = None
    self.JOB_ID = None
    self.workflow_commons = None
    self.request = None

    self.logSE = gConfig.getValue('/Resources/StorageElements/ProductionLogSE','LogSE')
    self.root = gConfig.getValue('/LocalSite/Root',os.getcwd())
    self.site = gConfig.getValue('/LocalSite/Site','localSite')

    self.rm = ReplicaManager()
    self.log = gLogger.getSubLogger('UploadLogFile')
    self.log.setLevel('debug')

######################################################################
  def resolveInputVariables(self):

    if self.workflow_commons.has_key('LogFilePath'):
      self.logFilePath =  self.workflow_commons['LogFilePath']

    if self.workflow_commons.has_key('Request'):
      self.request = self.workflow_commons['Request']
    else:
      self.request = RequestContainer()

######################################################################
  def execute(self):
    """ Main executon method
    """
    # Add global reporting tool
    self.resolveInputVariables()

    result = self.setApplicationStatus('Uploading Logs')
    self.log.info('Initializing %s' % self.version)

    res = shellCall(0,'ls -al')
    if res['OK']:
      self.log.info('The contents of the working directory...')
      self.log.info(str(res['Value'][1]))
    else:
      self.log.error('Failed to list the log directory',str(res['Value'][2]))

    self.log.info('Job root is found to be %s' % (self.root))
    self.log.info('PRODUTION_ID = %s, JOB_ID = %s '  % (self.PRODUCTION_ID, self.JOB_ID))
    self.logdir = os.path.realpath('./job/log/%s/%s' % (self.PRODUCTION_ID, self.JOB_ID))
    self.log.info('Selected log files will be temporarily stored in %s' % self.logdir)

    res = self.finalize()
    return res

##############################################################################
  def finalize(self):
    """ finalize method performs final operations after all the job
        steps were executed. Only production jobs are treated.
    """

    self.log.info('Starting UploadLogFile finalize')
    ##########################################
    # First determine the files which should be saved
    self.log.info('Determining the files to be saved in the logs.')
    res = determineRelevantFiles()
    if not res['OK']:
      self.log.error('Completely failed to select relevant log files.',res['Message'])
      return S_OK()
    selectedFiles = res['Value']
    self.log.info('The following %s files were selected to be saved:' % len(selectedFiles))
    for file in selectedFiles:
      self.log.info(file)

    #########################################
    # Create a temporary directory containing these files
    self.log.info('Populating a temporary directory for selected files.')
    res = populateLogDirectory(selectedFiles)
    if not res['OK']:
      self.log.error('Completely failed to populate temporary log file directory.',res['Message'])
      return S_OK()
    self.log.info('%s populated with log files.' % self.logdir)

    #########################################
    # Attempt to uplaod logs to the LogSE
    self.log.info('Transferring log files to the %s' % self.logSE)
    self.log.info('PutDirectory %s %s %s' % (self.logFilePath, os.path.realpath(self.logdir),self.logSE))
    res = self.rm.putDirectory(self.logFilePath,os.path.realpath(self.logdir),self.logSE)
    if res['OK']:
      self.log.info('Successfully upload log directory to %s' % self.logSE)
      # TODO: The logURL should be constructed using the LogSE and StorageElement()
      #storageElement = StorageElement(self.logSE)
      #pfn = storageElement.getPfnForLfn(self.logFilePath)['Value']
      #logURL = getPfnForProtocol(res['Value'],'http')['Value']
      logURL = '<a href="http://lhcb-logs.cern.ch/storage%s">Log file directory</a>' % self.logFilePath
      self.setJobParameter('Log URL',logURL)
      self.log.info('Logs for this job may be retrieved from %s' % logURL)
      return S_OK()

    #########################################
    # Recover the logs to a failover storage element
    self.log.error('Completely failed to upload log files to %s.' % self.logSE,res['Message'])
    self.log.info('Uploading logs to failover Storage Element')

    ######### REALLY NASTY TEMPORARY CODE #######################
    # TODO: Use tar module if available
    tarFileDir = os.path.dirname(self.logdir)
    tarFileName = '%s.tar.gz' % tarFileDir
    comm = 'tar czvf %s %s' % (tarFileName,self.logdir)
    res = shellCall(0,comm)
    if not res['OK']:
      self.log.error('Failed to create tar file from directory','%s %s' % (self.logdir,res['Message']))
      return S_OK()
    ############################################################

    res = self.uploadFileToFailover(tarFileName,self.logFilePath)
    if not res['OK']:
      self.log.error('Failed to upload logs to all destinations')
      return S_OK()
    res = createLogUploadRequest(self.logSE,logFileLFN)
    if not res['OK']:
      self.log.error('Failed to create failover request', res['Message'])
    else:
      self.log.info('Successfully created failover request')
    return S_OK()

################################################################################
  def determineRelevantFiles(self):
    """ The files which are below a configurable size will be stored in the logs.
        This will typically pick up everything in the working directory minus the output data files.
    """
    # TODO: Get the file size from the CS
    maximumFileSize = 5*(1024*1024) # 5MB
    candidateFiles = os.listdir('.')
    selectedFiles = []
    try:
      for candidate in candidateFiles:
        fileSize = os.stat(candidate)[6]
        if fileSize < maximumFileSize:
          selectedFiles.append(candidate)
      return S_OK(selectedFiles)
    except Exception,x:
      self.log.exception('Exception while determining files to save.','',str(x))
      return S_ERROR()

################################################################################
  def populateLogDirectory(self,selectedFiles):
    """ A temporary directory is created for all the selected files.
        These files are then copied into this directory before being uploaded
    """
    # Create the temporary directory
    try:
      if not os.path.exists(self.logdir):
        os.makedirs(self.logdir)
    except Exception,x:
      self.log.exception('Exception while trying to create directory.',self.logdir,str(x))
      return S_ERROR()
    # Populate the temporary directory
    try:
      for file in selectedFiles:
        destinationFile = '%s/%s' % (self.logdir,os.path.basename(file))
        shutil.copy (file,destinationFile)
    except Exception,x:
      self.log.exception('Exception while trying to copy file.',file,str(x))
      self.log.info('This file will be skipped and can be considered lost.')
    # Now verify the contents of our target log dir
    successfulFiles = os.listdir(self.logdir)
    if len(successfulFiles) == 0:
      self.log.info('Failed to copy any files to the target directory.')
      return S_ERROR()
    else:
      self.log.info('Prepared %s files in the temporary directory.' % self.logdir)
      return S_OK()


################################################################################
  def uploadFileToFailover(self,localFile,logicalFileName,guid=None):
    """ This method will upload the localFile supplied to a failover storage element with the supplied logical file name
    """
    failoverStorageElements = gConfig.getValue('/Resources/StorageElementGroups/Tier1-Failover',[])
    self.log.info('Found %s possible failover Storage Elements.' % len(failoverStorageElements))
    random.shuffle(failoverStorageElements)
    for storageElement in failoverStorageElements:
      self.log.info('Attempting to upload to %s.' % storageElement)
      res = self.uploadDataFileToSE(localFile,logicalFileName,storageElement,guid,failover=True)
      if res['OK']:
        return res
    self.log.error('Failed to upload file to all failover Storage Elements.')
    return S_ERROR()

################################################################################
  def uploadDataFileToSE(self,localFile,logicalFileName,storageElement,guid,failover=False):
    """ This method uploads the supplied local file to the supplied Storage Element with the supplied LFN.
    """
    catalog = False
    if failover:
      catalog = 'LcgFileCatalogCombined'
    res = self.rm.putAndRegister(logicalFileName,localFile,storageElement,guid=guid,catalog=catalog)
    # In the event that the upload failed
    if not res['OK'] or not res['Value']['Successful'].has_key(logicalFileName):
      self.log.error('Failed to putAndRegisterFile','%s %s %s' % (localFile,logicalFileName,storageElement))
      return S_ERROR()
    self.log.info('Successfully put file in %s seconds.' % diracSE,res['Value']['Successful'][lfn]['put'])
    # In the event that there were no failed registrations
    if not res['Value']['Failed'].has_key(logicalFileName):
      self.log.info('Successfully registered in catalogs')
      return S_OK()
    # In the event that some of the registration operations failed
    for catalog in res['Value']['Failed'][logicalFileName].keys():
      # TODO: Create registration requests for the failed registrations
      self.log.error('DO SOMETHING HERE!!!!!!!!!!!!!!!!!!!!!!!!')
    return S_OK()

################################################################################
  def createLogUploadRequest(self,targetSE,logFileLFN):
    """ Set a request to upload job log files from the output sandbox
    """
    res = self.request.addSubRequest({'Attributes':{'Operation':'uploadLogFiles',
                                                       'TargetSE':targetSE,
                                                      'ExecutionOrder':0}},
                                         'logupload')
    if not res['OK']:
      return res
    index = res['Value']
    fileDict = {}
    fileDict['Status'] = 'Waiting'
    fileDict['LFN'] = logFileLFN
    result = self.request.setSubRequestFiles(index,'logupload',[fileDict])
    return S_OK()