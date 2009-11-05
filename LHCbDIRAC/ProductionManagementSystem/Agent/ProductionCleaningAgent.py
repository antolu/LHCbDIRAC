########################################################################
# $HeadURL$
########################################################################
__RCSID__   = "$Id$"
__VERSION__ = "$Revision: 1.5 $"

from DIRAC                                                     import S_OK, S_ERROR, gConfig, gMonitor, gLogger, rootPath
from DIRAC.Core.Base.AgentModule                               import AgentModule
from DIRAC.DataManagementSystem.Client.ReplicaManager          import ReplicaManager
from DIRAC.ProductionManagementSystem.Client.ProductionClient  import ProductionClient
from DIRAC.RequestManagementSystem.Client.RequestClient        import RequestClient
from DIRAC.WorkloadManagementSystem.Client.WMSClient           import WMSClient
from DIRAC.BookkeepingSystem.Client.BookkeepingClient          import BookkeepingClient
from DIRAC.Core.Utilities.List                                 import sortList,breakListIntoChunks
from DIRAC.Core.Utilities.Shifter                              import setupShifterProxyInEnv
from datetime                                                  import datetime,timedelta

import re, os

AGENT_NAME = 'ProductionManagement/ProductionCleaningAgent'

class ProductionCleaningAgent(AgentModule):

  #############################################################################
  def initialize(self):
    """Sets defaults
    """
    self.replicaManager = ReplicaManager()
    self.productionClient = ProductionClient()
    self.wmsClient = WMSClient()
    self.requestClient = RequestClient()
    self.bkClient = BookkeepingClient()
    self.am_setModuleParam("shifterProxy", "DataManager")
    self.am_setModuleParam("shifterProxyLocation","%s/runit/%s/proxy" % (rootPath,AGENT_NAME))
    return S_OK()

  #############################################################################
  def execute(self):
    """ The ProductionCleaningAgent execution method.
    """
    self.enableFlag = self.am_getOption('EnableFlag','True')
    if not self.enableFlag == 'True':
      self.log.info('ProductionCleaningAgent is disabled by configuration option %s/EnableFlag' %(self.section))
      return S_OK('Disabled via CS flag')

    # Obtain the productions in the Deleted status and remove any mention of the jobs/files
    res = self.productionClient.getProductionsWithStatus('Cleaning')
    if not res['OK']:
      gLogger.error("Failed to get Cleaning productions",res['Message'])
    else:
      prods = res['Value']
      if not prods:
        gLogger.info("No productions found in Cleaning status")
      else:
        gLogger.info("Found %d productions in Cleaning status" % len(prods))
        for prodID in sortList(prods):
          self.cleanProduction(int(prodID))

    # Obtain the productions where the remnant output data is to be removed
    res = self.productionClient.getProductionsWithStatus('RemovingFiles')
    if not res['OK']:
      gLogger.error("Failed to get RemovingFiles productions",res['Message'])
    else:
      prods = res['Value']
      if not prods:
        gLogger.info("No productions found in RemovingFiles status")
      else:
        gLogger.info("Found %d productions in RemovingFiles status" % len(prods))
        for prodID in sortList(prods):
          self.removeProductionOutput(int(prodID))

    # Obtain the productions to be archived and remove the jobs in the WMS and production management
    res = self.productionClient.getProductionsWithStatus('Completed')
    if not res['OK']:
      gLogger.error("Failed to get Completed productions",res['Message'])
    else:
      prods = res['Value']
      if not prods:
        gLogger.info("No productions found in Completed status")
      else:
        gLogger.info("Found %d productions in Completed status" % len(prods))
        for prodID in sortList(prods):
          res = self.productionClient.getProductionLastUpdate(prodID)
          if not res['OK']:
            gLogger.error("Failed to get last update time for production %d" % prodID)
          else:
            lastUpdateTime = res['Value']
            timeDelta = timedelta(days=7)
            maxCTime = datetime.utcnow() -  timeDelta
            if lastUpdateTime < maxCTime:
              self.archiveProduction(prodID)

    return S_OK()
  
  #############################################################################
  #
  # These are the functional methods for archiving and cleaning productions
  #

  def archiveProduction(self,prodID):
    """ This just removes job from the jobDB and the production DB
    """
    gLogger.info("Archiving production %s" % prodID)
    # Clean the jobs in the WMS and any failover requests found
    res = self.cleanProductionJobs(prodID)
    if not res['OK']:
      return res
    # Clean the production DB of the files and job information
    res = self.productionClient.cleanProduction(prodID)
    if not res['OK']:
      return res
    gLogger.info("Successfully archived production %d" % prodID)
    # Change the status of the production to archived
    res = self.productionClient.setProductionStatus(prodID,'Archived')
    if not res['OK']:
      gLogger.error("Failed to update status of production %s to Archived" % (prodID),res['Message'])
      return res
    gLogger.info("Updated status of production %s to Archived" % (prodID))
    return S_OK()

  def removeProductionOutput(self,prodID):
    """ This just removes any mention of the output data from the catalog and storage
    """
    gLogger.info("Removing output data for production %s" % prodID)
    res = self.getProductionDirectories(prodID)
    if not res['OK']:
      return res
    directories = res['Value']
    for directory in directories:
      if not re.search('/LOG/',directory):
        res = self.cleanCatalogContents(directory)
        if not res['OK']:
          return res
        res = self.cleanStorageContents(directory)
        if not res['OK']:
          return res
    gLogger.info("Removed directories in the catalog and storage for production")
    # Clean ALL the possible remnants found in the BK
    bkFlag = 'Yes'
    if not directories:
      bkFlag = ''
    res = self.cleanBKFiles(prodID,bkFlag)
    if not res['OK']:
      return res
    gLogger.info("Successfully removed output of production %d" % prodID)
    # Change the status of the production to RemovedFiles
    res = self.productionClient.setProductionStatus(prodID,'RemovedFiles')
    if not res['OK']:
      gLogger.error("Failed to update status of production %s to RemovedFiles" % (prodID),res['Message'])
      return res
    gLogger.info("Updated status of production %s to RemovedFiles" % (prodID))
    return S_OK()

  def cleanProduction(self,prodID):
    """ This removes any mention of the supplied production 
    """
    gLogger.info("Cleaning production %s" % prodID)
    res = self.getProductionDirectories(prodID)
    if not res['OK']:
      return res
    directories = res['Value']
    # Clean the jobs in the WMS and any failover requests found
    res = self.cleanProductionJobs(prodID)
    if not res['OK']:
      return res
    # Clean the production DB of the files and job information
    res = self.productionClient.cleanProduction(prodID)
    if not res['OK']:
      return res
    # Clean the log files for the jobs
    for directory in directories:
      if re.search('/LOG/',directory):
        res = self.cleanProductionLogFiles(directory)
        if not res['OK']:
          return res
      else:
        res = self.cleanCatalogContents(directory)
        if not res['OK']:
          return res
        res = self.cleanStorageContents(directory)
        if not res['OK']:
          return res
    # Clean ALL the possible remnants found in the BK
    bkFlag = 'Yes'
    if not directories:
      bkFlag = ''
    res = self.cleanBKFiles(prodID,'Yes')
    if not res['OK']:
      return res
    gLogger.info("Successfully cleaned production %d" % prodID)
    # Change the status of the production to deleted
    res = self.productionClient.setProductionStatus(prodID,'Deleted')
    if not res['OK']:
      gLogger.error("Failed to update status of production %s to Deleted" % (prodID),res['Message'])
      return res
    gLogger.info("Updated status of production %s to Deleted" % (prodID))
    return S_OK()

  #############################################################################
  #
  # These are the hacks to try and recover the production directories
  #

  def getProductionDirectories(self,prodID):
    """ Get the directories for the supplied productionID from the production management system
    """
    res = self.__getProductionDirectories(prodID)
    if not res['OK']:
      return res
    if not res['Value']:
      self.__createProductionDirectories(prodID)
      res = self.__getProductionDirectories(prodID)
    return res

  def __getProductionDirectories(self,prodID):
    directories = []
    res = self.productionClient.getParameters(prodID,pname='OutputDirectories')
    if res['OK']:
      directories = res['Value'].splitlines()
    elif res['Message'].endswith('not found'):
      gLogger.warn("The production was not found in the production management system.")
    elif res['Message'] == 'Parameter OutputDirectories not found for production':
      gLogger.warn("No output directories available for production.")
    else:
      gLogger.error("Completely failed to obtain production parameters",res['Message'])
      return res
    return S_OK(directories)

  def __createProductionDirectories(self,prodID):
    from DIRAC.LHCbSystem.Client.Production import Production 
    production = Production()
    res = production._setProductionParameters(prodID)

  #############################################################################
  #
  # These are the methods for performing the cleaning of catalogs and storage
  #

  def cleanStorageContents(self,directory):
    storageElements = gConfig.getValue('Resources/StorageElementGroups/Tier1_MC_M-DST',[])
    # Have to add some additional storage elements because:
    # 1: CNAF has to use two different SE types
    # 2: CNAF has to use different namespace for different rentention 
    storageElements.extend(['CNAF_MC-DST','CNAF-RAW'])
    for storageElement in sortList(storageElements):
      res = self.__removeStorageDirectory(directory,storageElement)
      if not res['OK']:
        return res
    return S_OK()

  def __removeStorageDirectory(self,directory,storageElement):
    gLogger.info('Removing the contents of %s at %s' % (directory,storageElement))
    res = self.replicaManager.getPfnForLfn([directory],storageElement)
    if not res['OK']:
      gLogger.error("Failed to get PFN for directory",res['Message'])
      return res
    for directory, error in res['Value']['Failed'].items():
      gLogger.error('Failed to obtain directory PFN from LFN','%s %s' % (directory,error))
    if res['Value']['Failed']:
      return S_ERROR('Failed to obtain directory PFN from LFNs')
    storageDirectory = res['Value']['Successful'].values()[0]   
    res = self.replicaManager.getStorageFileExists(storageDirectory,storageElement,singleFile=True)
    if not res['OK']:
      gLogger.error("Failed to obtain existance of directory",res['Message'])
      return res
    exists = res['Value']
    if not exists:
      gLogger.info("The directory %s does not exist at %s " % (directory,storageElement))
      return S_OK()
    res = self.replicaManager.removeStorageDirectory(storageDirectory,storageElement,recursive=True,singleDirectory=True)
    if not res['OK']:
      gLogger.error("Failed to remove storage directory",res['Message'])
      return res
    gLogger.info("Successfully removed %d files from %s at %s" % (res['Value']['FilesRemoved'],directory,storageElement))
    return S_OK()

  def cleanCatalogContents(self,directory):
    res = self.__getCatalogDirectoryContents([directory])
    if not res['OK']:
      return res
    filesFound = res['Value']
    if not filesFound:
      return S_OK()
    gLogger.info("Attempting to remove %d possible remnants from the catalog and storage" % len(filesFound))
    res = self.replicaManager.removeFile(filesFound)  
    if not res['OK']:
      return res
    for lfn,reason in res['Value']['Failed'].items():
      gLogger.error("Failed to remove file found in the catalog","%s %s" % (lfn,reason))
    if res['Value']['Failed']:
      return S_ERROR("Failed to remove all files found in the catalog")
    return S_OK()

  def __getCatalogDirectoryContents(self,directories):
    gLogger.info('Obtaining the catalog contents for %d directories:' % len(directories))
    for directory in directories:
      gLogger.info(directory)
    activeDirs = directories
    allFiles = {}
    while len(activeDirs) > 0:
      currentDir = activeDirs[0]
      res = self.replicaManager.getCatalogListDirectory(currentDir,singleFile=True)
      activeDirs.remove(currentDir)
      if not res['OK'] and res['Message'].endswith('The supplied path does not exist'):
        gLogger.info("The supplied directory %s does not exist" % currentDir)
      elif not res['OK']:
        gLogger.error('Failed to get directory contents','%s %s' % (currentDir,res['Message']))
      else:
        dirContents = res['Value']
        activeDirs.extend(dirContents['SubDirs'])
        allFiles.update(dirContents['Files'])
    gLogger.info("Found %d files" % len(allFiles))
    return S_OK(allFiles.keys())

  def cleanProductionLogFiles(self,directory):
    gLogger.info("Removing log files found in the directory %s" % directory)
    res = self.replicaManager.removeStorageDirectory(directory,'LogSE',singleDirectory=True)
    if not res['OK']:
      gLogger.error("Failed to remove log files",res['Message'])
      return res
    gLogger.info("Successfully removed production log directory")
    return S_OK()

  def cleanBKFiles(self,prodID,bkFlag=''):
    res = self.bkClient.getProductionFiles(prodID,'ALL',bkFlag) 
    if not res['OK']: 
      return res
    bkMetadata = res['Value']
    fileToRemove = []
    yesReplica = []
    gLogger.info("Found a total of %d files in the BK for production %d" % (len(bkMetadata),prodID))
    for lfn,metadata in bkMetadata.items(): 
      if metadata['FileType'] != 'LOG':
        fileToRemove.append(lfn)
        if metadata['GotReplica'] == 'Yes':
          yesReplica.append(lfn)
    gLogger.info("Attempting to remove %d possible remnants from the catalog and storage" % len(fileToRemove))
    res = self.replicaManager.removeFile(fileToRemove)
    if not res['OK']:
      return res
    for lfn,reason in res['Value']['Failed'].items():
      gLogger.error("Failed to remove file found in BK","%s %s" % (lfn,reason))
    if res['Value']['Failed']:
      return S_ERROR("Failed to remove all files found in the BK")
    if yesReplica:
      gLogger.info("Ensuring that %d files are removed from the BK" % (len(yesReplica))) 
      res = self.replicaManager.removeCatalogFile(yesReplica,catalogs=['BookkeepingDB'])
      if not res['OK']:
        return res
      for lfn,reason in res['Value']['Failed'].items():
        gLogger.error("Failed to remove file from BK","%s %s" % (lfn,reason))
      if res['Value']['Failed']:
        return S_ERROR("Failed to remove all files from the BK")
    gLogger.info("Successfully removed all files found in the BK")
    return S_OK()

  #############################################################################
  #
  # These are the methods for removing the jobs from the WMS and production management 
  #

  def cleanProductionJobs(self,prodID):
    res = self.__getProductionWMSIDs(prodID)
    if not res['OK']:
      return res
    jobIDs = res['Value']
    if jobIDs:
      res = self.__removeWMSJobs(jobIDs)
      if not res['OK']:
        return res
      res = self.__removeFailoverRequests(jobIDs)
      if not res['OK']:
        return res
    return S_OK()

  def __getProductionWMSIDs(self,prodID):
    res = self.productionClient.selectWMSJobs(prodID)
    if not res['OK']:
      gLogger.error("Failed to get WMS job IDs for production %d" % prodID,res['Message'])
      return res
    jobIDs = res['Value'].keys()
    gLogger.info("Found %d jobs for production" % len(jobIDs))
    return S_OK(jobIDs)

  def __removeWMSJobs(self,jobIDs):
    allRemove = True
    for jobList in breakListIntoChunks(jobIDs,500):
      res = self.wmsClient.deleteJob(jobList)
      if res['OK']:
        gLogger.info("Successfully removed %d jobs from WMS" % len(jobList))
      elif (res.has_key('InvalidJobIDs')) and (not res.has_key('NonauthorizedJobIDs')) and (not res.has_key('FailedJobIDs')):
        gLogger.info("Found %s jobs which did not exist in the WMS" % len(res['InvalidJobIDs']))
      elif res.has_key('NonauthorizedJobIDs'):
        gLogger.error("Failed to remove %s jobs because not authorized" % len(res['NonauthorizedJobIDs']))
        allRemove = False
      elif res.has_key('FailedJobIDs'):
        gLogger.error("Failed to remove %s jobs" % len(res['FailedJobIDs']))
        allRemove = False
    if allRemove:
      return S_OK()
    return S_ERROR("Failed to remove all jobs from WMS")

  def __removeFailoverRequests(self,jobIDs):
    res = self.requestClient.getRequestForJobs(jobIDs) 
    if not res['OK']: 
      gLogger.error("Failed to get requestID for jobs.",res['Message']) 
      return res 
    failoverRequests = res['Value']
    gLogger.info("Found %d jobs with associated failover requests" % len(failoverRequests))
    if not failoverRequests:
      return S_OK()
    failed = 0
    for jobID,requestName in failoverRequests.items():
      res = self.requestClient.deleteRequest(requestName) 
      if not res['OK']:
        gLogger.error("Failed to remove request from RequestDB",res['Message']) 
        failed +=1
      else:
        gLogger.verbose("Removed request %s associated to job %d." % (requestName,jobID)) 
    if failed:
      gLogger.info("Successfully removed %s requests" % (len(failoverRequests)-failed))
      gLogger.info("Failed to remove %s requests" % failed)
      return S_ERROR("Failed to remove all the request from RequestDB")
    gLogger.info("Successfully removed all the associated failover requests")
    return S_OK()
