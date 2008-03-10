########################################################################
# $Id: BookkeepingManagerAgent.py,v 1.15 2008/03/10 17:13:00 zmathe Exp $
########################################################################

""" 
BookkeepingManager agent process the ToDo directory and put the data to Oracle database.   
"""

__RCSID__ = "$Id: BookkeepingManagerAgent.py,v 1.15 2008/03/10 17:13:00 zmathe Exp $"

AGENT_NAME = 'Bookkeeping/BookkeepingManagerAgent'

from DIRAC.Core.Base.Agent                                                        import Agent
from DIRAC                                                                        import S_OK, S_ERROR
from DIRAC.BookkeepingSystem.Agent.XMLReader.XMLFilesReaderManager                import XMLFilesReaderManager
from DIRAC.BookkeepingSystem.Agent.ErrorReporterMgmt.ErrorReporterMgmt            import ErrorReporterMgmt
from DIRAC.BookkeepingSystem.Agent.DataMgmt.AMGABookkeepingDatabaseClient         import AMGABookkeepingDatabaseClient
from DIRAC.BookkeepingSystem.Agent.XMLReader.Replica.Replica                      import Replica
from DIRAC.BookkeepingSystem.Agent.XMLReader.Replica.ReplicaParam                 import ReplicaParam
from DIRAC.ConfigurationSystem.Client.Config                                      import gConfig
from DIRAC.BookkeepingSystem.Agent.FileSystem.FileSystemClient                    import FileSystemClient
from DIRAC.DataManagementSystem.Client.Catalog.LcgFileCatalogCombinedClient       import LcgFileCatalogCombinedClient
import os

class BookkeepingManagerAgent(Agent):

  #############################################################################
  def __init__(self):
    """ Constructor, takes system flag as argument.
    """
    Agent.__init__(self, AGENT_NAME)

  #############################################################################
  def initialize(self):
    """Initialize specific parameters for BookkeepingManageAgent.
    """
    result = Agent.initialize(self)
    
    self.xmlMgmt_ = XMLFilesReaderManager()
    self.errorMgmt_ = ErrorReporterMgmt()
    self.dataManager_ =  AMGABookkeepingDatabaseClient()
    self.fileClient_ = FileSystemClient()
    self.lcgFileCatalogClient_ = LcgFileCatalogCombinedClient()
    baseDir = gConfig.getValue(self.section+"/XMLProcessing", "/opt/bookkeeping/XMLProcessing/")
    self.done_ = baseDir + "Done/"
  
    return result

  #############################################################################
  def execute(self):
    self.log.info("BookkeepingAgent running!!!")
    
    self.xmlMgmt_.initialize()
    jobs = self.xmlMgmt_.getJobs()
    
    for job in jobs:
      result = self.__processJob(job)
      if result['OK']:
        self.__moveFileToDoneDirectory(job.getFileName())
    
    replicas = self.xmlMgmt_.getReplicas()
    for replica in replicas:
      result = self.__processReplicas(replica)
      if result['OK']:
        self.__moveFileToDoneDirectory(replica.getFileName())
    
    self.xmlMgmt_.destroy()
    
    return S_OK()

  #############################################################################
  def __processJob(self, job):
    """
    
    """
    deleteFileName = job.getFileName()
    
    self.log.info("Processing: " + deleteFileName)

    ##checking
    inputFiles = job.getJobInputFiles()
    for file in inputFiles:
      name = file.getFileName()
      result = self.dataManager_.file(name)
      if not result['OK']:
        self.errorMgmt_.reportError (10, "The file " + str(file) + " is missing.", deleteFileName)
        return S_ERROR()
      else:
        fileID = int(result['Value'])
        file.setFileID(fileID)
    
    outputFiles = job.getJobOutputFiles()
    for file in outputFiles:
      name = file.getFileName()
      result = self.dataManager_.file(name)
      if result['OK']:
        self.errorMgmt_.reportError (11, "The file " + str(name) + " already exists.", deleteFileName)
        return S_ERROR()
      
      typeName = file.getFileType()
      typeVersion = file.getFileVersion()
      result = self.dataManager_.fileTypeAndFileTypeVersion(typeName, typeVersion)
      if not result['OK']:
        self.errorMgmt_.reportError (12, "The type " + str(typeName) + ", version " + str(typeVersion) +"is missing.\n", deleteFileName)
        return S_ERROR()
      else:
        typeID = int(result['Value'])
        file.setTypeID(typeID)
      
      params = file.getFileParams()
      for param in params:
        paramName = param.getParamName()
        if paramName == "GUID":
          param.setParamName("lhcb_" + paramName)
        if paramName == "EventType":
          value = int(param.getParamValue())
          result = self.dataManager_.eventType(value)
          if not result['OK']:
            self.errorMgmt_.reportError (13, "The event type " + str(value) + " is missing.\n", deleteFileName)
            return S_ERROR()
      ################
      
        
    config = job.getJobConfiguration()
    result = self.dataManager_.insertJob(config.getConfigName(), config.getConfigVersion(), config.getDate())    
    if not result['OK']:
      self.errorMgmt_.reportError (13, "Unable to create Job : " + str(config.getConfigName()) + ", " + str(config.getConfigVersion()) + ", " + str(config.getDate()) + ".\n", deleteFileName)
      return S_ERROR()
    else:
      jobID = int(result['Value'])
      job.setJobId(jobID)
    
    params = job.getJobParams()
    for param in params:
      result = self.dataManager_.insertJobParameter(job.getJobId(), param.getName(), param.getValue(), param.getType())
      if not result['OK']:
        return S_ERROR()
    
    inputFiles = job.getJobInputFiles()
    for file in inputFiles:
      result = self.dataManager_.insertInputFile(job.getJobId(), file.getFileID())
      if not result['OK']:
        self.errorMgmt_.reportError (16, "Unable to add " + str(file.getFileName()), deleteFileName )
        return S_ERROR()
      
  
    outputFiles = job.getJobOutputFiles()
    for file in outputFiles:
      result = self.dataManager_.insertOutputFile(job.getJobId(), file.getFileName(), file.getTypeID())
      if not result['OK']:
        self.errorMgmt_.reportError (17, "Unable to create file " + str(file.getFileName()) + "!", deleteFileName)
        return S_ERROR()
      else:
        id = int(result['Value'])
        file.setFileID(id)
        
      params = file.getFileParams()
      for param in params:
        result = self.dataManager_.insertFileParam(file.getFileID(), param.getParamName(), param.getParamValue())
        if not result['OK']:
          return S_ERROR()
      
      qualities = file.getQualities()
      for quality in qualities:
        group = quality.getGroup()
        flag = quality.getFlag()
        result = self.dataManager_.insertQuality(file.getFileID(), group, flag)           
        if not result['OK']:
          self.errorMgmt_.reportError(19, "Unable to create Quality " + str(group) + "/" + str(flag) + "\" for file " + str(file.getFileName()) + ".\n", deleteFileName)                                 
          return S_ERROR()
        else:
          qualityID = int(result['Value'])
          quality.setQualityID(qualityID)
          
        params = quality.getParams()
        for param in params:
           name = param.getName()
           value = param.getValue()
           result = self.dataManager_.insertQualityParam(file.getFileID(), quality.getQualityID(), name, value)
           if not result['OK']:
             self.errorMgmt_.reportError (20, "Unable to create Parameter \"" + str(name) + " = " + str(value) + "\" for quality " + group + "/" + flag + "\".\n", deleteFileName)
             return S_ERROR()
                   
      replicas = file.getReplicas()
      for replica in replicas:
        params = replica.getaprams()
        for param in params: # just one param exist in params list, because JobReader only one param add to Replica
          name = param.getName()
          location = param.getLocation()
        result = self.dataManager_.insertReplica(file.getFileID(), name, location)           
        if not result['OK']:
          self.errorMgmt_.reportError(21, "Unable to create Replica " + str(name) + " (in " + str(location) + ") for file " + str(file.getFileName()) + ".\n", deleteFileName)
          return S_ERROR()
    
    self.log.info("End Processing: " + deleteFileName)
    
    return S_OK()
  
  #############################################################################
  def __processReplicas(self, replica):
    """
    
    """
    file = replica.getFileName()
    self.log.info("Processing replicas: " + str(file))
    fileID = -1
    
    params = replica.getaprams()
    delete = True
    replicaFileName = ""
    for param in params:
      name = param.getName()
      replicaFileName = param.getFile()
      location = param.getLocation()
      delete = param.getAction() == "Delete"
       
      result = self.dataManager_.file(replicaFileName)
      if not result['OK']:
        message = "No replica can be ";
        if (delete):
           message += "removed" 
        else:
           message += "added"
        message += " to file " + str(file) + " for " + str(location) + ".\n"
        self.errorMgmt_.reportError(23, message, file)
        return S_ERROR()
      else:
        fileID = int(result['Value'])
          
      if (delete):    
        list = self.lcgFileCatalogClient_.getReplicas(replicaFileName)
        if len(list) == 0:
          result = self.dataManager_.modifyReplica(fileID, "Got_Replica", "no")
          if not result['OK']:
            gLogger.warn("Unable to set the Got_Replica flag for " + str(replicaFileName))
            self.errorMgmt_.reportError(26, "Unable to set the Got_Replica flag for " + str(replicaFileName), file)
            return S_ERROR()
      else:
        result = self.dataManager_.modifyReplica(fileID, "Got_Replica", "yes")
        if not result['OK']:
          self.errorMgmt_.reportError(26, "Unable to set the Got_Replica flag for " + str(replicaFileName), file)
          return S_ERROR()
   
  
    self.log.info("End Processing replicas!")
    
    return S_OK()
  
  #############################################################################
  def __moveFileToDoneDirectory(self, fileName):
    name = os.path.split(fileName)[1]
    self.fileClient_.rename(fileName, self.done_+name)
        
      
              
      
    
  
