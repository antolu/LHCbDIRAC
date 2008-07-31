########################################################################
# $Id: XMLFilesReaderManager.py,v 1.9 2008/07/31 08:18:49 zmathe Exp $
########################################################################

"""

"""

from xml.dom.ext.reader                                                           import Sax
from DIRAC.BookkeepingSystem.Agent.FileSystem.FileSystemClient                    import FileSystemClient
from DIRAC.BookkeepingSystem.Agent.XMLReader.JobReader                            import JobReader
from DIRAC.BookkeepingSystem.Agent.XMLReader.ReplicaReader                        import ReplicaReader
from DIRAC.ConfigurationSystem.Client.Config                                      import gConfig
from DIRAC                                                                        import gLogger, S_OK, S_ERROR
from DIRAC.BookkeepingSystem.DB.BookkeepingDatabaseClient                         import BookkeepingDatabaseClient
#from DIRAC.BookkeepingSystem.Client.BookkeepingClient                             import BookkeepingClient
from DIRAC.DataManagementSystem.Client.Catalog.LcgFileCatalogCombinedClient       import LcgFileCatalogCombinedClient
from DIRAC.BookkeepingSystem.Agent.ErrorReporterMgmt.ErrorReporterMgmt            import ErrorReporterMgmt
import os,sys

__RCSID__ = "$Id: XMLFilesReaderManager.py,v 1.9 2008/07/31 08:18:49 zmathe Exp $"

global dataManager_
dataManager_ = BookkeepingDatabaseClient()

class XMLFilesReaderManager:
  
  #############################################################################
  def __init__(self):
    self.reader_ = Sax.Reader()
    self.fileClient_ = FileSystemClient()
    self.jobReader_ = JobReader()
    self.replicaReader_ = ReplicaReader()
    
    self.baseDir_ = gConfig.getValue("XMLProcessing", "/opt/bookkeeping/XMLProcessing/")
    self.errorsTmp_ = self.baseDir_ + "ErrorsTmp/"
    self.jobs_ = []
    self.replicas_ = []
    #self.dataManager_ = BookkeepingDatabaseClient()
    self.lcgFileCatalogClient_ = LcgFileCatalogCombinedClient()
    self.errorMgmt_ = ErrorReporterMgmt()
    
  
  #############################################################################
  def readFile(self, filename):
    """
    
    """
    try:
      stream = open(filename)
      doc = self.reader_.fromStream(stream)
      stream.close()
    

      docType = doc.doctype #job or replica
      type = docType._get_name().encode('ascii')
    except Exception,ex:
      gLogger.error("XML reading error",filename)
      return S_ERROR(ex)
        
    return type,doc,filename
  
  #############################################################################  
  def readXMLfromString(self, xmlString):
    
    try:
      doc = self.reader_.fromString(xmlString)
    
      docType = doc.doctype #job or replica
      type = docType._get_name().encode('ascii')
    
      if type == 'Replicas':
        replica = self.replicaReader_.readReplica(doc, "IN Memory")
        result = self.processReplicas(replica)
        return result        
      else: 
        if type == 'Job':
          job = self.jobReader_.readJob(doc, "IN Memory")
          result = self.processJob(job)
          return result
        else:
          gLogger.error("unknown XML file!!!")
    except Exception,ex:
      gLogger.error("XML reading error",ex)
      return S_ERROR(ex)
       
  
  #############################################################################  
  def processJob(self, job, errorReport=False):
    """
    
    """    
    gLogger.info("Start Processing")
    
    deleteFileName = job.getFileName()
    
    ##checking
    inputFiles = job.getJobInputFiles()
    for file in inputFiles:
      name = file.getFileName()
      result = dataManager_.checkfile(name) 
      if result['OK']:
        fileID = long(result['Value'][0][0])
        file.setFileID(fileID)
      else:
        self.errorMgmt_.reportError (10, "The file " + name + " is missing.", deleteFileName, errorReport)
        return S_ERROR('The file '+name+'not exist in the BKK database!!')

    outputFiles = job.getJobOutputFiles()
    for file in outputFiles:
      name = file.getFileName()
      result = dataManager_.checkfile(name)
      if result['OK']:
        self.errorMgmt_.reportError (11, "The file " + str(name) + " already exists.", deleteFileName, errorReport)
        return S_ERROR("The file " + str(name) + " already exists.")
      
      typeName = file.getFileType()
      typeVersion = file.getFileVersion()
      result = dataManager_.checkFileTypeAndVersion(typeName, typeVersion)
      if not result['OK']:
        self.errorMgmt_.reportError (12, "The type " + str(typeName) + ", version " + str(typeVersion) +"is missing.\n", deleteFileName, errorReport)
        return S_ERROR("The type " + str(typeName) + ", version " + str(typeVersion) +"is missing.\n")
      else:
        typeID = long(result['Value'][0][0])
        file.setTypeID(typeID)
      
      params = file.getFileParams()
      for param in params:
        paramName = param.getParamName()
        if paramName == "EventStat":
          eventNb = long(param.getParamValue())
          if eventNb <= 0:
            self.errorMgmt_.reportError (13,"The event number not greater 0!", deleteFileName)
            return S_ERROR("The event number not greater 0!")
        if paramName == "EventType":
          value = long(param.getParamValue())
          result = dataManager_.checkEventType(value)
          if not result['OK']:
            self.errorMgmt_.reportError (13, "The event type " + str(value) + " is missing.\n", deleteFileName, errorReport)
            return S_ERROR("The event type " + str(value) + " is missing.\n")
      ################
      
    config = job.getJobConfiguration()
    params = job.getJobParams()
            
    result = dataManager_.insertJob(job)    
    if not result['OK']:
      self.errorMgmt_.reportError (13, "Unable to create Job : " + str(config.getConfigName()) + ", " + str(config.getConfigVersion()) + ", " + str(config.getDate()) + ".\n", deleteFileName, errorReport)
      return S_ERROR("Unable to create Job : " + str(config.getConfigName()) + ", " + str(config.getConfigVersion()) + ", " + str(config.getDate()) + ".\n"+'Error: '+str(result['Message']))
    else:
      jobID = long(result['Value'])
      job.setJobId(jobID) 
    inputFiles = job.getJobInputFiles()
    for file in inputFiles:
      result = dataManager_.insertInputFile(job.getJobId(), file.getFileID())
      if not result['OK']:
        dataManager_.deleteJob(job.getJobId())
        self.errorMgmt_.reportError (16, "Unable to add " + str(file.getFileName()), deleteFileName, errorReport)
        return S_ERROR("Unable to add " + str(file.getFileName()))
      
  
    outputFiles = job.getJobOutputFiles()
    for file in outputFiles:
      result = dataManager_.insertOutputFile(job, file)
      if not result['OK']:
        dataManager_.deleteJob(job.getJobId())
        dataManager_.deleteInputFiles(job.getJobId())
        self.errorMgmt_.reportError (17, "Unable to create file " + str(file.getFileName()) + "!", deleteFileName, errorReport)
        return S_ERROR("Unable to create file " + str(file.getFileName()) + "!"+"ERROR: "+result["Message"])
      else:
        id = long(result['Value'])
        file.setFileID(id)
         
      qualities = file.getQualities()
      for quality in qualities:
        group = quality.getGroup()
        flag = quality.getFlag()
        result = dataManager_.insertQuality(file.getFileID(), group, flag)           
        if not result['OK']:
          self.errorMgmt_.reportError(19, "Unable to create Quality " + str(group) + "/" + str(flag) + "\" for file " + str(file.getFileName()) + ".\n", deleteFileName, errorReport)
          return S_ERROR("Unable to create Quality " + str(group) + "/" + str(flag) + "\" for file " + str(file.getFileName()) + ".\n")
        else:
          qualityID = long(result['Value'])
          quality.setQualityID(qualityID)
          
        params = quality.getParams()
        for param in params:
           name = param.getName()
           value = param.getValue()
           result = dataManager_.insertQualityParam(file.getFileID(), quality.getQualityID(), name, value)
           if not result['OK']:
             self.errorMgmt_.reportError (20, "Unable to create Parameter \"" + str(name) + " = " + str(value) + "\" for quality " + group + "/" + flag + "\".\n", deleteFileName, errorReport)
             return S_ERROR("Unable to create Parameter \"" + str(name) + " = " + str(value) + "\" for quality " + group + "/" + flag + "\".\n")
                   
      replicas = file.getReplicas()
      for replica in replicas:
        params = replica.getaprams()
        for param in params: # just one param exist in params list, because JobReader only one param add to Replica
          name = param.getName()
          location = param.getLocation()
        result = dataManager_.updateReplicaRow(file.getFileID(), 'No')  #, name, location)           
        if not result['OK']:
          self.errorMgmt_.reportError(21, "Unable to create Replica " + str(name) +".\n", deleteFileName, errorReport)
          return S_ERROR("Unable to create Replica " + str(name) +".\n")
    
    gLogger.info("End Processing!" )
    
    return S_OK()
  
  def processReplicas(self, replica, errorReport=False):
    """
    
    """
    file = replica.getFileName()
    gLogger.info("Processing replicas: " + str(file))
    fileID = -1
    
    params = replica.getaprams()
    delete = True
        
    replicaFileName = ""
    for param in params:
      name = param.getName()
      replicaFileName = param.getFile()
      location = param.getLocation()
      delete = param.getAction() == "Delete"
            
      result = dataManager_.checkfile(replicaFileName)
    
      if not result['OK']:
        message = "No replica can be ";
        if (delete):
           message += "removed" 
        else:
           message += "added"
        message += " to file " + str(file) + " for " + str(location) + ".\n"
        self.errorMgmt_.reportError(23, message, file, errorReport)
        return S_ERROR(message)
      else:
        fileID = long(result['Value'][0][0])
        gLogger.info(fileID)
      
      if (delete):    
        result = self.lcgFileCatalogClient_.getReplicas(replicaFileName)
        list = result['Value']
        replicaList = list['Successful']
        if len(replicaList) == 0:
          result = dataManager_.updateReplicaRow(fileID,"no")
          if not result['OK']:
            gLogger.warn("Unable to set the Got_Replica flag for " + str(replicaFileName))
            self.errorMgmt_.reportError(26, "Unable to set the Got_Replica flag for " + str(replicaFileName), file, errorReport)
            return S_ERROR("Unable to set the Got_Replica flag for " + str(replicaFileName))
         
      else:
        result = dataManager_.updateReplicaRow(fileID, "yes")
        if not result['OK']:
          self.errorMgmt_.reportError(26, "Unable to set the Got_Replica flag for " + str(replicaFileName), file, errorReport)
          return S_ERROR("Unable to set the Got_Replica flag for " + str(replicaFileName))
   
  
    gLogger.info("End Processing replicas!")
    
    return S_OK()
  
  #############################################################################  
  def getJobs(self):  
    return self.jobs_
  
  #############################################################################
  def getReplicas(self):
    return self.replicas_
  
  #############################################################################
  def _listTodoDirectory(self):
    return self.fileClient_.list(self.baseDir_+"ToDo")
  #############################################################################
  def fileaction(self, fn):
    """
    
    """
    try:
      fptr = open(fn,'r')
      data = fptr.read()
      fptr.close()
      if ( data.find('<Error id="10">')>0 ):
      # File does not (yet) exist
        return 'recover', 10
      elif ( data.find('<Error id="23">')>0 ):
      # No replica can be added to file
        return 'recover', 23
      elif ( data.find('<Error id="26">')>0 ):
        # Error while connectiong to genCatalog server
        return 'recover', 26
      elif ( data.find('<Error id="12">')>0 ):
      # The type LOG, version 0is missing. Io exception: Connection reset by peer: socket write error
        return 'recover', 12
      elif ( data.find('<Error id="7">')>0 ):
      # Attribute "SE" must be declared for element type "Replica".
        return 'save', 7
      elif ( data.find('<Error id="2">')>0 ):
      # Open quote is expected for attribute "Name|".
        return 'save', 2
      elif ( data.find('<Error id="25">')>0 ):
      # File already had a replica at
        return 'save', 25
      elif ( data.find('<Error id="11">')>0 ):
      # File already exists
        return 'save', 11
      else:
        #print 'Skip:', todo_dir+f
        return 'none', None
    except Exception, ex:
      gLogger.warn(ex)
      gLogger.info(fn+"file removed the ErrorTmp directory")
      name = os.path.split(fn[:-6])[1]
      self.fileClient_.rename(fn[:-6], self.errorsTmp_+name)
      self.fileClient_.rename(fn, self.errorsTmp_+name)
      #self.fileClient_.rm(fn)
      return 'none', None

  #############################################################################
  def _listErrorDirectory(self):
    """
    
    """
    fileList = self.fileClient_.list(self.baseDir_+"Errors")
    
    files = []
    for file in fileList: 
      if file[-6:] != '.error':
        fn = file + ".error"
        action, err = self.fileaction(fn)
        if ( action == 'recover' ):
          files += [file]
        else:
          name = os.path.split(file)[1]
          self.fileClient_.rename(file, self.errorsTmp_ + name)
          gLogger.info(file+" file moved the ErrorTmp directory!!!")
          gLogger.info(fn+" file moved the ErrorTmp directory!!!")  
          self.fileClient_.rename(fn, self.errorsTmp_ + name+".error")       
        
        """
        very important to remove also the error messages, because we have to now
        what is the problem!!!!
        """
        #self.fileClient_.rm(fn)
    return files
  
  #############################################################################   
  def initialize(self):
    """
    
    """
    jobList = self._listTodoDirectory()
    
    for fileName in jobList:
      type,doc,fullpath = self.readFile(fileName) 
      if type == 'Replicas':
        replica = self.replicaReader_.readReplica(doc, fullpath)
        self.replicas_ += [replica]
      else: 
        if type == 'Job':
          try:
            job = self.jobReader_.readJob(doc, fullpath)
            self.jobs_ += [job]
          except Exception,ex:
            gLogger.error("XML reading error2",ex)         
        else:
          name = os.path.split(fileName)[1]
          self.fileClient_.rename(fileName, self.errorsTmp_ + name)
          self.fileClient_.rm(fileName)
          gLogger.error("unknown XML file!!!")
          
         
    errorList = self._listErrorDirectory()   
    for fileName in errorList:
      type,doc,fullpath = self.readFile(fileName) 
      if type == 'Replicas':
        replica = self.replicaReader_.readReplica(doc, fullpath)
        self.replicas_ += [replica]
      else: 
        if type == 'Job':
          job = self.jobReader_.readJob(doc, fullpath)
          self.jobs_ += [job]
        else:
          name = os.path.split(fileName)[1]
          self.fileClient_.rename(fileName, self.errorsTmp_ + name)
          self.fileClient_.rm(fileName)
          print fileName,self.errorsTmp_ + name
          gLogger.error("unknown XML file!!!")
  
    return S_ERROR(ex)
  
  #############################################################################   
  def destroy(self):
    del self.jobs_[:]      
    del self.replicas_[:]
  
  #############################################################################   
    
        
    
