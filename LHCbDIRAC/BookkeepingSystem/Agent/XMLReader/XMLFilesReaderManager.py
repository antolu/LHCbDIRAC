########################################################################
# $Id: XMLFilesReaderManager.py,v 1.4 2008/07/04 14:05:07 zmathe Exp $
########################################################################

"""

"""

from xml.dom.ext.reader                                             import Sax
from DIRAC.BookkeepingSystem.Agent.FileSystem.FileSystemClient      import FileSystemClient
from DIRAC.BookkeepingSystem.Agent.XMLReader.JobReader              import JobReader
from DIRAC.BookkeepingSystem.Agent.XMLReader.ReplicaReader          import ReplicaReader
from DIRAC.ConfigurationSystem.Client.Config                        import gConfig
from DIRAC                                                          import gLogger, S_OK, S_ERROR
from DIRAC.BookkeepingSystem.DB.BookkeepingDatabaseClient           import BookkeepingDatabaseClient
import os,sys

__RCSID__ = "$Id: XMLFilesReaderManager.py,v 1.4 2008/07/04 14:05:07 zmathe Exp $"

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
    self.dataManager_ = BookkeepingDatabaseClient()
    
  
  #############################################################################
  def readFile(self, filename):
    """
    
    """
    stream = open(filename)
    doc = self.reader_.fromStream(stream)
    stream.close()
    

    docType = doc.doctype #job or replica
    type = docType._get_name().encode('ascii')
        
    return type,doc,filename
  
  #############################################################################  
  def readXMLfromString(self, xmlString):
    
    doc = self.reader_.fromString(xmlString)
    
    docType = doc.doctype #job or replica
    type = docType._get_name().encode('ascii')
    
    if type == 'Replicas':
        replica = self.replicaReader_.readReplica(doc, "IN Memory")
        return S_OK(replica)
    else: 
      if type == 'Job':
        job = self.jobReader_.readJob(doc, "IN Memory")
        result = self.__processJob(job)
        return result
      else:
        gLogger.error("unknown XML file!!!")
  
  #############################################################################  
  def __processJob(self, job):
    """
    
    """    
    self.log.info("Start Processing")

    ##checking
    inputFiles = job.getJobInputFiles()
    for file in inputFiles:
      name = file.getFileName()
      result = self.dataManager_.checkfile(name)
      
      if result['OK']:
        fileID = int(result['Value'][0][0])
        file.setFileID(fileID)

    outputFiles = job.getJobOutputFiles()
    for file in outputFiles:
      name = file.getFileName()
      result = self.dataManager_.checkfile(name)
      if result['OK']:
        return S_ERROR("The file " + str(name) + " already exists.")
      
      typeName = file.getFileType()
      typeVersion = file.getFileVersion()
      result = self.dataManager_.checkFileTypeAndVersion(typeName, typeVersion)
      if not result['OK']:
        return S_ERROR("The type " + str(typeName) + ", version " + str(typeVersion) +"is missing.\n")
      else:
        typeID = int(result['Value'][0][0])
        file.setTypeID(typeID)
      
      params = file.getFileParams()
      for param in params:
        paramName = param.getParamName()
        if paramName == "EventStat":
          eventNb = int(param.getParamValue())
          if eventNb <= 0:
            return S_ERROR("The event number not greater 0!")
        if paramName == "EventType":
          value = int(param.getParamValue())
          result = self.dataManager_.checkEventType(value)
          if not result['OK']:
            return S_ERROR("The event type " + str(value) + " is missing.\n")
      ################
      
    config = job.getJobConfiguration()
    params = job.getJobParams()
            
    result = self.dataManager_.insertJob(job)    
    if not result['OK']:
      return S_ERROR("Unable to create Job : " + str(config.getConfigName()) + ", " + str(config.getConfigVersion()) + ", " + str(config.getDate()) + ".\n")
    else:
      jobID = int(result['Value'])
      job.setJobId(jobID) 
    inputFiles = job.getJobInputFiles()
    for file in inputFiles:
      result = self.dataManager_.insertInputFile(job.getJobId(), file.getFileID())
      if not result['OK']:
        self.dataManager_.deleteJob(job.getJobId())
        return S_ERROR("Unable to add " + str(file.getFileName()))
      
  
    outputFiles = job.getJobOutputFiles()
    for file in outputFiles:
      result = self.dataManager_.insertOutputFile(job, file)
      if not result['OK']:
        self.dataManager_.deleteJob(job.getJobId())
        return S_ERROR("Unable to create file " + str(file.getFileName()) + "!")
      else:
        id = int(result['Value'])
        file.setFileID(id)
         
      qualities = file.getQualities()
      for quality in qualities:
        group = quality.getGroup()
        flag = quality.getFlag()
        result = self.dataManager_.insertQuality(file.getFileID(), group, flag)           
        if not result['OK']:
          return S_ERROR("Unable to create Quality " + str(group) + "/" + str(flag) + "\" for file " + str(file.getFileName()) + ".\n")
        else:
          qualityID = int(result['Value'])
          quality.setQualityID(qualityID)
          
        params = quality.getParams()
        for param in params:
           name = param.getName()
           value = param.getValue()
           result = self.dataManager_.insertQualityParam(file.getFileID(), quality.getQualityID(), name, value)
           if not result['OK']:
             return S_ERROR("Unable to create Parameter \"" + str(name) + " = " + str(value) + "\" for quality " + group + "/" + flag + "\".\n")
                   
      replicas = file.getReplicas()
      for replica in replicas:
        params = replica.getaprams()
        for param in params: # just one param exist in params list, because JobReader only one param add to Replica
          name = param.getName()
          location = param.getLocation()
        result = self.dataManager_.updateReplicaRow(file.getFileID(),'Yes')  #, name, location)           
        if not result['OK']:
          return S_ERROR("Unable to create Replica " + str(name) +".\n")
    
    self.log.info("End Processing!" )
    
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
          job = self.jobReader_.readJob(doc, fullpath)
          self.jobs_ += [job]
        else:
          name = os.path.split(fileName)[1]
          self.fileClient_.rename(fileName, self.errorsTmp_ + name)
          self.fileClient_.rm(fileName)
          print fileName,self.errorsTmp_ + name
          print 
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
  
  #############################################################################   
  def destroy(self):
    del self.jobs_[:]      
    del self.replicas_[:]
  
  #############################################################################   
    
        
    
