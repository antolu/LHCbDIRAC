########################################################################
# $Id: XMLFilesReaderManager.py,v 1.3 2008/07/01 10:54:27 zmathe Exp $
########################################################################

"""

"""

from xml.dom.ext.reader                                             import Sax
from DIRAC.BookkeepingSystem.Agent.FileSystem.FileSystemClient      import FileSystemClient
from DIRAC.BookkeepingSystem.Agent.XMLReader.JobReader              import JobReader
from DIRAC.BookkeepingSystem.Agent.XMLReader.ReplicaReader          import ReplicaReader
from DIRAC.ConfigurationSystem.Client.Config                        import gConfig
from DIRAC                                                          import gLogger, S_OK, S_ERROR
import os,sys

__RCSID__ = "$Id: XMLFilesReaderManager.py,v 1.3 2008/07/01 10:54:27 zmathe Exp $"

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
    
        
    
