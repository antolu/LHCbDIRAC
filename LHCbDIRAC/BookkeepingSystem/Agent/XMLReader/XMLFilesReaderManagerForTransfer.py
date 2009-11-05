########################################################################
# $Id$
########################################################################

"""

"""

from xml.dom.ext.reader                                                        import Sax
from DIRAC.BookkeepingSystem.Agent.FileSystem.FileSystemClient                 import FileSystemClient
from DIRAC.BookkeepingSystem.Agent.XMLReader.JobReader                         import JobReader
from DIRAC.BookkeepingSystem.Agent.XMLReader.ReplicaReader                     import ReplicaReader
from DIRAC.ConfigurationSystem.Client.Config                                   import gConfig
from DIRAC                                                                     import gLogger, S_OK, S_ERROR
from DIRAC.BookkeepingSystem.Client.BookkeepingClient                          import BookkeepingClient
from DIRAC.BookkeepingSystem.Agent.XMLReader.Job.SimulationConditions          import SimulationConditions
import os,sys

__RCSID__ = "$Id$"

class XMLFilesReaderManagerForTransfer:
  
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
    self.bkkClient_ = BookkeepingClient()
    
  
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
          
  #############################################################################   
  def destroy(self):
    del self.jobs_[:]      
    del self.replicas_[:]
  
  #############################################################################   
  
  def directTransfer(self):
    
    gLogger.info("Bookkeeping direct file transfer!!!")
    jobList = self._listTodoDirectory()
    
    for fileName in jobList:
      type,doc,fullpath = self.readFile(fileName) 
      if type == 'Replicas':
        replica = self.replicaReader_.readReplica(doc, fullpath)
        result = self.__translateReplicaAttributes(replica)
        if result['OK']:
          name = replica.getFileName().split("/")[5]
          gLogger.info("Send"+str(name)+" to volhcb07!!")
          self.bkkClient_.filetransfer(name, replica.writeToXML())
          #self.bkkClient_.sendBookkeeping('In Memory', replica.writeToXML())
      else: 
        if type == 'Job':
          job = self.jobReader_.readJob(doc, fullpath)
          result = self.__translateJobAttributes(job)
          if result['OK']:
            name = job.getFileName().split("/")[5]
            gLogger.info("Send "+str(name)+" to volhcb07!!")
            self.bkkClient_.filetransfer(name, job.writeToXML())
            #self.bkkClient_.sendBookkeeping('In Memory', job.writeToXML())
    
          
  #############################################################################
  def __translateJobAttributes(self, job):

    attrlist = { 'DIRAC_JOBID':'DiracJobId', \
                 'DIRAC_VERSION':'DiracVersion', \
                 'EXECTIME':'ExecTime', \
                 'XMLDDDBVERSION':'GeometryVersion', \
                 'EDG_WL_JOBID':'GridJobId', \
                 'JOBDATE':'JobStart', \
                 'LOCALJOBID':'LocalJobId', \
                 'LOCATION':'Location', \
                 'NAME':'Name', \
                 'NUMBEROFEVENTS':'NumberOfEvents', \
                 'PRODUCTION':'Production', \
                 'PROGRAMNAME':'ProgramName', \
                 'PROGRAMVERSION':'ProgramVersion', \
                 'STATISTICSREQUESTED':'StatisticsRequested', \
                 'CPU':'WNCPUPower', \
                 'CPUTIME':'WNCPUTime', \
                 'CACHE':'WNCache', \
                 'MEMORY':'WNMemory', \
                 'MODEL':'WNModel', \
                 'HOST':'WorkerNode' }

 

    fileattr = {'EVENTSTAT':'EventStat', 
                'EVENTTYPE':'EventTypeId' , 
                'LOGNAME':'FileName', 
                'GOT_REPLICA':'GotReplica',
                'GUID':'Guid',
                'MD5SUM':'MD5Sum', 
                'SIZE':'FileSize' }

    
    configs = job.getJobConfiguration()
    if configs.getConfigName() == 'MC' and configs.getConfigVersion() == "DC06" or configs.getConfigName() == 'DC06':
      configs.setConfigName('MC')
      configs.setConfigVersion('DC06')
            
      removeParams = []
      for param in  job.getJobParams():
        name = param.getName()
        if attrlist.has_key(name.upper()):
          param.setName(attrlist[name.upper()])
        else:
          removeParams += [param]
       
      for param in removeParams:    
         job.removeJobParam(param)
      
      
      for file in job.getJobOutputFiles():
        removeFileParams = []
        params = file.getFileParams()
        for param in params:
          name = param.getParamName()
          if fileattr.has_key(name.upper()):
            param.setParamName(fileattr[name.upper()])
          else:
            removeFileParams += [param]
        for param in removeFileParams:
          file.removeFileParam(param)
      
      sim = SimulationConditions()
      sim.addParam("BeamCond", "Collisions")
      sim.addParam("BeamEnergy" ,"7 TeV")
      sim.addParam("Generator", "Pythia 6.325.2")
      sim.addParam("MagneticField", "-100%")
      sim.addParam("DetectorCond", "Normal")
      sim.addParam("Luminosity", "Fixed 2 10**32")
      job.addSimulationCond(sim)  
    else:
      return S_ERROR()
    return S_OK()
  
  #############################################################################
  def __translateReplicaAttributes(self, replica):
    return S_OK()
    
