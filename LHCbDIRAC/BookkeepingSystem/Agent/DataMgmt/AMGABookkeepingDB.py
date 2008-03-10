########################################################################
# $Id: AMGABookkeepingDB.py,v 1.10 2008/03/10 12:29:54 zmathe Exp $
########################################################################

"""

"""

from DIRAC.BookkeepingSystem.Agent.DataMgmt.IBookkeepingDB           import IBookkeepingDB
from DIRAC.BookkeepingSystem.Agent.DataMgmt.DB                       import DB
from DIRAC                                                           import gLogger, S_OK, S_ERROR
from DIRAC.ConfigurationSystem.Client.Config                         import gConfig

__RCSID__ = "$Id: AMGABookkeepingDB.py,v 1.10 2008/03/10 12:29:54 zmathe Exp $"

class AMGABookkeepingDB(IBookkeepingDB):
  
  #############################################################################
  def __init__(self):
    super(AMGABookkeepingDB, self).__init__()
    self.hostName_ = gConfig.getValue("hostName", "volhcb07.cern.ch")
    self.port_ = gConfig.getValue("port", 8822)
    self.password_ = gConfig.getValue("password", "root")
    
    self.db_ = DB(self.hostName_, self.port_, self.password_)
 
  #############################################################################
  def getdbManager(self):
    return self.db_  
  
  #############################################################################
  def file(self, fileName):
    """
    
    """
    result = []
    try:
      self.db_.selectAttr(["/files:FILE", "/files:JOB_ID", "/files:TYPE_ID"], "/files:LOGNAME=\"" + str(fileName) + "\"")
      while not self.db_.eot():
        result = self.db_.getSelectAttrEntry()
    except Exception, ex:
      gLogger.error(ex)
      return S_ERROR(ex) 
    if ( result == [] ):
      gLogger.error("File not found! with" + str(fileName))
      return S_ERROR("File not found! with" + str(fileName))
    else:
      return S_OK(result)      
      
  #############################################################################
  def fileTypeAndFileTypeVersion(self, type, version):
    """
    
    """
    result = []
    try:
      self.db_.selectAttr(["/typeParams:FILE"], "/typeParams:Name=\"" + str(type) + "\" and /typeParams:Version=\"" + str(version) + "\"")
      while not self.db_.eot():
        result = self.db_.getSelectAttrEntry()
    except Exception, ex:
      gLogger.error("fileTypeAndFileTypeVersion: " + str(ex))
      return S_ERROR(ex)
    
    if ( result == [] ):
      gLogger.error("Type not found! with name " + str(type) + " and version " + str(version))
      return S_ERROR("Type not found! with name " + str(type) + " and version " + str(version))
    else:
      return S_OK(int(result[0]))      
  
  #############################################################################
  def eventType(self, eventTypeId):
    """
    
    """
    result = []
    try:
      self.db_.selectAttr(["/evtTypes:DESCRIPTION", "/evtTypes:PRIMARY", "/evtTypes:DECAY"], "/evtTypes:EVTTYPE_ID=" + str(eventTypeId))
      while not self.db_.eot():
        result = self.db_.getSelectAttrEntry()
    except Exception, ex:
      gLogger.error("eventType: " + str(ex))
      return S_ERROR(ex)
    
    if ( result == [] ):
      gLogger.error("Event type " + str(eventTypeId) + " not found!")
      return S_ERROR("Event type " + str(eventTypeId) + " not found!")
    else:
      return S_OK(result)      
  
  
  #############################################################################
  def insertJob(self, jobName, jobConfVersion, date):
    """
    
    """
    try:
      self.db_.getattr("/NextBookkeepingIDs/ids", ["JOB_ID"])
      value = []
      id = -1
      while not self.db_.eot():
        file, value = self.db_.getEntry()
      
      if (value != []):
        id = int(value[0]) + 1
      else:
        gLogger.error("Cannot find the next bookkeeping index!")
        return S_ERROR("Cannot find the next bookkeeping index!")
      
      self.db_.addEntry("/jobs/" + str(id), ['CONFIGNAME', 'CONFIGVERSION', 'JOB_ID', 'JOBDATE'], [jobName, jobConfVersion, str(id), date])
      self.db_.setAttr("/NextBookkeepingIDs/ids", ["JOB_ID"], [str(id)])
    except Exception, ex:
      gLogger.error("Insert Job: " + str(ex))
      return S_ERROR(ex)
    
    return S_OK(id)
  
  #############################################################################
  def insertJobParameter(self, jobID, name, value, type):
    """
    
    """
    try:
      self.db_.addEntry("/jobParams/" + str(jobID), ["TYPE", "JOB_ID"], [type, str(jobID)])   
    except Exception, ex:
      try:
        self.db_.setAttr("/jobParams/" + str(jobID), [name.replace(' ','_')], [value])
      except Exception, ex:
        try:
          self.db_.addAttr("/jobParams/" + str(jobID), name.replace(' ','_'), "varchar2(255)")
          self.db_.setAttr("/jobParams/" + str(jobID), [name.replace(' ','_')], [value])
        except Exception. ex:   
          gLogger.error("Insert job parameters: " + str(ex))
          return S_ERROR(ex)
    return S_OK()
  
  #############################################################################
  def insertInputFile(self, jobID, inputfileID): #addInputFile
    """
    
    """
    try:
      self.db_.getattr("/NextBookkeepingIDs/ids", ["INPUTFILE_ID"])
      value = []
      id = -1
      while not self.db_.eot():
        file, value = self.db_.getEntry()
      
      if (value != []):
        id = int(value[0]) + 1
      else:
        gLogger.error("Cannot find the next bookkeeping index!")
        return S_ERROR("Cannot find the next bookkeeping index!")
      
      self.db_.addEntry("/inputFiles/" + str(id), ["JOB_ID", "FILE_ID"], [str(jobID), str(inputfileID)])
      self.db_.setAttr("/NextBookkeepingIDs/ids",["INPUTFILE_ID"], [str(id)]);
    
    except Exception, ex:
      gLogger.error("insert input file: " + str(ex))
      return S_ERROR(ex) 
    
    return S_OK()
  
  #############################################################################
  def insertOutputFile(self, jobID, name, typeID):
    """
    
    """
    try:
      self.db_.getattr("/NextBookkeepingIDs/ids", ["FILE_ID"])
      value = []
      id = -1
      while not self.db_.eot():
        file, value = self.db_.getEntry()
      
        if (value != []):
          id = int(value[0]) + 1
        else:
          gLogger.error("Cannot find the next bookkeeping index!")
          return S_ERROR("Cannot find the next bookkeeping index!")
           
      self.db_.addEntry("/files/"+str(id), ["TYPE_ID", "LOGNAME", "JOB_ID", "FILE_ID"], [str(typeID), name, str(jobID), str(id)]) 
      self.db_.setAttr("/NextBookkeepingIDs/ids", ["FILE_ID"], [str(id)]);       
      
    except Exception, ex:
      gLogger.error("Insert outputFile: " + str(ex))
      return S_ERROR(ex)
    return S_OK(id)
  
  #############################################################################
  def insertFileParam(self, id, name, value):
    """
    
    """
    try:
      self.db_.addEntry("/fileParams/" + str(id), ["FILE_ID"], [str(id)])
    except Exception, ex: #catch(CommandException ce){;}//the entry "/jobParams/"+id is already in the table
      try: 
        #self.db_.getAttr("/fileParams/" + str(id), [name.replace(' ','_')]) #?
        self.db_.setAttr("/fileParams/" + str(id), [name.replace(' ','_')], [value])
      except Exception, ex:
        try:
          self.db_.addAttr("/fileParams/" + str(id),name.replace(' ','_'), "varchar2(255)")
          self.db_.setAttr("/fileParams/" + str(id), [name.replace(' ','_')], [value]);
        except Exception, ex:
          gLogger.error("Insert file Param: " + str(ex))
          return S_ERROR(ex)
    return S_OK()
  
  #############################################################################
  def insertReplica(self, fileID, name, location):
    """
    
    """
    try:     
      self.db_.getattr("/NextBookkeepingIDs/ids", ["REPLICA_ID"])
      value = []
      id = -1
      while not self.db_.eot():
        file, value = self.db_.getEntry()
      
        if (value != []):
          id = int(value[0]) + 1
        else:
          gLogger.error("Cannot find the next bookkeeping index!")
          return S_ERROR("Cannot find the next bookkeeping index!")
           
   
      self.db_.addEntry("/replicas/All/" + str(id), ["REPLICA", "LOCATION", "FILE_ID"], [name, location, str(fileID)]) 
      self.db_.setAttr("/NextBookkeepingIDs/ids", ["REPLICA_ID"], [str(id)])
      return S_OK()
  
    except Exception, ex:
      gLogger.error("Insert replica: " + str(ex))
      return S_ERROR(ex)
  
  #############################################################################
  def deleteJob(self, jobID):
    """
    
    """
    try:
      self.db_.removeEntry("/jobs/"+str(jobID));
    except Exception, ex:
      gLogger.error("Delete Job: " + str(ex))
      return S_ERROR()
    return S_OK()
  
  #############################################################################
  def deleteFile(self):
    """
    
    """
    try:
      self.db_.removeEntry("/files/"+file.fileID());
    except Exception, ex:
      gLogger.error("delete file: " + str(ex))
      return S_ERROR()
    return S_OK()
  
  #############################################################################
  def insertQuality(self, fileID, group, flag ):
    """
    
    """
    try:     
      self.db_.getattr("/NextBookkeepingIDs/ids", ["QUALITY_ID"])
      value = []
      id = -1
      while not self.db_.eot():
        file, value = self.db_.getEntry()
      
        if (value != []):
          id = int(value[0]) + 1
        else:
          gLogger.error("Cannot find the next bookkeeping index!")
          return S_ERROR("Cannot find the next bookkeeping index!")
      
      self.db_.addEntry("/qualityParams/" + str(id), ["Flag", "Group", "FILE_ID", "QUALITY_ID"], [flag, group, str(fileID), str(id)]) 
      self.db_.setAttr("/NextBookkeepingIDs/ids", ["QUALITY_ID"], [str(id)])
    
    except Exception, ex:
      gLogger.error("Insert quality" + str(ex))
      return S_ERROR()
    
    return S_OK(id)
  
  #############################################################################
  def insertQualityParam(self, fileID, qualityID, name, value):
    """
    
    """
    try:
      self.db_.addEntry("/qualityParams/" + str(qualityID), ["FILE_ID", "QUALITY_ID"], [str(fileID), str(qualityID)])
    except Exception, ex:
      try:
        #attvalue = self.db_.getAttr("/qualityParams/" + str(qualityID), [name.replace(' ','_')])
        self.db_.setAttr("/qualityParams/" + str(qualityID), [name.replace(' ','_'), value])
      except Exception, ex:
        try:
          self.db_.addAttr("/qualityParams/" + str(qualityID), name.replace(' ','_'), "varchar2(255)")
          self.db_.setAttr("/qualityParams/" + str(qualityID), [name.replace(' ','_'), value])
        
        except Exception, ex:
          gLogger.error("Insert quality param" + str(ex))
          return S_ERROR(ex)
  
  def modifyReplica(fileID , name, value):
    try:
      self.db_.setAttr("/fileParams/"+str(fileID), [name.replace(' ','_'), value])
    except Exception, ex:
      gLogger.error("Attribute not found!!")
      return S_ERROR(ex)
    return S_OK()
  #############################################################################
