########################################################################
# $Id: IBookkeepingDatabaseClient.py,v 1.2 2008/06/30 14:58:10 zmathe Exp $
########################################################################

"""

"""

from DIRAC.BookkeepingSystem.DB.IBookkeepingDB             import IBookkeepingDB
from DIRAC                                                 import gLogger, S_OK, S_ERROR

__RCSID__ = "$Id: IBookkeepingDatabaseClient.py,v 1.2 2008/06/30 14:58:10 zmathe Exp $"

class IBookkeepingDatabaseClient(object):
    
  #############################################################################
  def __init__(self, DatabaseManager = IBookkeepingDB()):
    self.databaseManager_ = DatabaseManager
    
  #############################################################################
  def getManager(self):
    return self.databaseManager_
  
  #############################################################################
  def file(self, fileName):
    return self.getManager().file(fileName)
  
  #############################################################################
  def fileTypeAndFileTypeVersion(self, type, version):
    return self.getManager().fileTypeAndFileTypeVersion(type, version)
  
  #############################################################################
  def eventType(self, eventTypeId):
    return self.getManager().eventType(eventTypeId)
  
  #############################################################################
  def insertJob(self, jobName, jobConfVersion, date):
    return self.getManager().insertJob(jobName, jobConfVersion, date)
  
  #############################################################################
  def insertJob(self, config, jobParams):
    return self.getManager().insertJob(config, jobParams)
  
  #############################################################################
  def insertJobParameter(self, jobID, name, value, type):
    return self.getManager().insertJobParameter(jobID, name, value, type)
  
  #############################################################################
  def insertInputFile(self, jobID, inputfile):
    return self.getManager().insertInputFile(jobID, inputfile)
  
  #############################################################################
  def insertOutputFile(self, jobID, name, value):
    return self.getManager().insertOutputFile(jobID, name, value)
  
  #############################################################################
  def insertOutputFile(self, job, file):
    return self.getManager().insertOutputFile(job, file)
  
  #############################################################################
  def insertFileParam(self, fileID, name, value):
    return self.getManager().insertFileParam(fileID, name, value)
  
  #############################################################################
  def insertReplica(self, fileID, name, location):
    return self.getManager().insertReplica(fileID, name, location)
  
  #############################################################################
  def deleteJob(self, job):
    return self.getManager().deleteJob(job)
  
  #############################################################################
  def deleteFile(self, file):
    return self.getManager().deleteFile(file)
  
  #############################################################################
  def insertQuality(self, fileID, group, flag ):
    return self.getManager().insertQuality(fileID, group, flag)
  
  #############################################################################
  def insertQualityParam(self, fileID, qualityID, name, value):
    return self.getManager().insertQualityParam(fileID, name, value)
  
  #############################################################################
  def modifyReplica(self, fileID , name, value):
    return self.getManager().modifyReplica(fileID , name, value)
  
  #Oracle 
  #############################################################################
  def checkfile(self, fileName): 
    return self.getManager().checkfile(fileName)
  
  #############################################################################
  def checkFileTypeAndVersion(self, type, version): 
    return self.getManager().checkFileTypeAndVersion(type, version)
  
  def checkEventType(self, eventTypeId):  
    return self.getManager().checkEventType(eventTypeId)
  
  #############################################################################
  def insertJob(self, config, jobParams):
    return self.getManager().insertJob(config, jobParams)
    
  #############################################################################
  def insertInputFile(self, jobID, FileId):
    return self.getManager().insertInputFile(jobID, FileId)
    
  #############################################################################
  def insertOutputFile(self, job, file):
    return self.getManager().insertOutputFile(job, file)
      
  #############################################################################
  def updateReplicaRow(self, fileID, replica): #, name, location):
    return self.getManager().updateReplicaRow(fileID, replica)
    
  #############################################################################
  def deleteJob(self, job):
    return self.getManager().deleteJob(job)
  
  #############################################################################
  def deleteFile(self, file):
    return self.getManager().deleteFile(file)
  