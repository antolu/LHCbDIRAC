########################################################################
# $Id: IBookkeepingDatabaseClient.py,v 1.2 2008/03/03 09:56:25 zmathe Exp $
########################################################################

"""

"""

from DIRAC.BookkeepingSystem.Agent.DataMgmt.IBookkeepingDB import IBookkeepingDB
from DIRAC                                                 import gLogger, S_OK, S_ERROR

__RCSID__ = "$Id: IBookkeepingDatabaseClient.py,v 1.2 2008/03/03 09:56:25 zmathe Exp $"

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
  def insertJobParameter(self, jobID, name, value, type):
    return self.getManager().insertJobParameter(jobID, name, value, type)
  
  #############################################################################
  def insertInputFile(self, jobID, inputfile):
    return self.getManager().insertInputFile(jobID, inputfile)
  
  #############################################################################
  def insertOutputFile(self, jobID, name, value):
    return self.getManager().insertOutputFile(jobID, name, value)
  
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
