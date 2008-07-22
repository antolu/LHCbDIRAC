########################################################################
# $Id: IBookkeepingDatabaseClient.py,v 1.5 2008/07/22 14:14:50 zmathe Exp $
########################################################################

"""

"""

from DIRAC.BookkeepingSystem.DB.IBookkeepingDB             import IBookkeepingDB
from DIRAC                                                 import gLogger, S_OK, S_ERROR

__RCSID__ = "$Id: IBookkeepingDatabaseClient.py,v 1.5 2008/07/22 14:14:50 zmathe Exp $"

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
  def insertJob(self, job):
    return self.getManager().insertJob(job)
  
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
  def insertInputFile(self, jobID, FileId):
    return self.getManager().insertInputFile(jobID, FileId)
    
  #############################################################################
  def insertOutputFile(self, job, file):
    return self.getManager().insertOutputFile(job, file)
      
  #############################################################################
  def updateReplicaRow(self, fileID, replica): #, name, location):
    return self.getManager().updateReplicaRow(fileID, replica)
  
  #############################################################################
  def removeReplica(self, File, Name, Locations, SE):
    return self.getManager().removeReplica(File, Name, Locations, SE)
  
  #############################################################################
  def addReplica(self, File, Name, Locations, SE):
    return self.getManager().addReplica(File, Name, Locations, SE)
    
  #############################################################################
  def deleteJob(self, job):
    return self.getManager().deleteJob(job)
  
  #############################################################################
  def deleteFile(self, file):
    return self.getManager().deleteFile(file)
  
  #############################################################################
  def getAvailableConfigurations(self):
    return self.getManager().getAvailableConfigurations()
  
  #############################################################################
  def getAvailableEventTypes(self):
    return self.getManager().getAvailableEventTypes()
  
  #############################################################################
  def getEventTypes(self, configName, configVersion):
    return self.getManager().getEventTypes(configName, configVersion)
  
  #############################################################################
  def getSpecificFiles(self,configName, configVersion, programName, programVersion, fileType, eventTypeId, production):
    return self.getManager().getSpecificFiles(configName, configVersion, programName, programVersion, fileType, eventTypeId, production)
  
  #############################################################################
  def getProcessingPass(self):
    return self.getManager().getProcessingPass()
  
  #############################################################################  
  def getProductionsWithPocessingPass(self, processingPass):
    return self.getManager().getProductionsWithPocessingPass(processingPass)
  
  #############################################################################  
  def getFilesByProduction(self, production, eventtype, filetype):
    return self.getManager().getFilesByProduction(production, eventtype, filetype)
  
  #############################################################################
  def getProductions(self, configName, configVersion, eventTypeId):
    return self.getManager().getProductions(configName, configVersion, eventTypeId)
  
  #############################################################################
  def getEventTyesWithProduction(self, production):
    return self.getManager().getEventTyesWithProduction(production)
  
  #############################################################################  
  def getFileTypesWithProduction(self, production, eventType):
    return self.getManager().getFileTypesWithProduction(production, eventType)
  
  #############################################################################  
  def getSpecificFilesWithoutProd(self, configName, configVersion, pname, pversion, filetype, eventType):
    return self.getManager().getSpecificFilesWithoutProd(configName, configVersion, pname, pversion, filetype, eventType)
  
  #############################################################################  
  def getFileTypes(self, configName, configVersion, eventType, prod):
    return self.getManager().getFileTypes(configName, configVersion, eventType, prod)
  
  #############################################################################  
  def getProgramNameAndVersion(self, configName, configVersion, eventType, prod, fileType):
    return self.getManager().getProgramNameAndVersion(configName, configVersion, eventType, prod, fileType)
  
  #############################################################################  
  def getConfigNameAndVersion(self, eventTypeId):
    return self.getManager().getConfigNameAndVersion(eventTypeId)
  
  #############################################################################  
  def getAvailableProcessingPass(self, configName, configVersion, eventTypeId):
    return self.getManager().getAvailableProcessingPass(configName, configVersion, eventTypeId)

  #############################################################################
  def getFileTypesWithEventType(self, configName, configVersion, eventTypeId, production):
    return self.getManager().getFileTypesWithEventType(configName, configVersion, eventTypeId, production)
  
  #############################################################################
  def getFileTypesWithEventTypeALL(self, configName, configVersion, eventTypeId):
    return self.getManager().getFileTypesWithEventTypeALL(configName, configVersion, eventTypeId)
  
  #############################################################################
  def getFilesByEventType(self, configName, configVersion, fileType, eventTypeId, production):
    return self.getManager().getFilesByEventType(configName, configVersion, fileType, eventTypeId, production)
  
  #############################################################################
  def getFilesByEventTypeALL(self, configName, configVersion, fileType, eventTypeId):
    return self.getManager().getFilesByEventTypeALL(configName, configVersion, fileType, eventTypeId)
  
  #############################################################################
  def getProductionsWithEventTypes(self, eventType, configName,  configVersion, processingPass):
    return self.getManager().getProductionsWithEventTypes(eventType, configName,  configVersion, processingPass)
  
  #############################################################################
  def getSimulationCondID(self, BeamCond, BeamEnergy, Generator, MagneticField, DetectorCond, Luminosity):
    return self.getManager().getSimulationCondID(BeamCond, BeamEnergy, Generator, MagneticField, DetectorCond, Luminosity)
  
  #############################################################################
  def getSimCondIDWhenFileName(self, fileName):
    return self.getManager().getSimCondIDWhenFileName(fileName)
  
  #############################################################################
  def insertSimConditions(self, BeamCond, BeamEnergy, Generator, MagneticField, DetectorCond, Luminosity):
    return self.getManager().insertSimConditions(BeamCond, BeamEnergy, Generator, MagneticField, DetectorCond, Luminosity)
  
  #############################################################################
  def insertEventTypes(self, evtid, desc, primary):
    return self.getManager().insertEventTypes(evtid,desc,primary)
  
  #############################################################################
  '''
    MONITORING
  '''
  def getJobsNb(self, prodid):
    return self.getManager().getJobsNb(prodid)
  
  #############################################################################
  def getNumberOfEvents(self, prodid):
    return self.getManager().getNumberOfEvents(prodid)
  
  #############################################################################
  def getSizeOfFiles(self, prodid):
    return self.getManager().getSizeOfFiles(prodid)
  
  #############################################################################
  def getNbOfFiles(self, prodid):
    return self.getManager().getNbOfFiles(prodid)
  
  #############################################################################
  def getProductionInformation(self, prodid):
    return self.getManager().getProductionInformation(prodid)
  
  #############################################################################
  def getNbOfJobsBySites(self, prodid):
    return self.getManager().getNbOfJobsBySites(prodid)
  
  '''
  END MONITORING
  '''