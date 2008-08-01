########################################################################
# $Id: IBookkeepingDatabaseClient.py,v 1.10 2008/08/01 13:12:32 zmathe Exp $
########################################################################

"""

"""

from DIRAC.BookkeepingSystem.DB.IBookkeepingDB             import IBookkeepingDB
from DIRAC                                                 import gLogger, S_OK, S_ERROR

__RCSID__ = "$Id: IBookkeepingDatabaseClient.py,v 1.10 2008/08/01 13:12:32 zmathe Exp $"

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
  def removeReplica(self, File):
    return self.getManager().removeReplica(File)
  
  #############################################################################
  def addReplica(self, File):
    return self.getManager().addReplica(File)
  
  def getFileMetadata(self, lfns):
    return self.getManager().getFileMetadata(lfns)
  
  #############################################################################
  def exists(self, lfns):
    return self.getManager().exists(lfns)
    
  #############################################################################
  def deleteJob(self, job):
    return self.getManager().deleteJob(job)
  
  #############################################################################
  def deleteFile(self, file):
    return self.getManager().deleteFile(file)
  
  #############################################################################
  def deleteInputFiles(self, jobid):
    return self.getManager().deleteInputFiles(jobid)
  
  #############################################################################
  def getAvailableConfigurations(self):
    return self.getManager().getAvailableConfigurations()
  
  #############################################################################
  def getSimulationConditions(self, configName, configVersion):
    return self.getManager().getSimulationConditions(configName, configVersion)
  
  #############################################################################
  def getProPassWithSimCond(self, configName, configVersion, simcondid):
    return self.getManager().getProPassWithSimCond(configName, configVersion, simcondid)
  
  #############################################################################
  def getEventTypeWithSimcond(self,configName, configVersion, simcondid, procPass):
    return self.getManager().getEventTypeWithSimcond(configName, configVersion, simcondid, procPass)
  
  #############################################################################
  def getProductionsWithSimcond(self, configName, configVersion, simcondid, procPass, evtId):
    return self.getManager().getProductionsWithSimcond(configName, configVersion, simcondid, procPass, evtId)
  
  #############################################################################
  def getFileTypesWithSimcond(self, configName, configVersion, simcondid, procPass, evtId, prod):
    return self.getManager().getFileTypesWithSimcond(configName, configVersion, simcondid, procPass, evtId, prod)
  
  #############################################################################  
  def getProgramNameWithSimcond(self, configName, configVersion, simcondid, procPass, evtId, prod, ftype):
    return self.getManager().getProgramNameWithSimcond(configName, configVersion, simcondid, procPass, evtId, prod, ftype)
  
  #############################################################################  
  def getFilesWithSimcond(self, configName, configVersion, simcondid, procPass, evtId, prod, ftype, progName, progVersion):
    return self.getManager().getFilesWithSimcond(configName, configVersion, simcondid, procPass, evtId, prod, ftype, progName, progVersion)
  
  
  
  
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
  def getLFNsByProduction(self, prodid):
    return self.getManager().getLFNsByProduction(prodid)
  
  #############################################################################
  def insertSimConditions(self, BeamCond, BeamEnergy, Generator, MagneticField, DetectorCond, Luminosity):
    return self.getManager().insertSimConditions(BeamCond, BeamEnergy, Generator, MagneticField, DetectorCond, Luminosity)
  
  #############################################################################
  def insertEventTypes(self, evtid, desc, primary):
    return self.getManager().insertEventTypes(evtid,desc,primary)
  
  #############################################################################
  def getAncestors(self, lfn, depth):
    return self.getManager().getAncestors(lfn, depth)
  
  #############################################################################
  def updateEventType(self, evid, desc, primary):
    return self.getManager().updateEventType(evid, desc, primary)
  
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