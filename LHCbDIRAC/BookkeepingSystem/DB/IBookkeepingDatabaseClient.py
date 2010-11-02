# $Id$
########################################################################

"""

"""

from LHCbDIRAC.BookkeepingSystem.DB.IBookkeepingDB             import IBookkeepingDB
from DIRAC                                                     import gLogger, S_OK, S_ERROR

__RCSID__ = "$Id$"

class IBookkeepingDatabaseClient(object):
    
  #############################################################################
  def __init__(self, DatabaseManager = IBookkeepingDB()):
    self.databaseManager_ = DatabaseManager
    
  #############################################################################
  def getManager(self):
    return self.databaseManager_
  
  #############################################################################
  def getAvailableSteps(self, dict={}):
    return self.getManager().getAvailableSteps(dict)
  
  #############################################################################
  def getAvailableFileTypes(self):
    return self.getManager().getAvailableFileTypes()
  
  #############################################################################
  def insertFileTypes(self, ftype, desc):
    return self.getManager().insertFileTypes(ftype, desc)
  
  #############################################################################
  def insertStep(self, dict):
    return self.getManager().insertStep(dict)
  
  #############################################################################
  def deleteStep(self, stepid):
    return self.getManager().deleteStep(stepid)
  
  #############################################################################
  def updateStep(self, dict):
    return self.getManager().updateStep(dict)
  
  #############################################################################
  def getStepInputFiles(self, StepId):
    return self.getManager().getStepInputFiles(StepId)
  
  #############################################################################
  def setStepInputFiles(self, stepid, files):
    return self.getManager().setStepInputFiles(stepid, files)
  
  #############################################################################
  def setStepOutputFiles(self, stepid, files):
    return self.getManager().setStepOutputFiles(stepid, files)
  
  #############################################################################
  def getStepOutputFiles(self, StepId):
    return self.getManager().getStepOutputFiles(StepId)
  
  #############################################################################
  def getAvailableConfigNames(self):
    return self.getManager().getAvailableConfigNames()
  
  #############################################################################
  def getConfigVersions(self, configname):
    return self.getManager().getConfigVersions(configname)
  #############################################################################
  def getConditions(self, configName, configVersion):
    return self.getManager().getConditions(configName, configVersion)
  
  #############################################################################
  def getProcessingPass(self, configName, configVersion, conddescription, path):
    return self.getManager().getProcessingPass(configName, configVersion, conddescription, path)
  
  #############################################################################
  def getProductions(self, configName, configVersion, conddescription, processing, evt):
    return self.getManager().getProductions(configName, configVersion, conddescription, processing, evt)
  
  #############################################################################
  def getFileTypes(self, configName, configVersion, conddescription, processing, evt, production):
    return self.getManager().getFileTypes(configName, configVersion, conddescription, processing, evt, production)
  
  #############################################################################
  def getFiles(self, configName, configVersion, conddescription, processing, evt, production, filetype, quality):
    return self.getManager().getFiles(configName, configVersion, conddescription, processing, evt, production, filetype, quality)
  
  #############################################################################  
  def getAvailableDataQuality(self):
    return self.getManager().getAvailableDataQuality()
  
  #############################################################################
  def getAvailableProductions(self):
	 return self.getManager().getAvailableProductions()
  
  #############################################################################
  def getAvailableRuns(self):
    return self.getManager().getAvailableRuns()
  
  #############################################################################
  def getAvailableEventTypes(self):
    return self.getManager().getAvailableEventTypes()
  
  #############################################################################
  def getMoreProductionInformations(self, prodid):
    return self.getManager().getMoreProductionInformations(prodid)
  
  #############################################################################
  def getJobInfo(self, lfn):
    return self.getManager().getJobInfo(lfn)
  
  #############################################################################
  def getRunNumber(self, lfn):
    return self.getManager().getRunNumber(lfn)
  
  #############################################################################
  def getProductionFiles(self, prod, ftype, gotreplica='ALL'):
    return self.getManager().getProductionFiles(prod, ftype, gotreplica)
  
  #############################################################################
  def getFilesForAGivenProduction(self, dict):
    return self.getManager().getFilesForAGivenProduction(dict)
  
  #############################################################################
  def getAvailableRunNumbers(self):
    return self.getManager().getAvailableRunNumbers()
  
  #############################################################################
  def getRunFiles(self, runid):
    return self.getManager().getRunFiles(runid)
  
  #############################################################################
  def updateFileMetaData(self, filename, filesAttr):
    return self.getManager().updateFileMetaData(filename, filesAttr)
  
  #############################################################################
  def renameFile(self, oldLFN, newLFN):
    return self.getManager().renameFile(oldLFN, newLFN)
  
  #############################################################################
  def getInputAndOutputJobFiles(self, jobids):
    return self.getManager().getInputAndOutputJobFiles(jobids)
  
  #############################################################################
  def getInputAndOutputJobFiles(self, jobids):
    return self.getManager().getInputAndOutputJobFiles(jobids)
  
  #############################################################################
  def getJobsIds(self, filelist):
    return self.getManager().getJobsIds(filelist)
  
  #############################################################################
  def insertTag(self, name, tag):
    return self.getManager().insertTag(name, tag)
  
  #############################################################################  
  def setQuality(self, lfns, flag):
    return self.getManager().setQuality(lfns, flag)
 
  #############################################################################
  def setRunQualityWithProcessing(self, runNB, procpass, flag):
    return self.getManager().setRunQualityWithProcessing(runNB, procpass, flag)
  
  #############################################################################  
  def setQualityRun(self, runNb, flag):
    return self.getManager().setQualityRun(runNb, flag)
  
  #############################################################################
  def setQualityProduction(self, prod, flag):
    return self.getManager().setQualityProduction(prod, flag)
    
  #############################################################################
  def getSimCondIDWhenFileName(self, fileName):
    return self.getManager().getSimCondIDWhenFileName(fileName)
  
  #############################################################################  
  def getLFNsByProduction(self, prodid):
    raise self.getManager().getLFNsByProduction(prodid)
  
  #############################################################################  
  def getAncestors(self, lfn, depth):
    return self.getManager().getAncestors(lfn, depth)
  
  #############################################################################  
  def getAllAncestors(self, lfn, depth):
    return self.getManager().getAllAncestors(lfn, depth)
  
  #############################################################################  
  def getAllAncestorsWithFileMetaData(self, lfn, depth):
    return self.getAllAncestorsWithFileMetaData(lfn, depth)
  
  #############################################################################  
  def getAllDescendents(self, lfn, depth, production, checkreplica):
    return self.getManager().getAllDescendents(lfn, depth, production, checkreplica)
  
  #############################################################################  
  def getDescendents(self, lfn, depth ):
    return self.getManager().getDescendents(lfn, depth)
  
  #############################################################################
  def checkfile(self, fileName): #file
    return self.getManager().checkfile(fileName)
  
  #############################################################################
  def checkFileTypeAndVersion(self, type, version): #fileTypeAndFileTypeVersion(self, type, version):
    return self.getManager().checkFileTypeAndVersion(type, version)
  
  #############################################################################
  def checkEventType(self, eventTypeId):  #eventType(self, eventTypeId):
    return self.getManager().checkEventType(eventTypeId)
  
  #############################################################################
  def insertJob(self, job):
    return self.getManager().insertJob(job)
  
  #############################################################################
  def insertInputFile(self, jobID, FileId):
    return self.getManager().insertInputFile(jobID, FileId)
  
  #############################################################################
  def insertOutputFile(self, file):
    return self.getManager().insertOutputFile(file)
  
  #############################################################################
  def updateReplicaRow(self, fileID, replica):
    return self.getManager().updateReplicaRow(fileID, replica)
  
  #############################################################################
  def deleteJob(self, job):
    return self.getManager().deleteJob(job)
  
  #############################################################################
  def deleteInputFiles(self, jobid):
    return self.getManager().deleteInputFiles(jobid)
  
  #############################################################################
  def deleteFiles(self, lfns):
    return self.getManager().deleteFiles(lfns)
  
  #############################################################################
  def insertSimConditions(self, simdesc, BeamCond, BeamEnergy, Generator, MagneticField, DetectorCond, Luminosity):
    return self.getManager().insertSimConditions(simdesc, BeamCond, BeamEnergy, Generator, MagneticField, DetectorCond, Luminosity)
  
  #############################################################################
  def getSimConditions(self):
    return self.getSimConditions()
  
  #############################################################################
  def removeReplica(self, fileName):
    return self.getManager().removeReplica(fileName)
  
  #############################################################################
  def getFileMetadata(self, lfns):
    return self.getManager().getFileMetadata(lfns)
  
  #############################################################################
  def getFilesInformations(self,lfns):
    return self.getManager().getFilesInformations(lfns)
  
  #############################################################################
  def getFileMetaDataForUsers(self, lfns):
    return self.getManager().getFileMetaDataForUsers(lfns)