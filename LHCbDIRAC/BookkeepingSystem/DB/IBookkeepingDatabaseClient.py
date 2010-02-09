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
  def file(self, fileName):
    return self.getManager().file(fileName)
  
  #############################################################################
  def fileTypeAndFileTypeVersion(self, type, version):
    return self.getManager().fileTypeAndFileTypeVersion(type, version)
  
  #############################################################################
  def getFilesInformations(self,lfns):
    return self.getManager().getFilesInformations(lfns)
  
  #############################################################################
  def getProductionsWithPrgAndEvt(self, programName='ALL', programversion='ALL', evt='ALL'):
    return self.getManager().getProductionsWithPrgAndEvt(programName, programversion, evt)
  
  #############################################################################
  def eventType(self, eventTypeId):
    return self.getManager().eventType(eventTypeId)
  
  #############################################################################
  def getProcessingPassDescfromProduction(self, prod):
    return self.getManager().getProcessingPassDescfromProduction(prod)
  
  #############################################################################
  def getLogfile(self, lfn):
    return self.getManager().getLogfile(lfn)
  
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
  def insertOutputFile(self, file):
    return self.getManager().insertOutputFile(file)
  
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
  def getAvailableProductions(self):
    return self.getManager().getAvailableProductions()
  
  #############################################################################
  def deleteFiles(self, lfns):
    return self.getManager().deleteFiles(lfns)
  
  #############################################################################
  def insertQuality(self, fileID, group, flag ):
    return self.getManager().insertQuality(fileID, group, flag)
  
  #############################################################################
  def insertQualityParam(self, fileID, qualityID, name, value):
    return self.getManager().insertQualityParam(fileID, name, value)
  
  #############################################################################
  def getMoreProductionInformations(self, prodid):
    return self.getManager().getMoreProductionInformations(prodid)
  
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
  def updateReplicaRow(self, fileID, replica): #, name, location):
    return self.getManager().updateReplicaRow(fileID, replica)
  
  #############################################################################
  def removeReplica(self, File):
    return self.getManager().removeReplica(File)
  
  #############################################################################
  def getAvailableRuns(self):
    return self.getManager().getAvailableRuns()
  
  #############################################################################
  def getProductionFiles(self, prod, fileType, replica='ALL'):
    return self.getManager().getProductionFiles(prod, fileType, replica)
  
  #############################################################################
  def getProductionFilesWithAGivenDate(self, prod, ftype, startDate = None, endDate = None):
    return self.getManager().getProductionFilesWithAGivenDate(prod, ftype, startDate, endDate)
  
  #############################################################################
  def getProductionSummary(self, cName, cVersion, simdesc='ALL', pgroup='ALL', production='ALL', ftype='ALL', evttype='ALL'):
    return  self.getManager().getProductionSummary(cName, cVersion, simdesc, pgroup, production, ftype, evttype)
  
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
  def getAvailableConfigNames(self):
    return self.getManager().getAvailableConfigNames()
  
  #############################################################################
  def getConfigVersions(self, configname):
    return self.getManager().getConfigVersions(configname)
  
  #############################################################################
  def getSimulationConditions(self, configName, configVersion, realdata):
    return self.getManager().getSimulationConditions(configName, configVersion, realdata)
  
  #############################################################################
  def getProPassWithSimCond(self, configName, configVersion, simcondid):
    return self.getManager().getProPassWithSimCond(configName, configVersion, simcondid)
  
  def getProPassWithSimCondNew(self, configName, configVersion, simcondid):
    return self.getManager().getProPassWithSimCondNew(configName, configVersion, simcondid)
  
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
  def getSimConditions(self):
    return self.getManager().getSimConditions()
  
  #############################################################################
  def getProductionSimulationCond(self, prod):
    return self.getManager().getProductionSimulationCond(prod)
  
  #############################################################################
  def getProductionProcessing(self, prod):
    return self.getManager().getProductionProcessing(prod)
  
  #############################################################################
  def getLimitedFilesWithSimcond(self, configName, configVersion, simcondid, procPass, evtId, prod, ftype, progName, progVersion, startitem, maxitems):
    return self.getManager().getLimitedFilesWithSimcond(configName, configVersion, simcondid, procPass, evtId, prod, ftype, progName, progVersion, startitem, maxitems)
  
  #############################################################################
  def getRunInformations(self, runnb):
    return self.getManager().getRunInformations(runnb)
  
  #############################################################################
  def checkProductionStatus(self, productionid = None, lfns = []):
    return self.getManager().checkProductionStatus(productionid, lfns)
  
  #############################################################################
  def getAvailableEventTypes(self):
    return self.getManager().getAvailableEventTypes()
  
  #############################################################################
  def getAvailableFileTypes(self):
    return self.getManager().getAvailableFileTypes()
  
  #############################################################################
  def getFileMetaDataForUsers(self, lfns):
    return self.getManager().getFileMetaDataForUsers(lfns)
  
  #############################################################################
  def getProcessingPassDesc(self, totalproc, passid, simid='ALL'):
    return self.getManager().getProcessingPassDesc(totalproc, passid, simid)
  
  #############################################################################
  def getProcessingPassDesc_new(self, totalproc, simid='ALL'):
    return self.getManager().getProcessingPassDesc_new(totalproc, simid)
  
  #############################################################################
  def getProductionFilesForUsers(self, prod, ftype, SortDict, StartItem, Maxitems):
    return self.getManager().getProductionFilesForUsers(prod, ftype, SortDict, StartItem, Maxitems)
  
  #############################################################################
  def getEventTypes(self, configName, configVersion):
    return self.getManager().getEventTypes(configName, configVersion)
  
  #############################################################################
  def getSimCondWithEventType(self, configName, configVersion, eventType, realdata):
    return self.getManager().getSimCondWithEventType(configName, configVersion, eventType, realdata)
  
  #############################################################################
  def getProPassWithEventType(self, configName, configVersion, eventType, simcond):
    return self.getManager().getProPassWithEventType(configName, configVersion, eventType, simcond)
  
  #############################################################################
  def getProPassWithEventTypeNew(self, configName, configVersion, eventType, simcond):
    return self.getManager().getProPassWithEventTypeNew(configName, configVersion, eventType, simcond)
  
  #############################################################################
  def insert_procressing_pass(self, programs, groupdesc, simcond, inputProdTotalProcessingPass, production):
    return self.getManager().insert_procressing_pass(programs, groupdesc, simcond, inputProdTotalProcessingPass, production)
  
  #############################################################################
  def getSpecificFiles(self,configName, configVersion, programName, programVersion, fileType, eventTypeId, production):
    return self.getManager().getSpecificFiles(configName, configVersion, programName, programVersion, fileType, eventTypeId, production)
  
  #############################################################################
  def getPass_index(self):
    return self.getManager().getPass_index()
  
  #############################################################################  
  def checkPass_index(self, programs):
    return self.getManager().checkPass_index(programs)
  
  #############################################################################  
  def insert_pass_index(self, groupdesc, step0, step1, step2, step3, step4, step5, step6):
    return self.getManager().insert_pass_index( groupdesc, step0, step1, step2, step3, step4, step5, step6)
  
  #############################################################################  
  def insert_pass_index_new(self, groupdesc, step0, step1, step2, step3, step4, step5, step6):
    return self.getManager().insert_pass_index_new( groupdesc, step0, step1, step2, step3, step4, step5, step6)
  
  #############################################################################  
  def insertProcessing(self, production, passdesc, inputprod, simdesc):
    return self.getManager().insertProcessing(production, passdesc, inputprod, simdesc)
  
  #############################################################################  
  def insert_procressing_passRealData(self, steps, groupdesc, daqdesc, inputProdTotalProcessingPass, production):
    return self.getManager().insert_procressing_passRealData(steps, groupdesc, daqdesc, inputProdTotalProcessingPass, production)
  
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
  def getSimulationCondIdByDesc(self, desc):
    return self.getManager().getSimulationCondIdByDesc(desc)
  
  #############################################################################
  def getSimCondIDWhenFileName(self, fileName):
    return self.getManager().getSimCondIDWhenFileName(fileName)
  
  #############################################################################
  def getLFNsByProduction(self, prodid):
    return self.getManager().getLFNsByProduction(prodid)
  
  #############################################################################  
  def getLimitedNbOfFiles(self,configName, configVersion, simcondid, procPass, evtId, prod, ftype, progName, progVersion):
    return self.getManager().getLimitedNbOfFiles(configName, configVersion, simcondid, procPass, evtId, prod, ftype, progName, progVersion)
    
  #############################################################################
  def insertSimConditions(self, simdesc, BeamCond, BeamEnergy, Generator, MagneticField, DetectorCond, Luminosity):
    return self.getManager().insertSimConditions(simdesc, BeamCond, BeamEnergy, Generator, MagneticField, DetectorCond, Luminosity)
  
  #############################################################################
  def insertEventTypes(self, evtid, desc, primary):
    return self.getManager().insertEventTypes(evtid,desc,primary)
  
  #############################################################################
  def getAncestors(self, lfn, depth):
    return self.getManager().getAncestors(lfn, depth)
  
  #############################################################################
  def getAllAncestors(self, lfn, depth):
    return self.getManager().getAllAncestors(lfn, depth)
  
  #############################################################################
  def getAllAncestorsWithFileMetaData(self, lfn, depth):
    return self.getManager().getAllAncestorsWithFileMetaData(lfn, depth)
  
  #############################################################################
  def getDescendents(self, lfn, depth):
      return self.getManager().getDescendents(lfn, depth)
    
  #############################################################################
  def updateEventType(self, evid, desc, primary):
    return self.getManager().updateEventType(evid, desc, primary)
  
  #############################################################################
  def insertDataTakingCond(self, conditions): 
    return self.getManager().insertDataTakingCond(conditions)
  
  #############################################################################
  def getDataTakingCondId(self, condition):
    return self.getManager().getDataTakingCondId(condition)
  
  #############################################################################
  def getJobInfo(self, lfn):
    return self.getManager().getJobInfo(lfn)
  
  #############################################################################
  def getPassIndexID(self, programName, programVersion):
    return self.getManager().getPassIndexID(programName, programVersion)
  
  #############################################################################
  def insertProcessing_pass(self, prod, passid, simcond):
    return self.getManager().insertProcessing_pass(prod, passid, simcond)
  
  #############################################################################
  def listProcessingPass(self, prod = None):
    return self.getManager().listProcessingPass(prod)
  
  #############################################################################
  def getProcessingPassGroups(self):
     return self.getManager().getProcessingPassGroups()
  
  #############################################################################
  def insert_pass_group(self, gropupdesc):
    return self.getManager().insert_pass_group(gropupdesc)
  
  #############################################################################
  def updateFileMetaData(self, filename, filesAttr):
    return self.getManager().updateFileMetaData(filename, filesAttr)
  
  #############################################################################
  def renameFile(self, oldLFN, newLFN):
    return self.getManager().renameFile(oldLFN, newLFN)
  
  #############################################################################
  def getJobsIds(self, filelist):
    return self.getManager().getJobsIds(filelist)
  
  #############################################################################
  def getInputAndOutputJobFiles(self, jobids):
    return self.getManager().getInputAndOutputJobFiles(jobids)
  
  #############################################################################
  def checkProcessingPassAndSimCond(self, production):
    return self.getManager().checkProcessingPassAndSimCond(production)
  
  #############################################################################
  def getFilesWithGivenDataSets(self, simdesc, datataking, procPass,ftype, evt, configname, configversion, prod, flag , startDate, endDate):
    return self.getManager().getFilesWithGivenDataSets(simdesc, datataking, procPass,ftype, evt, configname, configversion, prod, flag, startDate, endDate)
  
  #############################################################################
  def insert_aplications(self, appName, appVersion, option, dddb, condb, extrapack):
    return self.getManager().insert_aplications(appName, appVersion, option, dddb, condb, extrapack)
  
  #############################################################################
  def insert_pass_index_migration(self, passid, descr, groupid, step0,step1, step2,step3,step4,step5,step6):
    return self.getManager().insert_pass_index_migration(passid, descr, groupid, step0,step1, step2,step3,step4,step5,step6)
  
  #############################################################################
  def getSteps(self, prodid):
    return self.getManager().getSteps(prodid)
  
  #############################################################################
  def checkAddProduction(self, steps, groupdesc, simcond, inputProdTotalProcessingPass, production):  
    return self.getManager().checkAddProduction(steps, groupdesc, simcond, inputProdTotalProcessingPass, production)
  
  #############################################################################
  def setQuality(self, lfns, flag):
    return self.getManager().setQuality(lfns, flag)
  
  #############################################################################
  def setQualityRun(self, runNb, flag):
    return self.getManager().setQualityRun(runNb, flag)
  
  #############################################################################
  def setQualityProduction(self, prod, flag):
    return self.getManager().setQualityProduction(prod, flag)
  
  #############################################################################
  def getAvailableDataQuality(self):
    return self.getManager().getAvailableDataQuality()
  
  #############################################################################
  def getRunFiles(self, runid):
    return self.getManager().getRunFiles(runid)
  
  #############################################################################
  def getRunNumber(self, lfn):
    return self.getManager().getRunNumber(lfn)
  
  #############################################################################
  def getAvailableRunNumbers(self):
    return self.getManager().getAvailableRunNumbers()
  
  #############################################################################
  def getProPassWithRunNumber(self, runnumber):
    return self.getManager().getProPassWithRunNumber(runnumber)
  
  #############################################################################
  def getEventTypeWithAgivenRuns(self,runnumber, processing):
    return self.getManager().getEventTypeWithAgivenRuns(runnumber, processing)
  
  #############################################################################
  def getFileTypesWithAgivenRun(self, runnumber, procPass, evtId):
    return self.getManager().getFileTypesWithAgivenRun(runnumber, procPass, evtId)
  
  #############################################################################
  def getLimitedNbOfRunFiles(self,  procPass, evtId, runnumber, ftype):
    return self.getManager().getLimitedNbOfRunFiles(procPass, evtId, runnumber, ftype)
  
  #############################################################################
  def getLimitedFilesWithAgivenRun(self, procPass, evtId, runnumber, ftype, startitem, maxitems):
    return self.getManager().getLimitedFilesWithAgivenRun(procPass, evtId, runnumber, ftype, startitem, maxitems)
  
  #############################################################################
  def getRunFilesWithAgivenRun(self, procPass, evtId, runnumber, ftype):
    return self.getManager().getRunFilesWithAgivenRun(procPass, evtId, runnumber, ftype)
  
  #############################################################################
  def getFileHistory(self, lfn):
    return self.getManager().getFileHistory(lfn)
  
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
  
  #############################################################################
  def getConfigsAndEvtType(self, prodid):
    return self.getManager().getConfigsAndEvtType(prodid)
  
  #############################################################################
  def getProductionInformationsFromView(self, prodid):
    return self.getManager().getProductionInformationsFromView(prodid)
  
  '''
  END MONITORING
  '''