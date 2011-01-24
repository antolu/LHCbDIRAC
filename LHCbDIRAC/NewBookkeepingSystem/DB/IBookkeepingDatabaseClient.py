# $Id$
########################################################################

"""

"""

from LHCbDIRAC.NewBookkeepingSystem.DB.IBookkeepingDB             import IBookkeepingDB
from DIRAC                                                        import gLogger, S_OK, S_ERROR

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
  def getConditions(self, configName, configVersion, evt):
    return self.getManager().getConditions(configName, configVersion, evt)
  
  #############################################################################
  def getProcessingPass(self, configName, configVersion, conddescription, runnumber,prod, path):
    return self.getManager().getProcessingPass(configName, configVersion, conddescription, runnumber, prod, path)
  
  #############################################################################
  def getStandardProcessingPass(self, configName, configVersion, conddescription, eventType, prod, path='/'):
    return self.getManager().getStandardProcessingPass(configName, configVersion, conddescription, eventType, prod, path)
  
  #############################################################################
  def getProductions(self, configName, configVersion, conddescription, processing, evt):
    return self.getManager().getProductions(configName, configVersion, conddescription, processing, evt)
  
  #############################################################################
  def getFileTypes(self, configName, configVersion, conddescription, processing, evt, runnb, production):
    return self.getManager().getFileTypes(configName, configVersion, conddescription, processing, evt, runnb, production)
  
  #############################################################################
  def getFiles(self, configName, configVersion, conddescription, processing, evt, production, filetype, quality, runnb):
    return self.getManager().getFiles(configName, configVersion, conddescription, processing, evt, production, filetype, quality, runnb)
  
  #############################################################################
  def getFilesSumary(self, configName, configVersion, conddescription, processing, evt, production, filetype, quality, runnb):
    return self.getManager().getFilesSumary(configName, configVersion, conddescription, processing, evt, production, filetype, quality, runnb)
  
  #############################################################################
  def getLimitedFiles(self, configName, configVersion, conddescription, processing, evt, production, filetype, quality, runnb, startitem, maxiten):
    return self.getManager().getLimitedFiles(configName, configVersion, conddescription, processing, evt, production, filetype, quality, runnb, startitem, maxiten)
  
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
  def getLFNsByProduction(self, prodid):
    return self.getManager().getLFNsByProduction(prodid)
  
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
    return self.getManager().getSimConditions()
  
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
  
  #############################################################################
  def getProductionFilesForUsers(self, prod, ftypeDict, SortDict, StartItem, Maxitems):
    return self.getManager().getProductionFilesForUsers(prod, ftypeDict, SortDict, StartItem, Maxitems)
  
  #############################################################################
  def exists(self, lfns):
    return self.getManager().exists(lfns)
  
  #############################################################################
  def addReplica(self, fileName):
    return self.getManager().addReplica(fileName)
  
  #############################################################################
  def getRunInformations(self, runnb):
    return self.getManager().getRunInformations(runnb)
  
  #############################################################################
  def getLogfile(self, lfn):
    return self.getManager().getLogfile(lfn)
  
  #############################################################################
  def updateEventType(self, evid, desc, primary):
    return self.getManager().updateEventType(evid, desc, primary)
  
  #############################################################################
  def getProductionSummary(self, cName, cVersion, simdesc, pgroup, production, ftype, evttype):
    return self.getManager().getProductionSummary(self, cName, cVersion, simdesc, pgroup, production, ftype, evttype)
  
  #############################################################################
  def getFileHistory(self, lfn):
    return self.getManager().getFileHistory(lfn)
  
  #############################################################################
  def getProductionInformationsFromView(self, prodid):
    return self.getManager().getProductionInformationsFromView(prodid)
  
  #############################################################################
  def getJobsNb(self, prodid):
    return self.getManager().getJobsNb(prodid)
    
  #############################################################################
  def getNumberOfEvents(self, prodid):
    return self.getManager().getNumberOfEvents(prodid)
  
  #############################################################################
  def getSizeOfFiles(self, prodid):
    return self.getManager().getSizeOfFiles(prodid)
  
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
  def getSteps(self, prodid):
    return self.getManager().getSteps(prodid)
  
  #############################################################################
  def getNbOfJobsBySites(self, prodid):
    return self.getManager().getNbOfJobsBySites(prodid)
  
  #############################################################################
  def getConfigsAndEvtType(self, prodid):
    return self.getManager().getConfigsAndEvtType(prodid)
  
  #############################################################################
  def getAvailableTags(self):
    return self.getManager().getAvailableTags()
  
  #############################################################################
  def getProcessedEvents(self, prodid):
    return self.getManager().getProcessedEvents(prodid)
  
  #############################################################################
  def getRunsWithAGivenDates(self, dict):
    return self.getManager().getRunsWithAGivenDates(dict)
  
  #############################################################################
  def getProductiosWithAGivenRunAndProcessing(self, dict):
    return self.getManager().getProductiosWithAGivenRunAndProcessing(dict)
  
  #############################################################################
  def getDataQualityForRuns(self, runs):
    return self.getManager().getDataQualityForRuns(runs)
  
  #############################################################################
  def setFilesInvisible(self, lfns):
    return self.getManager().setFilesInvisible(lfns)
  
  #############################################################################
  def getTotalProcessingPass(self, prod):
    return self.getManager().getTotalProcessingPass(prod)
  
  #############################################################################
  def getRunFlag(self, runnb, processing):
    return self.getManager().getRunFlag(runnb, processing)
  
  #############################################################################
  def getAvailableConfigurations(self):
    return self.getManager().getAvailableConfigurations()
  
  #############################################################################
  def getProductionProcessingPassID(self, prodid):
    return self.getManager().getProductionProcessingPassID(prodid)
  
  #############################################################################
  def getProductionProcessingPass(self, prodid):
    return self.getManager().getProductionProcessingPass(prodid)
  
  #############################################################################
  def getRunProcessingPass(self, runnumber):
    return self.getManager().getRunProcessingPass(runnumber)
  
  #############################################################################
  def checkProductionStatus(self, productionid = None, lfns = []):
    return self.getManager().checkProductionStatus(productionid, lfns)
  
  #############################################################################
  def getProductionSimulationCond(self, prod):
    return self.getManager().getProductionSimulationCond(prod)
  
  #############################################################################
  def getFilesWithGivenDataSets(self, simdesc, datataking, procPass, ftype, evt, configName, configVersion, production, flag, startDate, endDate, nbofEvents, startRunID, endRunID, runnumbers, replicaFlag):
    return self.getManager().getFilesWithGivenDataSets(simdesc, datataking, procPass, ftype, evt, configName, configVersion, production, flag, startDate, endDate, nbofEvents, startRunID, endRunID, runnumbers, replicaFlag)
  
  #############################################################################
  def getFilesWithGivenDataSetsForUsers(self, simdesc, datataking, procPass, ftype, evt, configName, configVersion, production, flag, startDate, endDate, nbofEvents, startRunID, endRunID, runnumbers, replicaFlag):
    return self.getManager().getFilesWithGivenDataSetsForUsers(simdesc, datataking, procPass, ftype, evt, configName, configVersion, production, flag, startDate, endDate, nbofEvents, startRunID, endRunID, runnumbers, replicaFlag)
  
  #############################################################################
  def getDataTakingCondId(self, condition):
    return self.getManager().getDataTakingCondId(condition)

  #############################################################################
  def getStepIdandName(self, programName, programVersion):
    return self.getManager().getStepIdandName(programName, programVersion)
  
  #############################################################################
  def addProduction(self, production, simcond, daq, steps, inputproc):
    return self.getManager().addProduction(production, simcond, daq, steps, inputproc)
  
  #############################################################################
  def checkProcessingPassAndSimCond(self, production):
    return self.getManager().checkProcessingPassAndSimCond(production)
  
  #############################################################################
  def getEventTypes(self, configName, configVersion):
    return self.getManager().getEventTypes(configName, configVersion)
  
  #############################################################################
  def getStandardEventTypes(self, configName, configVersion, prod):
    return self.getManager().getStandardEventTypes(configName, configVersion, prod )
  
  #############################################################################
  def getProcessingPassSteps(self, procpass, cond, stepname):
    return self.getManager().getProcessingPassSteps(procpass, cond, stepname)
      
  #############################################################################
  def getProductionProcessingPassSteps(self, prod):
    return self.getManager().getProductionProcessingPassSteps(prod)
  
  #############################################################################
  def getStepIdandNameForRUN(self, programName, programVersion):
    return self.getManager().getStepIdandNameForRUN(programName, programVersion)
  
  #############################################################################
  def getDataTakingCondDesc(self, condition):
    return self.getManager().getDataTakingCondDesc(condition)
  
  #############################################################################
  def getProductionOutputFiles(self, prod):
    return self.getManager().getProductionOutputFiles(prod)
  
  #############################################################################
  def existsTag(self, name, value):
    return self.getManager().existsTag(name, value)
  
  #############################################################################
  def getRunQuality(self, procpass, flag):
    return self.getManager().getRunQuality(procpass, flag)
    