# $Id$
########################################################################

"""

"""

from LHCbDIRAC.BookkeepingSystem.DB.IBookkeepingDB             import IBookkeepingDB
from DIRAC                                                        import gLogger, S_OK, S_ERROR

__RCSID__ = "$Id$"

class IBookkeepingDatabaseClient(object):

  #############################################################################
  def __init__(self, DatabaseManager=IBookkeepingDB()):
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
  def insertFileTypes(self, ftype, desc, fileType):
    return self.getManager().insertFileTypes(ftype, desc, fileType)

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
  def getProcessingPass(self, configName, configVersion, conddescription, runnumber, prod, evt, path):
    return self.getManager().getProcessingPass(configName, configVersion, conddescription, runnumber, prod, evt, path)

  #############################################################################
  def getProductions(self, configName, configVersion, conddescription, processing, evt):
    return self.getManager().getProductions(configName, configVersion, conddescription, processing, evt)

  #############################################################################
  def getFileTypes(self, configName, configVersion, conddescription, processing, evt, runnb, production):
    return self.getManager().getFileTypes(configName, configVersion, conddescription, processing, evt, runnb, production)

  #############################################################################
  def getFilesWithMetadata(self, configName, configVersion, conddescription, processing, evt, production, filetype, quality, runnb, visible):
    return self.getManager().getFilesWithMetadata(configName, configVersion, conddescription, processing, evt, production, filetype, quality, runnb, visible)

  #############################################################################
  def getFilesSummary(self, configName, configVersion, conddescription, processing, evt, production, filetype, quality, runnb, startrun, endrun, visible):
    return self.getManager().getFilesSummary(configName, configVersion, conddescription, processing, evt, production, filetype, quality, runnb, startrun, endrun, visible)

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
  def getJobInformation(self, dict):
    return self.getManager().getJobInformation(dict)

  #############################################################################
  def getRunNumber(self, lfn):
    return self.getManager().getRunNumber(lfn)

  #############################################################################
  def getProductionFiles(self, prod, ftype, gotreplica='ALL'):
    return self.getManager().getProductionFiles(prod, ftype, gotreplica)

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
  def getJobInputAndOutputJobFiles(self, jobids):
    return self.getManager().getJobInputAndOutputJobFiles(jobids)

  #############################################################################
  def insertTag(self, name, tag):
    return self.getManager().insertTag(name, tag)

  #############################################################################
  def setRunAndProcessingPassDataQuality(self, runNB, procpass, flag):
    return self.getManager().setRunAndProcessingPassDataQuality(runNB, procpass, flag)

  #############################################################################
  def setRunDataQuality(self, runNb, flag):
    return self.getManager().setRunDataQuality(runNb, flag)

  #############################################################################
  def setProductionDataQuality(self, prod, flag):
    return self.getManager().setProductionDataQuality(prod, flag)

  #############################################################################
  def getFileAncestors(self, lfn, depth, replica):
    return self.getManager().getFileAncestors(lfn, depth, replica)

  #############################################################################
  def getFileDescendents(self, lfn, depth, production, checkreplica):
    return self.getManager().getFileDescendents(lfn, depth, production, checkreplica)

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
  def insertSimConditions(self, simdesc, BeamCond, BeamEnergy, Generator, MagneticField, DetectorCond, Luminosity, G4settings):
    return self.getManager().insertSimConditions(simdesc, BeamCond, BeamEnergy, Generator, MagneticField, DetectorCond, Luminosity, G4settings)

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
  def getFileMetaDataForWeb(self, lfns):
    return self.getManager().getFileMetaDataForWeb(lfns)

  #############################################################################
  def getProductionFilesForWeb(self, prod, ftypeDict, SortDict, StartItem, Maxitems):
    return self.getManager().getProductionFilesForWeb(prod, ftypeDict, SortDict, StartItem, Maxitems)

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
  def getRunInformation(self, runnb):
    return self.getManager().getRunInformation(runnb)

  #############################################################################
  def getFileCreationLog(self, lfn):
    return self.getManager().getFileCreationLog(lfn)

  #############################################################################
  def updateEventType(self, evid, desc, primary):
    return self.getManager().updateEventType(evid, desc, primary)

  #############################################################################
  def getProductionSummary(self, cName, cVersion, simdesc, pgroup, production, ftype, evttype):
    return self.getManager().getProductionSummary(cName, cVersion, simdesc, pgroup, production, ftype, evttype)

  #############################################################################
  def getFileHistory(self, lfn):
    return self.getManager().getFileHistory(lfn)

  #############################################################################
  def getProductionInformationsFromView(self, prodid):
    return self.getManager().getProductionInformationsFromView(prodid)

  #############################################################################
  def getProductionNbOfJobs(self, prodid):
    return self.getManager().getProductionNbOfJobs(prodid)

  #############################################################################
  def getProductionNbOfEvents(self, prodid):
    return self.getManager().getProductionNbOfEvents(prodid)

  #############################################################################
  def getProductionSizeOfFiles(self, prodid):
    return self.getManager().getProductionSizeOfFiles(prodid)

  #############################################################################
  def getProductionNbOfFiles(self, prodid):
    return self.getManager().getProductionNbOfFiles(prodid)

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
  def getProductionProcessedEvents(self, prodid):
    return self.getManager().getProductionProcessedEvents(prodid)

  #############################################################################
  def getRunsForAGivenPeriod(self, dict):
    return self.getManager().getRunsForAGivenPeriod(dict)

  #############################################################################
  def getProductionsFromView(self, dict):
    return self.getManager().getProductionsFromView(dict)

  #############################################################################
  def getRunFilesDataQuality(self, runs):
    return self.getManager().getRunFilesDataQuality(runs)

  #############################################################################
  def setFilesInvisible(self, lfns):
    return self.getManager().setFilesInvisible(lfns)

  #############################################################################
  def setFilesVisible(self, lfns):
    return self.getManager().setFilesVisible(lfns)

  #############################################################################
  def getTotalProcessingPass(self, prod):
    return self.getManager().getTotalProcessingPass(prod)

  #############################################################################
  def getRunAndProcessingPassDataQuality(self, runnb, processing):
    return self.getManager().getRunAndProcessingPassDataQuality(runnb, processing)

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
  def getProductionFilesStatus(self, productionid=None, lfns=[]):
    return self.getManager().getProductionFilesStatus(productionid, lfns)

  #############################################################################
  def getProductionSimulationCond(self, prod):
    return self.getManager().getProductionSimulationCond(prod)

  #############################################################################
  def getFiles(self, simdesc, datataking, procPass, ftype, evt, configName, configVersion, production, flag, startDate, endDate, nbofEvents, startRunID, endRunID, runnumbers, replicaFlag, visible, filesize, tck):
    return self.getManager().getFiles(simdesc, datataking, procPass, ftype, evt, configName, configVersion, production, flag, startDate, endDate, nbofEvents, startRunID, endRunID, runnumbers, replicaFlag, visible, filesize, tck)

  #############################################################################
  def getVisibleFilesWithMetadata(self, simdesc, datataking, procPass, ftype, evt, configName, configVersion, production, flag, startDate, endDate, nbofEvents, startRunID, endRunID, runnumbers, replicaFlag, tcks):
    return self.getManager().getVisibleFilesWithMetadata(simdesc, datataking, procPass, ftype, evt, configName, configVersion, production, flag, startDate, endDate, nbofEvents, startRunID, endRunID, runnumbers, replicaFlag, tcks)

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
  def getEventTypes(self, configName, configVersion, production):
    return self.getManager().getEventTypes(configName, configVersion, production)

  #############################################################################
  def getProcessingPassSteps(self, procpass, cond, stepname):
    return self.getManager().getProcessingPassSteps(procpass, cond, stepname)

  #############################################################################
  def getProductionProcessingPassSteps(self, prod):
    return self.getManager().getProductionProcessingPassSteps(prod)

  #############################################################################
  def getStepIdandNameForRUN(self, programName, programVersion, conddb, dddb):
    return self.getManager().getStepIdandNameForRUN(programName, programVersion, conddb, dddb)

  #############################################################################
  def getDataTakingCondDesc(self, condition):
    return self.getManager().getDataTakingCondDesc(condition)

  #############################################################################
  def getProductionOutputFileTypes(self, prod):
    return self.getManager().getProductionOutputFileTypes(prod)

  #############################################################################
  def existsTag(self, name, value):
    return self.getManager().existsTag(name, value)

  #############################################################################
  def getRunWithProcessingPassAndDataQuality(self, procpass, flag):
    return self.getManager().getRunWithProcessingPassAndDataQuality(procpass, flag)

  #############################################################################
  def insertDataTakingCond(self, conditions):
    return self.getManager().insertDataTakingCond(conditions)

  #############################################################################
  def deleteSetpContiner(self, prod):
    return self.getManager().deleteSetpContiner(prod)

  #############################################################################
  def getRunNbAndTck(self, lfn):
    return self.getManager().getRunNbAndTck(lfn)

  #############################################################################
  def deleteProductionsContiner(self, prod):
    return self.getManager().deleteProductionsContiner(prod)

  #############################################################################
  def insertEventTypes(self, evid, desc, primary):
    return self.getManager().insertEventTypes(evid, desc, primary)

  #############################################################################
  def getRuns(self, cName, cVersion):
    return self.getManager().getRuns(cName, cVersion)

  #############################################################################
  def getRunAndProcessingPass(self, runnb):
    return self.getManager().getRunAndProcessingPass(runnb)

  #############################################################################
  def getProcessingPassId(self, fullpath):
    return self.getManager().getProcessingPassId(fullpath)

  #############################################################################
  def getNbOfRawFiles(self, runid, eventtype):
    return self.getManager().getNbOfRawFiles(runid, eventtype)

  #############################################################################
  def getFileTypeVersion(self, lfn):
    return self.getManager().getFileTypeVersion(lfn)

  #############################################################################
  def insertRuntimeProject(self, projectid, runtimeprojectid):
    return self.getManager().insertRuntimeProject(projectid, runtimeprojectid)

  #############################################################################
  def getRuntimeProjects(self, dict):
    return self.getManager().getRuntimeProjects(dict)

  #############################################################################
  def updateRuntimeProject(self, projectid, runtimeprojectid):
    return self.getManager().updateRuntimeProject(projectid, runtimeprojectid)

  #############################################################################
  def removeRuntimeProject(self, stepid):
    return self.getManager().removeRuntimeProject(stepid)

  #############################################################################
  def getTCKs(self, configName, configVersion, conddescription, processing, evt, production, filetype, quality, runnb):
    return self.getManager().getTCKs(configName, configVersion, conddescription, processing, evt, production, filetype, quality, runnb)

  #############################################################################
  def getStepsMetadata(self, configName, configVersion, cond, procpass, evt, production, filetype, runnb):
    return self.getManager().getStepsMetadata(configName, configVersion, cond, procpass, evt, production, filetype, runnb)

  #############################################################################
  def getDirectoryMetadata(self, lfn):
    return self.getManager().getDirectoryMetadata(lfn)

  #############################################################################
  def getFilesForGUID(self, guid):
    return self.getManager().getFilesForGUID(guid)

  #############################################################################
  def setFileDataQuality(self, lfns, flag):
    return self.getManager().setFileDataQuality(lfns, flag)

  #############################################################################
  def getRunsGroupedByDataTaking(self):
    return self.getManager().getRunsGroupedByDataTaking()
