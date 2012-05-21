########################################################################
# $Id$
########################################################################

"""

"""

__RCSID__ = "$Id$"

from DIRAC                                      import gLogger, S_OK, S_ERROR

class IBookkeepingDB(object):

  #############################################################################
  def __init__(self):
    pass

  #############################################################################
  def getAvailableSteps(self, dict):
    gLogger.error('This method is not implemented!')

  #############################################################################
  def getAvailableFileTypes(self):
    gLogger.error('This method is not implemented!')

  #############################################################################
  def insertFileTypes(self, ftype, desc, fileType):
    gLogger.error('This method is not implemented!')

  #############################################################################
  def insertStep(self, dict):
    gLogger.error('This method is not implemented!')

  #############################################################################
  def getSimulationConditions(self, configName, configVersion, realdata):
    gLogger.error('This method is not implemented!')

  def deleteStep(self, stepid):
    gLogger.error('This method is not implemented!')

  #############################################################################
  def updateStep(self, dict):
    gLogger.error('This method is not implemented!')

  #############################################################################
  def getStepInputFiles(self, StepId):
    gLogger.error('This method is not implemented!')

  #############################################################################
  def setStepInputFiles(self, stepid, files):
    gLogger.error('This method is not implemented!')

  #############################################################################
  def setStepOutputFiles(self, stepid, files):
    gLogger.error('This method is not implemented!')

  #############################################################################
  def getStepOutputFiles(self, StepId):
    gLogger.error('This method is not implemented!')

  #############################################################################
  def getAvailableConfigNames(self):
    gLogger.error('This method is not implemented!')

  #############################################################################
  def getConfigVersions(self, configname):
    gLogger.error('This method is not implemented!')

  #############################################################################
  def getProcessingPass(self, configName, configVersion, conddescription, runnumber, prod, evt, path):
    gLogger.error('This method is not implemented!')

  #############################################################################
  def getConditions(self, configName, configVersion, evt):
    gLogger.error('This method is not implemented!')

  #############################################################################
  def getProductions(self, configName, configVersion, conddescription, processing, evt):
    gLogger.error('This method is not implemented!')

  #############################################################################
  def getFileTypes(self, configName, configVersion, conddescription, processing, evt, runnb, production):
    gLogger.error('This method is not implemented!')

  #############################################################################
  def getFilesWithMetadata(self, configName, configVersion, conddescription, processing, evt, production, filetype, quality, runnb, visible):
    gLogger.error('This method is not implemented!')

  #############################################################################
  def getFilesSummary(self, configName, configVersion, conddescription, processing, evt, production, filetype, quality, runnb, startrun, endrun, visible):
    gLogger.error('This method is not implemented!')

  #############################################################################
  def getLimitedFiles(self, configName, configVersion, conddescription, processing, evt, production, filetype, quality, runnb, startItem, maxItem):
    gLogger.error('This method is not implemented!')

  #############################################################################
  def getAvailableDataQuality(self):
    gLogger.error('This method is not implemented!')

  #############################################################################
  def getAvailableProductions(self):
    gLogger.error('This method is not implemented!')

  #############################################################################
  def getAvailableRuns(self):
    gLogger.error('This method is not implemented!')

  #############################################################################
  def getAvailableEventTypes(self):
    gLogger.error('This method is not implemented!')

  #############################################################################
  def getMoreProductionInformations(self, prodid):
    gLogger.error('This method is not implemented!')

  #############################################################################
  def getJobInfo(self, lfn):
    gLogger.error('This method is not implemented!')

  #############################################################################
  def getJobInformation(self, lfn):
    gLogger.error('This method is not implemented!')

  #############################################################################
  def getRunNumber(self, lfn):
    gLogger.error('This method is not implemented!')

  #############################################################################
  def getProductionFiles(self, prod, ftype, gotreplica='ALL'):
    gLogger.error('This method is not implemented!')

  #############################################################################
  def getRunFiles(self, runid):
    gLogger.error('This method is not implemented!')

  #############################################################################
  def updateFileMetaData(self, filename, filesAttr):
    gLogger.error('This method is not implemented!')

  #############################################################################
  def renameFile(self, oldLFN, newLFN):
    gLogger.error('This method is not implemented!')
  #############################################################################
  def getJobInputAndOutputJobFiles(self, jobids):
    gLogger.error('This method is not implemented!')

  #############################################################################
  def insertTag(self, name, tag):
    gLogger.error('This method is not implemented!')

  #############################################################################
  def setFileDataQuality(self, lfns, flag):
    gLogger.error('This method is not implemented!')

  #############################################################################
  def setRunAndProcessingPassDataQuality(self, runNB, procpass, flag):
    gLogger.error('This method is not implemented!')

  #############################################################################
  def setRunDataQuality(self, runNb, flag):
    gLogger.error('This method is not implemented!')

  #############################################################################
  def setProductionDataQuality(self, prod, flag):
    gLogger.error('This method is not implemented!')

  #############################################################################
  def getSimCondIDWhenFileName(self, fileName):
    gLogger.error('This method is not implemented!')

  #############################################################################
  def getFileAncestors(self, lfn, depth, replica):
    gLogger.error('This method is not implemented!')

  #############################################################################
  def getFileDescendents(self, lfn, depth=0, production=0, checkreplica=False):
    gLogger.error('This method is not implemented!')

  #############################################################################
  def checkfile(self, fileName): #file
    gLogger.error('This method is not implemented!')
  #############################################################################
  def checkFileTypeAndVersion(self, type, version): #fileTypeAndFileTypeVersion(self, type, version):
    gLogger.error('This method is not implemented!')

  #############################################################################
  def checkEventType(self, eventTypeId):  #eventType(self, eventTypeId):
    gLogger.error('This method is not implemented!')

  #############################################################################
  def insertJob(self, job):
    gLogger.error('This method is not implemented!')

  #############################################################################
  def insertInputFile(self, jobID, FileId):
    gLogger.error('This method is not implemented!')

  #############################################################################
  def insertOutputFile(self, file):
    gLogger.error('This method is not implemented!')

  #############################################################################
  def updateReplicaRow(self, fileID, replica):
    gLogger.error('This method is not implemented!')

  #############################################################################
  def deleteJob(self, job):
    gLogger.error('This method is not implemented!')

  #############################################################################
  def deleteInputFiles(self, jobid):
    gLogger.error('This method is not implemented!')

  #############################################################################
  def deleteFiles(self, lfns):
    gLogger.error('This method is not implemented!')

  #############################################################################
  def insertSimConditions(self, simdesc, BeamCond, BeamEnergy, Generator, MagneticField, DetectorCond, Luminosity, G4settings):
    gLogger.error('This method is not implemented!')

  #############################################################################
  def getSimConditions(self):
    gLogger.error('This method is not implemented!')

  #############################################################################
  def removeReplica(self, fileName):
    gLogger.error('This method is not implemented!')

  #############################################################################
  def getFileMetadata(self, lfns):
    gLogger.error('This method is not implemented!')

  #############################################################################
  def getFileMetaDataForWeb(self, lfns):
    gLogger.error('This method is not implemented!')

  #############################################################################
  def getProductionFilesForWeb(self, prod, ftypeDict, SortDict, StartItem, Maxitems):
    gLogger.error('This method is not implemented!')

  #############################################################################
  def exists(self, lfns):
    gLogger.error('This method is not implemented!')

  #############################################################################
  def addReplica(self, fileName):
    gLogger.error('This method is not implemented!')

  #############################################################################
  def getRunInformations(self, runnb):
    gLogger.error('This method is not implemented!')

  #############################################################################
  def getRunInformation(self, runnb):
    gLogger.error('This method is not implemented!')


  #############################################################################
  def getFileCreationLog(self, lfn):
    gLogger.error('This method is not implemented!')

  #############################################################################
  def updateEventType(self, evid, desc, primary):
    gLogger.error('This method is not implemented!')

  #############################################################################
  def getProductionSummary(self, cName, cVersion, simdesc, pgroup, production, ftype, evttype):
    gLogger.error('This method is not implemented!')

  #############################################################################
  def getFileHistory(self, lfn):
    gLogger.error('This method is not implemented!')

  #############################################################################
  def getProductionInformationsFromView(self, prodid):
    gLogger.error('This method is not implemented!')

  #############################################################################
  def getProductionNbOfJobs(self, prodid):
    gLogger.error('This method is not implemented!')

  #############################################################################
  def getProductionNbOfEvents(self, prodid):
    gLogger.error('This method is not implemented!')

  #############################################################################
  def getProductionSizeOfFiles(self, prodid):
    gLogger.error('This method is not implemented!')

  #############################################################################
  def getProductionNbOfFiles(self, prodid):
    gLogger.error('This method is not implemented!')

  #############################################################################
  def getProductionInformation(self, prodid):
    gLogger.error('This method is not implemented!')

  #############################################################################
  def getSteps(self, prodid):
    gLogger.error('This method is not implemented!')

  #############################################################################
  def getNbOfJobsBySites(self, prodid):
    gLogger.error('This method is not implemented!')

  #############################################################################
  def getConfigsAndEvtType(self, prodid):
    gLogger.error('This method is not implemented!')

  #############################################################################
  def getAvailableTags(self):
    gLogger.error('This method is not implemented!')

  #############################################################################
  def getProductionProcessedEvents(self, prodid):
    gLogger.error('This method is not implemented!')

  #############################################################################
  def getRunsForAGivenPeriod(self, dict):
    gLogger.error('This method is not implemented!')

  #############################################################################
  def getProductionsFromView(self, dict):
    gLogger.error('This method is not implemented!')

  #############################################################################
  def getRunFilesDataQuality(self, runs):
    gLogger.error('This method is not implemented!')

  #############################################################################
  def setFilesInvisible(self, lfns):
    gLogger.error('This method is not implemented!')

  #############################################################################
  def setFilesVisible(self, lfns):
    gLogger.error('This method is not implemented!')

  #############################################################################
  def getTotalProcessingPass(self, prod):
    gLogger.error('This method is not implemented!')

  #############################################################################
  def getRunAndProcessingPassDataQuality(self, runnb, processing):
    gLogger.error('This method is not implemented!')

  #############################################################################
  def getAvailableConfigurations(self):
    gLogger.error('This method is not implemented!')

  #############################################################################
  def getProductionProcessingPassID(self, prodid):
    gLogger.error('This method is not implemented!')

  #############################################################################
  def getProductionProcessingPass(self, prodid):
    gLogger.error('This method is not implemented!')

  #############################################################################
  def getRunProcessingPass(self, runnumber):
    gLogger.error('This method is not implemented!')

  #############################################################################
  def getProductionFilesStatus(self, productionid , lfns):
    gLogger.error('This method is not implemented!')

  #############################################################################
  def getProductionSimulationCond(self, prod):
    gLogger.error('This method is not implemented!')

  #############################################################################
  def getFiles(self, simdesc, datataking, procPass, ftype, evt, configName, configVersion, production, flag, startDate, endDate, nbofEvents, startRunID, endRunID, runnumbers, replicaFlag, visible, filesize, tcks):
    gLogger.error('This method is not implemented!')

  def getVisibleFilesWithMetadata(self, simdesc, datataking, procPass, ftype, evt, configName, configVersion, production, flag, startDate, endDate, nbofEvents, startRunID, endRunID, runnumbers, replicaFlag, tcks):
    gLogger.error('This method is not implemented!')

  #############################################################################
  def getDataTakingCondId(self, condition):
    gLogger.error('This method is not implemented!')

  #############################################################################
  def getStepIdandName(self, programName, programVersion):
    gLogger.error('This method is not implemented!')

  #############################################################################
  def addProduction(self, production, simcond, daq, steps, inputproc):
    gLogger.error('This method is not implemented!')

  #############################################################################
  def checkProcessingPassAndSimCond(self, production):
    gLogger.error('This method is not implemented!')

  #############################################################################
  def getEventTypes(self, configName, configVersion, production):
    gLogger.error('This method is not implemented!')

  #############################################################################
  def getProcessingPassSteps(self, procpass, cond, stepname):
    gLogger.error('This method is not implemented!')

  #############################################################################
  def getProductionProcessingPassSteps(self, prod):
    gLogger.error('This method is not implemented!')

  #############################################################################
  def getStepIdandNameForRUN(self, programName, programVersion, conddb, dddb):
    gLogger.error('This method is not implemented!')

  #############################################################################
  def getDataTakingCondDesc(self, condition):
    gLogger.error('This method is not implemented!')

  #############################################################################
  def getProductionOutputFileTypes(self, prod):
    gLogger.error('This method is not implemented!')

  #############################################################################
  def existsTag(self, name, value):
    gLogger.error('This method is not implemented!')

  #############################################################################
  def getRunWithProcessingPassAndDataQuality(self, procpass, flag):
    gLogger.error('This method is not implemented!')

  #############################################################################
  def insertDataTakingCond(self, conditions):
    gLogger.error('This method is not implemented!')

  #############################################################################
  def deleteSetpContiner(self, prod):
    gLogger.error('This method is not implemented!')

  #############################################################################
  def getRunNbAndTck(self, lfn):
    gLogger.error('This method is not implemented!')

  #############################################################################
  def deleteProductionsContiner(self, prod):
    gLogger.error('This method is not implemented!')

  #############################################################################
  def getRuns(self, cName, cVersion):
    gLogger.error('This method is not implemented!')

  #############################################################################
  def getRunAndProcessingPass(self, runnb):
    gLogger.error('This method is not implemented!')

  #############################################################################
  def getProcessingPassId(self, fullpath):
    gLogger.error('This method is not implemented!')

  #############################################################################
  def getNbOfRawFiles(self, runid, eventtype):
    gLogger.error('This method is not implemented!')

  #############################################################################
  def getFileTypeVersion(self, lfn):
    gLogger.error('This method is not implemented!')

  #############################################################################
  def insertRuntimeProject(self, projectid, runtimeprojectid):
    gLogger.error('This method is not implemented!')

  #############################################################################
  def getRuntimeProjects(self, dict):
    gLogger.error('This method is not implemented!')

  #############################################################################
  def updateRuntimeProject(self, projectid, runtimeprojectid):
    gLogger.error('This method is not implemented!')

  #############################################################################
  def removeRuntimeProject(self, stepid):
    gLogger.error('This method is not implemented!')

  #############################################################################
  def getTCKs(self, configName, configVersion, conddescription, processing, evt, production, filetype, quality, runnb):
    gLogger.error('This method is not implemented!')

  #############################################################################
  def getStepsMetadata(self, configName, configVersion, cond, procpass, evt, production, filetype, runnb):
    gLogger.error('This method is not implemented!')

  #############################################################################
  def getDirectoryMetadata(self, lfn):
    gLogger.error('This method is not implemented!')

  #############################################################################
  def getFilesForGUID(self, guid):
    gLogger.error('This method is not implemented!')
