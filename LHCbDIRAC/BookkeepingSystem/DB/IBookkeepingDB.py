"""Interface for the databases"""
########################################################################
# $Id$
########################################################################

__RCSID__ = "$Id$"

from DIRAC                                      import gLogger, S_ERROR

class IBookkeepingDB(object):
  """This class declare all the methods which are needed to manipulate the database"""
  #############################################################################
  def __init__(self):
    pass

  #############################################################################
  def getAvailableSteps(self, in_dict):
    """It is the interface of the BookkeepingClient: more information is in the BookkeepingClient.py"""
    gLogger.error('This method is not implemented!')
    return S_ERROR('This method is not implemented!')

  #############################################################################
  def getAvailableFileTypes(self):
    """It is the interface of the BookkeepingClient: more information is in the BookkeepingClient.py"""
    gLogger.error('This method is not implemented!')
    return S_ERROR('This method is not implemented!')

  #############################################################################
  def insertFileTypes(self, ftype, desc, fileType):
    """It is the interface of the BookkeepingClient: more information is in the BookkeepingClient.py"""
    gLogger.error('This method is not implemented!')
    return S_ERROR('This method is not implemented!')

  #############################################################################
  def insertStep(self, in_dict):
    """It is the interface of the BookkeepingClient: more information is in the BookkeepingClient.py"""
    gLogger.error('This method is not implemented!')
    return S_ERROR('This method is not implemented!')

  #############################################################################
  def getSimulationConditions(self, configName, configVersion, realdata):
    """It is the interface of the BookkeepingClient: more information is in the BookkeepingClient.py"""
    gLogger.error('This method is not implemented!')
    return S_ERROR('This method is not implemented!')

  def deleteStep(self, stepid):
    """It is the interface of the BookkeepingClient: more information is in the BookkeepingClient.py"""
    gLogger.error('This method is not implemented!')
    return S_ERROR('This method is not implemented!')

  #############################################################################
  def updateStep(self, in_dict):
    """It is the interface of the BookkeepingClient: more information is in the BookkeepingClient.py"""
    gLogger.error('This method is not implemented!')
    return S_ERROR('This method is not implemented!')

  #############################################################################
  def getStepInputFiles(self, StepId):
    """It is the interface of the BookkeepingClient: more information is in the BookkeepingClient.py"""
    gLogger.error('This method is not implemented!')
    return S_ERROR('This method is not implemented!')

  #############################################################################
  def setStepInputFiles(self, stepid, files):
    """It is the interface of the BookkeepingClient: more information is in the BookkeepingClient.py"""
    gLogger.error('This method is not implemented!')
    return S_ERROR('This method is not implemented!')

  #############################################################################
  def setStepOutputFiles(self, stepid, files):
    """It is the interface of the BookkeepingClient: more information is in the BookkeepingClient.py"""
    gLogger.error('This method is not implemented!')
    return S_ERROR('This method is not implemented!')

  #############################################################################
  def getStepOutputFiles(self, StepId):
    """It is the interface of the BookkeepingClient: more information is in the BookkeepingClient.py"""
    gLogger.error('This method is not implemented!')
    return S_ERROR('This method is not implemented!')

  #############################################################################
  def getAvailableConfigNames(self):
    """It is the interface of the BookkeepingClient: more information is in the BookkeepingClient.py"""
    gLogger.error('This method is not implemented!')
    return S_ERROR('This method is not implemented!')

  #############################################################################
  def getConfigVersions(self, configname):
    """It is the interface of the BookkeepingClient: more information is in the BookkeepingClient.py"""
    gLogger.error('This method is not implemented!')
    return S_ERROR('This method is not implemented!')

  #############################################################################
  def getProcessingPass(self, configName, configVersion, conddescription, runnumber, prod, evt, path):
    """It is the interface of the BookkeepingClient: more information is in the BookkeepingClient.py"""
    gLogger.error('This method is not implemented!')
    return S_ERROR('This method is not implemented!')

  #############################################################################
  def getConditions(self, configName, configVersion, evt):
    """It is the interface of the BookkeepingClient: more information is in the BookkeepingClient.py"""
    gLogger.error('This method is not implemented!')
    return S_ERROR('This method is not implemented!')

  #############################################################################
  def getProductions(self, configName, configVersion, conddescription, processing, evt, visible):
    """It is the interface of the BookkeepingClient: more information is in the BookkeepingClient.py"""
    gLogger.error('This method is not implemented!')
    return S_ERROR('This method is not implemented!')

  #############################################################################
  def getFileTypes(self, configName, configVersion, conddescription, processing, evt, runnb, production):
    """It is the interface of the BookkeepingClient: more information is in the BookkeepingClient.py"""
    gLogger.error('This method is not implemented!')
    return S_ERROR('This method is not implemented!')

  #############################################################################
  def getFilesWithMetadata(self, configName, configVersion, conddescription, processing, evt, production, filetype, quality, runnb, visible, replicaflag):
    """It is the interface of the BookkeepingClient: more information is in the BookkeepingClient.py"""
    gLogger.error('This method is not implemented!')
    return S_ERROR('This method is not implemented!')

  #############################################################################
  def getFilesSummary(self, configName, configVersion, conddescription, processing, evt, production, filetype, quality, runnb, startrun, endrun, visible):
    """It is the interface of the BookkeepingClient: more information is in the BookkeepingClient.py"""
    gLogger.error('This method is not implemented!')
    return S_ERROR('This method is not implemented!')

  #############################################################################
  def getLimitedFiles(self, configName, configVersion, conddescription, processing, evt, production, filetype, quality, runnb, startItem, maxItem):
    """It is the interface of the BookkeepingClient: more information is in the BookkeepingClient.py"""
    gLogger.error('This method is not implemented!')
    return S_ERROR('This method is not implemented!')

  #############################################################################
  def getAvailableDataQuality(self):
    """It is the interface of the BookkeepingClient: more information is in the BookkeepingClient.py"""
    gLogger.error('This method is not implemented!')
    return S_ERROR('This method is not implemented!')

  #############################################################################
  def getAvailableProductions(self):
    """It is the interface of the BookkeepingClient: more information is in the BookkeepingClient.py"""
    gLogger.error('This method is not implemented!')
    return S_ERROR('This method is not implemented!')

  #############################################################################
  def getAvailableRuns(self):
    """It is the interface of the BookkeepingClient: more information is in the BookkeepingClient.py"""
    gLogger.error('This method is not implemented!')
    return S_ERROR('This method is not implemented!')

  #############################################################################
  def getAvailableEventTypes(self):
    """It is the interface of the BookkeepingClient: more information is in the BookkeepingClient.py"""
    gLogger.error('This method is not implemented!')
    return S_ERROR('This method is not implemented!')

  #############################################################################
  def getMoreProductionInformations(self, prodid):
    """It is the interface of the BookkeepingClient: more information is in the BookkeepingClient.py"""
    gLogger.error('This method is not implemented!')
    return S_ERROR('This method is not implemented!')

  #############################################################################
  def getJobInfo(self, lfn):
    """It is the interface of the BookkeepingClient: more information is in the BookkeepingClient.py"""
    gLogger.error('This method is not implemented!')
    return S_ERROR('This method is not implemented!')

  #############################################################################
  def getJobInformation(self, lfn):
    """It is the interface of the BookkeepingClient: more information is in the BookkeepingClient.py"""
    gLogger.error('This method is not implemented!')
    return S_ERROR('This method is not implemented!')

  #############################################################################
  def getRunNumber(self, lfn):
    """It is the interface of the BookkeepingClient: more information is in the BookkeepingClient.py"""
    gLogger.error('This method is not implemented!')
    return S_ERROR('This method is not implemented!')

  #############################################################################
  def getProductionFiles(self, prod, ftype, gotreplica='ALL'):
    """It is the interface of the BookkeepingClient: more information is in the BookkeepingClient.py"""
    gLogger.error('This method is not implemented!')
    return S_ERROR('This method is not implemented!')

  #############################################################################
  def getRunFiles(self, runid):
    """It is the interface of the BookkeepingClient: more information is in the BookkeepingClient.py"""
    gLogger.error('This method is not implemented!')
    return S_ERROR('This method is not implemented!')

  #############################################################################
  def updateFileMetaData(self, filename, filesAttr):
    """It is the interface of the BookkeepingClient: more information is in the BookkeepingClient.py"""
    gLogger.error('This method is not implemented!')
    return S_ERROR('This method is not implemented!')

  #############################################################################
  def renameFile(self, oldLFN, newLFN):
    """It is the interface of the BookkeepingClient: more information is in the BookkeepingClient.py"""
    gLogger.error('This method is not implemented!')
    return S_ERROR('This method is not implemented!')

  #############################################################################
  def getJobInputAndOutputJobFiles(self, jobids):
    """It is the interface of the BookkeepingClient: more information is in the BookkeepingClient.py"""
    gLogger.error('This method is not implemented!')
    return S_ERROR('This method is not implemented!')

  #############################################################################
  def insertTag(self, name, tag):
    """It is the interface of the BookkeepingClient: more information is in the BookkeepingClient.py"""
    gLogger.error('This method is not implemented!')
    return S_ERROR('This method is not implemented!')

  #############################################################################
  def setFileDataQuality(self, lfns, flag):
    """It is the interface of the BookkeepingClient: more information is in the BookkeepingClient.py"""
    gLogger.error('This method is not implemented!')
    return S_ERROR('This method is not implemented!')

  #############################################################################
  def setRunAndProcessingPassDataQuality(self, runNB, procpass, flag):
    """It is the interface of the BookkeepingClient: more information is in the BookkeepingClient.py"""
    gLogger.error('This method is not implemented!')
    return S_ERROR('This method is not implemented!')

  #############################################################################
  def setRunDataQuality(self, runNb, flag):
    """It is the interface of the BookkeepingClient: more information is in the BookkeepingClient.py"""
    gLogger.error('This method is not implemented!')
    return S_ERROR('This method is not implemented!')

  #############################################################################
  def setProductionDataQuality(self, prod, flag):
    """It is the interface of the BookkeepingClient: more information is in the BookkeepingClient.py"""
    gLogger.error('This method is not implemented!')
    return S_ERROR('This method is not implemented!')

  #############################################################################
  def getSimCondIDWhenFileName(self, fileName):
    """It is the interface of the BookkeepingClient: more information is in the BookkeepingClient.py"""
    gLogger.error('This method is not implemented!')
    return S_ERROR('This method is not implemented!')

  #############################################################################
  def getFileAncestors(self, lfn, depth, replica):
    """It is the interface of the BookkeepingClient: more information is in the BookkeepingClient.py"""
    gLogger.error('This method is not implemented!')
    return S_ERROR('This method is not implemented!')

  #############################################################################
  def getFileDescendents(self, lfn, depth=0, production=0, checkreplica=False):
    """It is the interface of the BookkeepingClient: more information is in the BookkeepingClient.py"""
    gLogger.error('This method is not implemented!')
    return S_ERROR('This method is not implemented!')

  #############################################################################
  def checkfile(self, fileName): #file
    """It is the interface of the BookkeepingClient: more information is in the BookkeepingClient.py"""
    gLogger.error('This method is not implemented!')
    return S_ERROR('This method is not implemented!')

  #############################################################################
  def checkFileTypeAndVersion(self, type, version): #fileTypeAndFileTypeVersion(self, type, version):
    """It is the interface of the BookkeepingClient: more information is in the BookkeepingClient.py"""
    gLogger.error('This method is not implemented!')
    return S_ERROR('This method is not implemented!')

  #############################################################################
  def checkEventType(self, eventTypeId):  #eventType(self, eventTypeId):
    """It is the interface of the BookkeepingClient: more information is in the BookkeepingClient.py"""
    gLogger.error('This method is not implemented!')
    return S_ERROR('This method is not implemented!')

  #############################################################################
  def insertJob(self, job):
    """It is the interface of the BookkeepingClient: more information is in the BookkeepingClient.py"""
    gLogger.error('This method is not implemented!')
    return S_ERROR('This method is not implemented!')

  #############################################################################
  def insertInputFile(self, jobID, FileId):
    """It is the interface of the BookkeepingClient: more information is in the BookkeepingClient.py"""
    gLogger.error('This method is not implemented!')
    return S_ERROR('This method is not implemented!')

  #############################################################################
  def insertOutputFile(self, file):
    """It is the interface of the BookkeepingClient: more information is in the BookkeepingClient.py"""
    gLogger.error('This method is not implemented!')
    return S_ERROR('This method is not implemented!')

  #############################################################################
  def updateReplicaRow(self, fileID, replica):
    """It is the interface of the BookkeepingClient: more information is in the BookkeepingClient.py"""
    gLogger.error('This method is not implemented!')
    return S_ERROR('This method is not implemented!')

  #############################################################################
  def deleteJob(self, job):
    """It is the interface of the BookkeepingClient: more information is in the BookkeepingClient.py"""
    gLogger.error('This method is not implemented!')
    return S_ERROR('This method is not implemented!')

  #############################################################################
  def deleteInputFiles(self, jobid):
    """It is the interface of the BookkeepingClient: more information is in the BookkeepingClient.py"""
    gLogger.error('This method is not implemented!')
    return S_ERROR('This method is not implemented!')

  #############################################################################
  def deleteFiles(self, lfns):
    """It is the interface of the BookkeepingClient: more information is in the BookkeepingClient.py"""
    gLogger.error('This method is not implemented!')
    return S_ERROR('This method is not implemented!')

  #############################################################################
  def insertSimConditions(self, simdesc, BeamCond, BeamEnergy, Generator, MagneticField, DetectorCond, Luminosity, G4settings):
    """It is the interface of the BookkeepingClient: more information is in the BookkeepingClient.py"""
    gLogger.error('This method is not implemented!')
    return S_ERROR('This method is not implemented!')

  #############################################################################
  def getSimConditions(self):
    """It is the interface of the BookkeepingClient: more information is in the BookkeepingClient.py"""
    gLogger.error('This method is not implemented!')
    return S_ERROR('This method is not implemented!')

  #############################################################################
  def removeReplica(self, fileName):
    """It is the interface of the BookkeepingClient: more information is in the BookkeepingClient.py"""
    gLogger.error('This method is not implemented!')
    return S_ERROR('This method is not implemented!')

  #############################################################################
  def getFileMetadata(self, lfns):
    """It is the interface of the BookkeepingClient: more information is in the BookkeepingClient.py"""
    gLogger.error('This method is not implemented!')
    return S_ERROR('This method is not implemented!')

  #############################################################################
  def getFileMetaDataForWeb(self, lfns):
    """It is the interface of the BookkeepingClient: more information is in the BookkeepingClient.py"""
    gLogger.error('This method is not implemented!')
    return S_ERROR('This method is not implemented!')

  #############################################################################
  def getProductionFilesForWeb(self, prod, ftypeDict, SortDict, StartItem, Maxitems):
    """It is the interface of the BookkeepingClient: more information is in the BookkeepingClient.py"""
    gLogger.error('This method is not implemented!')
    return S_ERROR('This method is not implemented!')

  #############################################################################
  def exists(self, lfns):
    """It is the interface of the BookkeepingClient: more information is in the BookkeepingClient.py"""
    gLogger.error('This method is not implemented!')
    return S_ERROR('This method is not implemented!')

  #############################################################################
  def addReplica(self, fileName):
    """It is the interface of the BookkeepingClient: more information is in the BookkeepingClient.py"""
    gLogger.error('This method is not implemented!')
    return S_ERROR('This method is not implemented!')

  #############################################################################
  def getRunInformations(self, runnb):
    """It is the interface of the BookkeepingClient: more information is in the BookkeepingClient.py"""
    gLogger.error('This method is not implemented!')
    return S_ERROR('This method is not implemented!')

  #############################################################################
  def getRunInformation(self, runnb):
    """It is the interface of the BookkeepingClient: more information is in the BookkeepingClient.py"""
    gLogger.error('This method is not implemented!')
    return S_ERROR('This method is not implemented!')


  #############################################################################
  def getFileCreationLog(self, lfn):
    """It is the interface of the BookkeepingClient: more information is in the BookkeepingClient.py"""
    gLogger.error('This method is not implemented!')
    return S_ERROR('This method is not implemented!')

  #############################################################################
  def updateEventType(self, evid, desc, primary):
    """It is the interface of the BookkeepingClient: more information is in the BookkeepingClient.py"""
    gLogger.error('This method is not implemented!')
    return S_ERROR('This method is not implemented!')

  #############################################################################
  def getProductionSummary(self, cName, cVersion, simdesc, pgroup, production, ftype, evttype):
    """It is the interface of the BookkeepingClient: more information is in the BookkeepingClient.py"""
    gLogger.error('This method is not implemented!')
    return S_ERROR('This method is not implemented!')

  #############################################################################
  def getFileHistory(self, lfn):
    """It is the interface of the BookkeepingClient: more information is in the BookkeepingClient.py"""
    gLogger.error('This method is not implemented!')
    return S_ERROR('This method is not implemented!')

  #############################################################################
  def getProductionInformationsFromView(self, prodid):
    """It is the interface of the BookkeepingClient: more information is in the BookkeepingClient.py"""
    gLogger.error('This method is not implemented!')
    return S_ERROR('This method is not implemented!')

  #############################################################################
  def getProductionNbOfJobs(self, prodid):
    """It is the interface of the BookkeepingClient: more information is in the BookkeepingClient.py"""
    gLogger.error('This method is not implemented!')
    return S_ERROR('This method is not implemented!')

  #############################################################################
  def getProductionNbOfEvents(self, prodid):
    """It is the interface of the BookkeepingClient: more information is in the BookkeepingClient.py"""
    gLogger.error('This method is not implemented!')
    return S_ERROR('This method is not implemented!')

  #############################################################################
  def getProductionSizeOfFiles(self, prodid):
    """It is the interface of the BookkeepingClient: more information is in the BookkeepingClient.py"""
    gLogger.error('This method is not implemented!')
    return S_ERROR('This method is not implemented!')

  #############################################################################
  def getProductionNbOfFiles(self, prodid):
    """It is the interface of the BookkeepingClient: more information is in the BookkeepingClient.py"""
    gLogger.error('This method is not implemented!')
    return S_ERROR('This method is not implemented!')

  #############################################################################
  def getProductionInformation(self, prodid):
    """It is the interface of the BookkeepingClient: more information is in the BookkeepingClient.py"""
    gLogger.error('This method is not implemented!')
    return S_ERROR('This method is not implemented!')

  #############################################################################
  def getSteps(self, prodid):
    """It is the interface of the BookkeepingClient: more information is in the BookkeepingClient.py"""
    gLogger.error('This method is not implemented!')
    return S_ERROR('This method is not implemented!')

  #############################################################################
  def getNbOfJobsBySites(self, prodid):
    """It is the interface of the BookkeepingClient: more information is in the BookkeepingClient.py"""
    gLogger.error('This method is not implemented!')
    return S_ERROR('This method is not implemented!')

  #############################################################################
  def getConfigsAndEvtType(self, prodid):
    """It is the interface of the BookkeepingClient: more information is in the BookkeepingClient.py"""
    gLogger.error('This method is not implemented!')
    return S_ERROR('This method is not implemented!')

  #############################################################################
  def getAvailableTags(self):
    """It is the interface of the BookkeepingClient: more information is in the BookkeepingClient.py"""
    gLogger.error('This method is not implemented!')
    return S_ERROR('This method is not implemented!')

  #############################################################################
  def getProductionProcessedEvents(self, prodid):
    """It is the interface of the BookkeepingClient: more information is in the BookkeepingClient.py"""
    gLogger.error('This method is not implemented!')
    return S_ERROR('This method is not implemented!')

  #############################################################################
  def getRunsForAGivenPeriod(self, in_dict):
    """It is the interface of the BookkeepingClient: more information is in the BookkeepingClient.py"""
    gLogger.error('This method is not implemented!')
    return S_ERROR('This method is not implemented!')

  #############################################################################
  def getProductionsFromView(self, in_dict):
    """It is the interface of the BookkeepingClient: more information is in the BookkeepingClient.py"""
    gLogger.error('This method is not implemented!')
    return S_ERROR('This method is not implemented!')

  #############################################################################
  def getRunFilesDataQuality(self, runs):
    """It is the interface of the BookkeepingClient: more information is in the BookkeepingClient.py"""
    gLogger.error('This method is not implemented!')
    return S_ERROR('This method is not implemented!')

  #############################################################################
  def setFilesInvisible(self, lfns):
    """It is the interface of the BookkeepingClient: more information is in the BookkeepingClient.py"""
    gLogger.error('This method is not implemented!')
    return S_ERROR('This method is not implemented!')

  #############################################################################
  def setFilesVisible(self, lfns):
    """It is the interface of the BookkeepingClient: more information is in the BookkeepingClient.py"""
    gLogger.error('This method is not implemented!')
    return S_ERROR('This method is not implemented!')

  #############################################################################
  def getTotalProcessingPass(self, prod):
    """It is the interface of the BookkeepingClient: more information is in the BookkeepingClient.py"""
    gLogger.error('This method is not implemented!')
    return S_ERROR('This method is not implemented!')

  #############################################################################
  def getRunAndProcessingPassDataQuality(self, runnb, processing):
    """It is the interface of the BookkeepingClient: more information is in the BookkeepingClient.py"""
    gLogger.error('This method is not implemented!')
    return S_ERROR('This method is not implemented!')

  #############################################################################
  def getAvailableConfigurations(self):
    """It is the interface of the BookkeepingClient: more information is in the BookkeepingClient.py"""
    gLogger.error('This method is not implemented!')
    return S_ERROR('This method is not implemented!')

  #############################################################################
  def getProductionProcessingPassID(self, prodid):
    """It is the interface of the BookkeepingClient: more information is in the BookkeepingClient.py"""
    gLogger.error('This method is not implemented!')
    return S_ERROR('This method is not implemented!')

  #############################################################################
  def getProductionProcessingPass(self, prodid):
    """It is the interface of the BookkeepingClient: more information is in the BookkeepingClient.py"""
    gLogger.error('This method is not implemented!')
    return S_ERROR('This method is not implemented!')

  #############################################################################
  def getRunProcessingPass(self, runnumber):
    """It is the interface of the BookkeepingClient: more information is in the BookkeepingClient.py"""
    gLogger.error('This method is not implemented!')
    return S_ERROR('This method is not implemented!')

  #############################################################################
  def getProductionFilesStatus(self, productionid , lfns):
    """It is the interface of the BookkeepingClient: more information is in the BookkeepingClient.py"""
    gLogger.error('This method is not implemented!')
    return S_ERROR('This method is not implemented!')

  #############################################################################
  def getProductionSimulationCond(self, prod):
    """It is the interface of the BookkeepingClient: more information is in the BookkeepingClient.py"""
    gLogger.error('This method is not implemented!')
    return S_ERROR('This method is not implemented!')

  #############################################################################
  def getFiles(self, simdesc, datataking, procPass, ftype, evt, configName, configVersion, production, flag, startDate, endDate, nbofEvents, startRunID, endRunID, runnumbers, replicaFlag, visible, filesize, tcks):
    """It is the interface of the BookkeepingClient: more information is in the BookkeepingClient.py"""
    gLogger.error('This method is not implemented!')
    return S_ERROR('This method is not implemented!')

  #############################################################################
  def getVisibleFilesWithMetadata(self, simdesc, datataking, procPass, ftype, evt, configName, configVersion, production, flag, startDate, endDate, nbofEvents, startRunID, endRunID, runnumbers, replicaFlag, tcks):
    """It is the interface of the BookkeepingClient: more information is in the BookkeepingClient.py"""
    gLogger.error('This method is not implemented!')
    return S_ERROR('This method is not implemented!')

  #############################################################################
  def getDataTakingCondId(self, condition):
    """It is the interface of the BookkeepingClient: more information is in the BookkeepingClient.py"""
    gLogger.error('This method is not implemented!')
    return S_ERROR('This method is not implemented!')

  #############################################################################
  def getStepIdandName(self, programName, programVersion):
    """It is the interface of the BookkeepingClient: more information is in the BookkeepingClient.py"""
    gLogger.error('This method is not implemented!')
    return S_ERROR('This method is not implemented!')

  #############################################################################
  def addProduction(self, production, simcond, daq, steps, inputproc):
    """It is the interface of the BookkeepingClient: more information is in the BookkeepingClient.py"""
    gLogger.error('This method is not implemented!')
    return S_ERROR('This method is not implemented!')

  #############################################################################
  def checkProcessingPassAndSimCond(self, production):
    """It is the interface of the BookkeepingClient: more information is in the BookkeepingClient.py"""
    gLogger.error('This method is not implemented!')
    return S_ERROR('This method is not implemented!')

  #############################################################################
  def getEventTypes(self, configName, configVersion, production):
    """It is the interface of the BookkeepingClient: more information is in the BookkeepingClient.py"""
    gLogger.error('This method is not implemented!')
    return S_ERROR('This method is not implemented!')

  #############################################################################
  def getProcessingPassSteps(self, procpass, cond, stepname):
    """It is the interface of the BookkeepingClient: more information is in the BookkeepingClient.py"""
    gLogger.error('This method is not implemented!')
    return S_ERROR('This method is not implemented!')

  #############################################################################
  def getProductionProcessingPassSteps(self, prod):
    """It is the interface of the BookkeepingClient: more information is in the BookkeepingClient.py"""
    gLogger.error('This method is not implemented!')
    return S_ERROR('This method is not implemented!')

  #############################################################################
  def getStepIdandNameForRUN(self, programName, programVersion, conddb, dddb):
    """It is the interface of the BookkeepingClient: more information is in the BookkeepingClient.py"""
    gLogger.error('This method is not implemented!')
    return S_ERROR('This method is not implemented!')

  #############################################################################
  def getDataTakingCondDesc(self, condition):
    """It is the interface of the BookkeepingClient: more information is in the BookkeepingClient.py"""
    gLogger.error('This method is not implemented!')
    return S_ERROR('This method is not implemented!')

  #############################################################################
  def getProductionOutputFileTypes(self, prod):
    """It is the interface of the BookkeepingClient: more information is in the BookkeepingClient.py"""
    gLogger.error('This method is not implemented!')
    return S_ERROR('This method is not implemented!')

  #############################################################################
  def existsTag(self, name, value):
    """It is the interface of the BookkeepingClient: more information is in the BookkeepingClient.py"""
    gLogger.error('This method is not implemented!')
    return S_ERROR('This method is not implemented!')

  #############################################################################
  def getRunWithProcessingPassAndDataQuality(self, procpass, flag):
    """It is the interface of the BookkeepingClient: more information is in the BookkeepingClient.py"""
    gLogger.error('This method is not implemented!')
    return S_ERROR('This method is not implemented!')

  #############################################################################
  def insertDataTakingCond(self, conditions):
    """It is the interface of the BookkeepingClient: more information is in the BookkeepingClient.py"""
    gLogger.error('This method is not implemented!')
    return S_ERROR('This method is not implemented!')

  #############################################################################
  def deleteSetpContiner(self, prod):
    """It is the interface of the BookkeepingClient: more information is in the BookkeepingClient.py"""
    gLogger.error('This method is not implemented!')
    return S_ERROR('This method is not implemented!')

  #############################################################################
  def getRunNbAndTck(self, lfn):
    """It is the interface of the BookkeepingClient: more information is in the BookkeepingClient.py"""
    gLogger.error('This method is not implemented!')
    return S_ERROR('This method is not implemented!')

  #############################################################################
  def deleteProductionsContiner(self, prod):
    """It is the interface of the BookkeepingClient: more information is in the BookkeepingClient.py"""
    gLogger.error('This method is not implemented!')
    return S_ERROR('This method is not implemented!')

  #############################################################################
  def getRuns(self, cName, cVersion):
    """It is the interface of the BookkeepingClient: more information is in the BookkeepingClient.py"""
    gLogger.error('This method is not implemented!')
    return S_ERROR('This method is not implemented!')

  #############################################################################
  def getRunAndProcessingPass(self, runnb):
    """It is the interface of the BookkeepingClient: more information is in the BookkeepingClient.py"""
    gLogger.error('This method is not implemented!')
    return S_ERROR('This method is not implemented!')

  #############################################################################
  def getProcessingPassId(self, fullpath):
    """It is the interface of the BookkeepingClient: more information is in the BookkeepingClient.py"""
    gLogger.error('This method is not implemented!')
    return S_ERROR('This method is not implemented!')

  #############################################################################
  def getNbOfRawFiles(self, runid, eventtype):
    """It is the interface of the BookkeepingClient: more information is in the BookkeepingClient.py"""
    gLogger.error('This method is not implemented!')
    return S_ERROR('This method is not implemented!')

  #############################################################################
  def getFileTypeVersion(self, lfn):
    """It is the interface of the BookkeepingClient: more information is in the BookkeepingClient.py"""
    gLogger.error('This method is not implemented!')
    return S_ERROR('This method is not implemented!')

  #############################################################################
  def insertRuntimeProject(self, projectid, runtimeprojectid):
    """It is the interface of the BookkeepingClient: more information is in the BookkeepingClient.py"""
    gLogger.error('This method is not implemented!')
    return S_ERROR('This method is not implemented!')

  #############################################################################
  def getRuntimeProjects(self, in_dict):
    """It is the interface of the BookkeepingClient: more information is in the BookkeepingClient.py"""
    gLogger.error('This method is not implemented!')
    return S_ERROR('This method is not implemented!')

  #############################################################################
  def updateRuntimeProject(self, projectid, runtimeprojectid):
    """It is the interface of the BookkeepingClient: more information is in the BookkeepingClient.py"""
    gLogger.error('This method is not implemented!')
    return S_ERROR('This method is not implemented!')

  #############################################################################
  def removeRuntimeProject(self, stepid):
    """It is the interface of the BookkeepingClient: more information is in the BookkeepingClient.py"""
    gLogger.error('This method is not implemented!')
    return S_ERROR('This method is not implemented!')

  #############################################################################
  def getTCKs(self, configName, configVersion, conddescription, processing, evt, production, filetype, quality, runnb):
    """It is the interface of the BookkeepingClient: more information is in the BookkeepingClient.py"""
    gLogger.error('This method is not implemented!')
    return S_ERROR('This method is not implemented!')

  #############################################################################
  def getStepsMetadata(self, configName, configVersion, cond, procpass, evt, production, filetype, runnb):
    """It is the interface of the BookkeepingClient: more information is in the BookkeepingClient.py"""
    gLogger.error('This method is not implemented!')
    return S_ERROR('This method is not implemented!')

  #############################################################################
  def getDirectoryMetadata(self, lfn):
    """It is the interface of the BookkeepingClient: more information is in the BookkeepingClient.py"""
    gLogger.error('This method is not implemented!')
    return S_ERROR('This method is not implemented!')

  #############################################################################
  def getFilesForGUID(self, guid):
    """It is the interface of the BookkeepingClient: more information is in the BookkeepingClient.py"""
    gLogger.error('This method is not implemented!')
    return S_ERROR('This method is not implemented!')

  def getRunsGroupedByDataTaking(self):
    """It is the interface of the BookkeepingClient: more information is in the BookkeepingClient.py"""
    gLogger.error('This method is not implemented!')
    return S_ERROR('This method is not implemented!')

