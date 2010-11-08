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
  def insertFileTypes(self, ftype, desc):
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
  def getProcessingPass(self, configName, configVersion, conddescription, path):
    gLogger.error('This method is not implemented!')
  
  #############################################################################
  def getProductions(self, configName, configVersion, conddescription, processing, evt):
     gLogger.error('This method is not implemented!')
  
  #############################################################################
  def getFileTypes(self, configName, configVersion, conddescription, processing, evt, production):
    gLogger.error('This method is not implemented!')
  
  #############################################################################
  def getFiles(self, configName, configVersion, conddescription, processing, evt, production, filetype, quality):
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
  def getRunNumber(self, lfn):
    gLogger.error('This method is not implemented!')
  
  #############################################################################
  def getProductionFiles(self, prod, ftype, gotreplica='ALL'):
    gLogger.error('This method is not implemented!')
  
  #############################################################################
  def getFilesForAGivenProduction(self, dict):
    gLogger.error('This method is not implemented!')
  
  #############################################################################
  def getAvailableRunNumbers(self):
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
  def getInputAndOutputJobFiles(self, jobids):
    gLogger.error('This method is not implemented!')
    
  #############################################################################
  def getInputAndOutputJobFiles(self, jobids):
    gLogger.error('This method is not implemented!')
  
  #############################################################################
  def getJobsIds(self, filelist):
    gLogger.error('This method is not implemented!')
    
  #############################################################################
  def insertTag(self, name, tag):
    gLogger.error('This method is not implemented!')
    
  #############################################################################  
  def setQuality(self, lfns, flag):
    gLogger.error('This method is not implemented!')
  
  #############################################################################
  def setRunQualityWithProcessing(self, runNB, procpass, flag):
    gLogger.error('This method is not implemented!')
    
  #############################################################################  
  def setQualityRun(self, runNb, flag):
    gLogger.error('This method is not implemented!')
    
  #############################################################################
  def setQualityProduction(self, prod, flag):
    gLogger.error('This method is not implemented!')
    
  #############################################################################
  def getSimCondIDWhenFileName(self, fileName):
    gLogger.error('This method is not implemented!')
    
  #############################################################################  
  def getLFNsByProduction(self, prodid):
    gLogger.error('This method is not implemented!')
    
  #############################################################################  
  def getAncestors(self, lfn, depth):
    gLogger.error('This method is not implemented!')
  
  #############################################################################  
  def getAllAncestors(self, lfn, depth):
    gLogger.error('This method is not implemented!')
    
  #############################################################################  
  def getAllAncestorsWithFileMetaData(self, lfn, depth):
    gLogger.error('This method is not implemented!')
  
  #############################################################################  
  def getAllDescendents(self, lfn, depth = 0, production=0, checkreplica=False):
    gLogger.error('This method is not implemented!')
  
  #############################################################################  
  def getDescendents(self, lfn, depth = 0):
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
  def insertSimConditions(self, simdesc, BeamCond, BeamEnergy, Generator, MagneticField, DetectorCond, Luminosity):
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
  def getFilesInformations(self,lfns):
    gLogger.error('This method is not implemented!')
    
  #############################################################################
  def getFileMetaDataForUsers(self, lfns):
    gLogger.error('This method is not implemented!')
    
  #############################################################################
  def getProductionFilesForUsers(self, prod, ftypeDict, SortDict, StartItem, Maxitems):
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
  def getLogfile(self, lfn):
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
  def getJobsNb(self, prodid):
    gLogger.error('This method is not implemented!')
  
  #############################################################################
  def getNumberOfEvents(self, prodid):
    gLogger.error('This method is not implemented!')
    
  #############################################################################
  def getSizeOfFiles(self, prodid):
    gLogger.error('This method is not implemented!')
    
  #############################################################################
  def getSizeOfFiles(self, prodid):
    gLogger.error('This method is not implemented!')
    
  #############################################################################
  def getNbOfFiles(self, prodid):
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
  def getProcessedEvents(self, prodid):
    gLogger.error('This method is not implemented!')
    
  #############################################################################
  def getRunsWithAGivenDates(self, dict):
    gLogger.error('This method is not implemented!')
    
  #############################################################################
  def getProductiosWithAGivenRunAndProcessing(self, dict):
    gLogger.error('This method is not implemented!')
  
  #############################################################################
  def getDataQualityForRuns(self, runs):
    gLogger.error('This method is not implemented!')
    
  #############################################################################
  def setFilesInvisible(self, lfns):
    gLogger.error('This method is not implemented!')
  
  #############################################################################
  def getTotalProcessingPass(self, prod):
    gLogger.error('This method is not implemented!')
  
  #############################################################################
  def getRunFlag(self, runnb, processing):
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
  def checkProductionStatus(self, productionid , lfns):
    gLogger.error('This method is not implemented!')
  
  #############################################################################
  def getProductionSimulationCond(self, prod):
    gLogger.error('This method is not implemented!')
  
  #############################################################################
  def getFilesWithGivenDataSets(self, simdesc, datataking, procPass, ftype, evt, configName, configVersion, production, flag, startDate, endDate, nbofEvents, startRunID, endRunID, runnumbers, replicaFlag):
    gLogger.error('This method is not implemented!')
    
  def getFilesWithGivenDataSetsForUsers(self, simdesc, datataking, procPass, ftype, evt, configName, configVersion, production, flag, startDate, endDate, nbofEvents, startRunID, endRunID, runnumbers, replicaFlag):
    gLogger.error('This method is not implemented!')