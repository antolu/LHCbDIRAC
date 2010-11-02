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
    
  #############################################################################  
  def getDescendents(self, lfn, depth = 0):
    
  
  #############################################################################
  def checkfile(self, fileName): #file

  #############################################################################
  def checkFileTypeAndVersion(self, type, version): #fileTypeAndFileTypeVersion(self, type, version):
  
  #############################################################################
  def checkEventType(self, eventTypeId):  #eventType(self, eventTypeId):
    
  
  #############################################################################
  def insertJob(self, job):
  
  #############################################################################
  def insertInputFile(self, jobID, FileId):
  
  #############################################################################
  def insertOutputFile(self, file):
    
  #############################################################################
  def updateReplicaRow(self, fileID, replica):
  
  #############################################################################
  def deleteJob(self, job):
  
  #############################################################################
  def deleteInputFiles(self, jobid):
  
  #############################################################################
  def deleteFiles(self, lfns):
  
  #############################################################################
  def insertSimConditions(self, simdesc, BeamCond, BeamEnergy, Generator, MagneticField, DetectorCond, Luminosity):
    
  
  #############################################################################
  def getSimConditions(self):
  
  
  #############################################################################
  def removeReplica(self, fileName):
  
  #############################################################################
  def getFileMetadata(self, lfns):
  
  #############################################################################
  def getFilesInformations(self,lfns):
    
  
  #############################################################################
  def getFileMetaDataForUsers(self, lfns):