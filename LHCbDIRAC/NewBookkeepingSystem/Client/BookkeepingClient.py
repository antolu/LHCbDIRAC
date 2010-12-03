########################################################################
# $Id$
########################################################################

"""
dict = {'EventTypeId': 93000000, 
        'ConfigVersion': 'Collision10', 
        'ProcessingPass': '/Real Data', 
        'ConfigName': 'LHCb', 
        'ConditionDescription': 'Beam3500GeV-VeloClosed-MagDown',
        'Production':7421
         }
"""
import DIRAC
from DIRAC                           import S_OK, S_ERROR, gLogger, gConfig
from DIRAC.Core.DISET.RPCClient      import RPCClient
from DIRAC.Core.DISET.TransferClient import TransferClient
from DIRAC.Core.Utilities            import DEncode
from DIRAC.Core.Base                 import Script
import types,cPickle,os, tempfile

Script.parseCommandLine( ignoreErrors = True )

__RCSID__ = "$Id$"

class BookkeepingClient:

  def __init__( self, rpcClient = None ):
    self.rpcClient = rpcClient

  def __getServer(self,timeout=600):
    if self.rpcClient:
      return self.rpcClient
    else:
      return RPCClient('Bookkeeping/NewBookkeepingManager', timeout=timeout)

  #############################################################################
  def echo(self,string):
    server = self.__getServer()
    res = server.echo(string)
    print res

  #############################################################################
  def sendBookkeeping(self, name, data):
      """
      Send XML file to BookkeepingManager.
      name- XML file name
      data - XML file
      """
      server = self.__getServer()
      result = server.sendBookkeeping(name, data)
      return result
  
  #############################################################################
  def getAvailableSteps(self, dict = {}):
    server = self.__getServer()
    return server.getAvailableSteps(dict)
  
  #############################################################################
  def getAvailableFileTypes(self):
    server = self.__getServer()
    retVal = server.getAvailableFileTypes()
    if retVal['OK']:
      records = []
      parameters = ["FileType","Description"]
      for record in retVal['Value']:
        records += [[record[0],record[1]]]
      return S_OK({'ParameterNames':parameters,'Records':records,'TotalRecords':len(records)})
    return retVal
  
  #############################################################################
  def insertFileTypes(self, ftype, desc):
    server = self.__getServer()
    return server.insertFileTypes(ftype, desc)
  
  #############################################################################
  def insertStep(self, dict):
    server = self.__getServer()
    return server.insertStep(dict)
  
  #############################################################################
  def deleteStep(self, stepid):
    server = self.__getServer()
    return server.deleteStep(int(stepid))
  
  #############################################################################
  def updateStep(self, dict):
    server = self.__getServer()
    return server.updateStep(dict)
  
  #############################################################################
  def getStepInputFiles(self, StepId):
    server = self.__getServer()
    return server.getStepInputFiles(int(StepId))
  
  #############################################################################
  def setStepInputFiles(self, stepid, files):
    server = self.__getServer()
    return server.setStepInputFiles(stepid, files)
  
  #############################################################################
  def setStepOutputFiles(self, stepid, files):
    server = self.__getServer()
    return server.setStepOutputFiles(stepid, files)
  
  #############################################################################
  def getStepOutputFiles(self, StepId):
    server = self.__getServer()
    return server.getStepOutputFiles(int(StepId))
  
  #############################################################################
  def getAvailableConfigNames(self):
    server = self.__getServer()
    return server.getAvailableConfigNames()
  
  #############################################################################
  def getConfigVersions(self, dict):
    server = self.__getServer()
    return server.getConfigVersions(dict)
  
  #############################################################################
  def getConditions(self, dict):
    server = self.__getServer()
    return server.getConditions(dict)
  
  #############################################################################
  def getProcessingPass(self, dict, path = '/'):
    server = self.__getServer()
    return server.getProcessingPass(dict, path)
  
  #############################################################################
  def getStandardProcessingPass(self, dict, path = '/'):
    server = self.__getServer()
    return server.getStandardProcessingPass(dict, path)
  
  #############################################################################
  def getProductions(self, dict):
    server = self.__getServer()
    return server.getProductions(dict)
  
  #############################################################################
  def getFileTypes(self, dict):
    server = self.__getServer()
    return server.getFileTypes(dict)
  
  #############################################################################
  def getFiles(self, dict):
    bkk = TransferClient('Bookkeeping/BookkeepingManager')
    s = cPickle.dumps(dict)
    file = tempfile.NamedTemporaryFile()
    params = str(s)
    result = bkk.receiveFile(file.name, params)
    if not result['OK']:
      return result
    else:
      value = cPickle.load(open(file.name))
      file.close()
      return S_OK(value)
    return S_ERROR()
  
  #############################################################################
  def getFilesSumary(self, dict):
    server = self.__getServer()
    return server.getFilesSumary(dict)
  
  #############################################################################
  def getLimitedFiles(self, dict):
    server = self.__getServer()
    return server.getLimitedFiles(dict)
  
  #############################################################################  
  def getAvailableDataQuality(self):
    server = self.__getServer()
    return server.getAvailableDataQuality()
  
  #############################################################################
  def getAvailableProductions(self):
     server = self.__getServer()
     result = server.getAvailableProductions()
     return result
  
  #############################################################################
  def getAvailableRuns(self):
    server = self.__getServer()
    return server.getAvailableRuns()
  
  #############################################################################
  def getAvailableEventTypes(self):
    server = self.__getServer()
    result = server.getAvailableEventTypes()
    return result
  
  #############################################################################
  def getMoreProductionInformations(self, prodid):
    server = self.__getServer()
    result = server.getMoreProductionInformations(prodid)
    return result
  
  #############################################################################
  def getJobInfo(self, lfn):
    server = self.__getServer()
    result = server.getJobInfo(lfn)
    return result

  #############################################################################
  def getRunNumber(self, lfn):
    server = self.__getServer()
    result = server.getRunNumber(lfn)
    return result
  
  #############################################################################
  def getProductionFiles(self, prod, fileType, replica='ALL'):
    server = self.__getServer()
    result = server.getProductionFiles(int(prod), fileType, replica)
    return result
  
  #############################################################################
  def getFilesForAGivenProduction(self, dict):
    server = self.__getServer()
    result = server.getFilesForAGivenProduction(dict)
    return result
  
  #############################################################################
  def getAvailableRunNumbers(self):
    server = self.__getServer()
    return server.getAvailableRunNumbers()
  
  #############################################################################
  def getRunFiles(self, runid):
    server = self.__getServer()
    result = server.getRunFiles(runid)
    return result

  #############################################################################
  def updateFileMetaData(self, filename, filesAttr):
    server = self.__getServer()
    return server.updateFileMetaData( filename, filesAttr)

  #############################################################################
  def renameFile(self, oldLFN, newLFN):
    server = self.__getServer()
    return server.renameFile(oldLFN, newLFN)

  #############################################################################
  def getInputAndOutputJobFiles(self, jobids):
    server = self.__getServer()
    return server.getInputAndOutputJobFiles(jobids)
  
  #############################################################################
  def getJobsIds(self, filelist):
    server = self.__getServer()
    return server.getJobsIds(filelist)

  #############################################################################
  def insertTag(self, values):
    server = self.__getServer()
    result = server.insertTag(values)
    return result

  #############################################################################
  def setQuality(self, lfns, flag):
    if type(lfns) == types.StringType:
      lfns = [lfns]
    server = self.__getServer()
    result = server.setQuality(lfns, flag)
    return result

  #############################################################################
  def setRunQualityWithProcessing(self, runNB, procpass, flag):
    server = self.__getServer()
    result = server.setRunQualityWithProcessing(long(runNB), procpass, flag)
    return result
  
  #############################################################################
  def setQualityRun(self, runNb, flag):
    server = self.__getServer()
    result = server.setQualityRun(runNb, flag)
    return result
  
  #############################################################################
  def setQualityProduction(self, prod, flag):
    server = self.__getServer()
    result = server.setQualityProduction(prod, flag)
    return result
  

  #############################################################################
  def getLFNsByProduction(self, prodid):
    server = self.__getServer()
    return server.getLFNsByProduction(long(prodid))
  
  #############################################################################
  def getAncestors(self, lfns, depth=1):
    server = self.__getServer()
    result = None
    if type(lfns) == types.StringType:
      result = server.getAncestors([lfns], depth)
    else:
      result = server.getAncestors(lfns, depth)
    return result

  #############################################################################
  def getAllAncestors(self, lfns, depth=1):
    server = self.__getServer()
    result = None
    if type(lfns) == types.StringType:
      result = server.getAllAncestors([lfns], depth)
    else:
      result = server.getAllAncestors(lfns, depth)
    return result
  
  #############################################################################
  def getAllAncestorsWithFileMetaData(self, lfns, depth=1):
    server = self.__getServer()
    result = None
    if type(lfns) == types.StringType:
      result = server.getAllAncestorsWithFileMetaData([lfns], depth)
    else:
      result = server.getAllAncestorsWithFileMetaData(lfns, depth)
    return result
  
  #############################################################################
  def getAllDescendents(self, lfns, depth = 0, production=0, checkreplica=False):
    server = self.__getServer()
    result = None
    if type(lfns) == types.StringType:
      result = server.getAllDescendents([lfns], depth, production, checkreplica)
    else:
      result = server.getAllDescendents(lfns, depth, production, checkreplica)
    return result
  
  #############################################################################
  def getDescendents(self, lfns, depth=0):
    server = self.__getServer()
    result = None
    if type(lfns) == types.StringType:
      result = server.getDescendents([lfns], depth)
    else:
      result = server.getDescendents(lfns, depth)
    return result
  
  #############################################################################
  def checkfile(self, fileName):
     server = self.__getServer()
     result = server.checkfile(fileName)
     return result

   #############################################################################
  def checkFileTypeAndVersion(self, type, version):
     server = self.__getServer()
     result = server.checkFileTypeAndVersion(type, version)
     return result

  #############################################################################
  def checkEventType(self, eventTypeId):
     server = self.__getServer()
     result = server.checkEventType(lonhg(eventTypeId))
     return result

  #############################################################################
  def insertJob(self, job):
     server = self.__getServer()
     result = server.insertJob(job)
     return result
  
  #############################################################################
  def insertInputFile(self, jobID, FileId):
     server = self.__getServer()
     result = server.insertInputFile(long(jobID), long(FileId))
     return result
  
    #############################################################################
  def insertOutputFile(self, file):
     server = self.__getServer()
     result = server.insertOutputFile(self, file)
     return result

  #############################################################################
  def updateReplicaRow(self, fileID, replica):
     server = self.__getServer()
     result = server.updateReplicaRow(long(fileID), replica)
     return result

  #############################################################################
  def deleteJob(self, job):
    server = self.__getServer()
    result = server.deleteJob(long(job))
    return result
  
  #############################################################################
  def deleteInputFiles(self, jobid):
     server = self.__getServer()
     result = server.deleteInputFiles(long(jobid))
     return result
  
  #############################################################################
  def deleteFiles(self, lfns):
    server = self.__getServer()
    result = server.deleteFiles(lfns)
    return result
  
  #############################################################################
  def insertSimConditions(self, simdesc, BeamCond, BeamEnergy, Generator, MagneticField, DetectorCond, Luminosity):
    server = self.__getServer()
    result = server.insertSimConditions(simdesc, BeamCond, BeamEnergy, Generator, MagneticField, DetectorCond, Luminosity)
    return result
  
  #############################################################################
  def getSimConditions(self):
    server = self.__getServer()
    result = server.getSimConditions()
    return result
  
  #############################################################################
  def removeReplica(self, fileName):
    server = self.__getServer()
    return server.removeReplica(fileName)
  
  #############################################################################
  def getFileMetadata(self, lfns):
    server = self.__getServer()
    return server.getFileMetadata(lfns)

  #############################################################################
  def getFilesInformations(self,lfns):
    server = self.__getServer()
    return server.getFilesInformations(lfns)
  
  #############################################################################
  def getFileMetaDataForUsers(self, lfns):
    server = self.__getServer()
    result = server.getFileMetaDataForUsers(lfns)
    return result
  
  #############################################################################
  def getProductionFilesForUsers(self, prod, ftype, SortDict, StartItem, Maxitems):
    server = self.__getServer()
    result = server.getProductionFilesForUsers(int(prod), ftype, SortDict, long(StartItem), long(Maxitems))
    return result
  
  #############################################################################
  def exists(self, lfns):
    server = self.__getServer()
    return server.exists(lfns)
  
  #############################################################################
  def addReplica(self, fileName):
    server = self.__getServer()
    return server.addReplica(fileName)
  
  #############################################################################
  def removeReplica(self, fileName):
    server = self.__getServer()
    return server.removeReplica(fileName)

  #############################################################################
  def getRunInformations(self, runnb):
    server = self.__getServer()
    result = server.getRunInformations(runnb)
    return result
  
  #############################################################################
  def getLogfile(self, lfn):
    server = self.__getServer()
    result = server.getLogfile(lfn)
    return result

  #############################################################################
  def addEventType(self, evid, desc, primary):
    server = self.__getServer()
    return server.addEventType(long(evid), desc, primary)

   #############################################################################
  def updateEventType(self, evid, desc, primary):
    server = self.__getServer()
    return server.updateEventType(long(evid), desc, primary)
  
  #############################################################################
  def addFiles(self, lfns):
    server = self.__getServer()
    return server.addFiles(lfns)

  #############################################################################
  def removeFiles(self, lfns):
    server = self.__getServer()
    return server.removeFiles(lfns)

  #############################################################################
  def getProductionSummary(self, dict):
    server = self.__getServer()
    result = server.getProductionSummary(dict)
    return result

  #############################################################################
  def getProductionInformations_new(self, prodid):
    server = self.__getServer()
    result = server.getProductionInformations_new(long(prodid))
    return result 
  
  #############################################################################
  def getProductionInformationsFromView(self, prodid):
    server = self.__getServer()
    return server.getProductionInformationsFromView(long(prodid))
  
  #############################################################################
  def getFileHistory(self, lfn):
    server = self.__getServer()
    return server.getFileHistory(lfn)
  
  #############################################################################
  def getJobsNb(self, prodid):
    server = self.__getServer()
    return server.getJobsNb(long(prodid))
  
  #############################################################################
  def getNumberOfEvents(self, prodid):
    server = self.__getServer()
    return server.getNumberOfEvents(long(prodid))
  
  #############################################################################
  def getSizeOfFiles(self, prodid):
    server = self.__getServer()
    return server.getSizeOfFiles(long(prodid))

  #############################################################################
  def getNbOfFiles(self, prodid):
    server = self.__getServer()
    return server.getNbOfFiles(long(prodid))

  #############################################################################
  def getProductionInformation(self, prodid):
    server = self.__getServer()
    return server.getProductionInformation(long(prodid))
  
  #############################################################################
  def getNbOfJobsBySites(self, prodid):
    server = self.__getServer()
    return server.getNbOfJobsBySites(long(prodid))

  #############################################################################
  def getAvailableTags(self):
    server = self.__getServer()
    return server.getAvailableTags()
  
  #############################################################################
  def getProcessedEvents(self, prodid):
    server = self.__getServer()
    return server.getProcessedEvents(int(prodid))
  
  #############################################################################
  def getRunsWithAGivenDates(self, dict):
    server = self.__getServer()
    return server.getRunsWithAGivenDates(dict)
  
  #############################################################################
  def getProductiosWithAGivenRunAndProcessing(self, dict):
    server = self.__getServer()
    return server.getProductiosWithAGivenRunAndProcessing(dict)
  
  #############################################################################
  def getDataQualityForRuns(self, runs):
    server = self.__getServer()
    return server.getDataQualityForRuns(runs)
  
  #############################################################################
  def setFilesInvisible(self, lfns):
    server = self.__getServer()
    result = None
    if type(lfns) == types.StringType:
      result = server.setFilesInvisible([lfns])
    else:
      result = server.setFilesInvisible(lfns)
    return result
  
  #############################################################################
  def getRunFlag(self, runnb, processing):
    server = self.__getServer()
    result = server.getRunFlag(long(runnb), long(processing))
    return result
  
  #############################################################################
  def getAvailableConfigurations(self):
    server = self.__getServer()
    result = server.getAvailableConfigurations()
    return result
  
  #######################################################################################################################################################################################################################################
  def getMoreProductionInformations(self, prodid):
    server = self.__getServer()
    result = server.getMoreProductionInformations(int(prodid))
    return result
  
  #############################################################################
  def getProductionProcessingPassID(self, prodid):
    server = self.__getServer()
    return server.getProductionProcessingPassID(long(prodid))
  
  #############################################################################
  def getProductionProcessingPass(self, prodid):
    server = self.__getServer()
    return server.getProductionProcessingPass(long(prodid))
  
  #############################################################################
  def getRunProcessingPass(self, runnumber):
    server = self.__getServer()
    return server.getRunProcessingPass(long(runnumber))
  
  ############################################################################
  def getProductionStatus(self, productionid = None, lfns = []):
    server = self.__getServer()
    result = None
    if productionid != None:
      result = server.checkProductionReplicas(productionid)
    else:
      result = server.checkLfns(lfns)
    return result
  
  #############################################################################
  def getFilesWithGivenDataSets(self, values):
    server = self.__getServer()
    return server.getFilesWithGivenDataSets(values)
  
  #############################################################################
  def getFilesWithGivenDataSetsForUsers(self, values):
    server = self.__getServer()
    return server.getFilesWithGivenDataSetsForUsers(values)
  
  #############################################################################
  def addProduction(self, dict):
    server = self.__getServer()
    return server.addProduction(dict)

  #############################################################################
  def getEventTypes(self, dict):
    server = self.__getServer()
    return server.getEventTypes(dict)
  
  #############################################################################
  def getStandardEventTypes(self, dict):
    server = self.__getServer()
    return server.getStandardEventTypes(dict)
  
  #############################################################################
  def getProcessingPassSteps(self, dict):
    server = self.__getServer()
    return server.getProcessingPassSteps(dict)
  
  #############################################################################
  def getProductionProcessingPassSteps(self, dict):
    server = self.__getServer()
    return server.getProductionProcessingPassSteps(dict)
  
  #############################################################################
  def getProductionOutputFiles(self, dict):
    server = self.__getServer()
    return server.getProductionOutputFiles(dict)