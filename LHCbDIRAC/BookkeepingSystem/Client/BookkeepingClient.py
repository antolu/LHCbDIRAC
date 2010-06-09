########################################################################
# $Id$
########################################################################

"""

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
      return RPCClient('Bookkeeping/BookkeepingManager', timeout=timeout)

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
  def filetransfer(self, name, data):
    server = self.__getServer()
    result = server.filetransfer(name, data)

  #############################################################################
  def deleteJob(self, job):
    server = self.__getServer()
    result = server.deleteJob(long(job))
    return result

  #############################################################################
  def deleteFiles(self, lfns):
    server = self.__getServer()
    result = server.deleteFiles(lfns)
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
  def insertTag(self, values):
    server = self.__getServer()
    result = server.insertTag(values)
    return result

  #############################################################################
  def insertOutputFile(self, file):
     server = self.__getServer()
     result = server.insertOutputFile(self, file)
     return result

  #############################################################################
  def deleteInputFiles(self, jobid):
     server = self.__getServer()
     result = server.deleteInputFiles(long(jobid))
     return result

  #############################################################################
  def getSimulationCondID(self, BeamCond, BeamEnergy, Generator, MagneticField, DetectorCond, Luminosity):
    server = self.__getServer()
    result = server.getSimulationCondID(BeamCond, BeamEnergy, Generator, MagneticField, DetectorCond, Luminosity)
    return result
  
  #############################################################################
  def getRunInformations(self, runnb):
    server = self.__getServer()
    result = server.getRunInformations(runnb)
    return result
  
  #############################################################################
  def getAvailableRuns(self):
    server = self.__getServer()
    result = server.getAvailableRuns()
    return result
  
  #############################################################################
  def getProductionStatus(self, productionid = None, lfns = []):
    server = self.__getServer()
    result = None
    if productionid != None:
      result = server.checkProductionReplicas(productionid)
    else:
      result = server.checkLfns(lfns)
    return result
  
  #############################################################################
  def getAvailableProductions(self):
     server = self.__getServer()
     result = server.getAvailableProductions()
     return result
  
  def getMoreProductionInformations(self, prodid):
    server = self.__getServer()
    result = server.getMoreProductionInformations(prodid)
    return result
   
  #############################################################################
  def insertSimConditions(self, simdesc, BeamCond, BeamEnergy, Generator, MagneticField, DetectorCond, Luminosity):
    server = self.__getServer()
    result = server.insertSimConditions(simdesc, BeamCond, BeamEnergy, Generator, MagneticField, DetectorCond, Luminosity)
    return result

  #############################################################################
  def getSimCondIDWhenFileName(self, fileName):
    server = self.__getServer()
    result = server.getSimCondIDWhenFileName(fileName)
    return result

  #############################################################################
  def updateReplicaRow(self, fileID, replica):
     server = self.__getServer()
     result = server.updateReplicaRow(long(fileID), replica)
     return result

  #############################################################################
  def getAvailableConfigurations(self):
    server = self.__getServer()
    result = server.getAvailableConfigurations()
    return result
  
  #############################################################################
  def setQuality(self, lfns, flag):
    if type(lfns) == types.StringType:
      lfns = [lfns]
    server = self.__getServer()
    result = server.setQuality(lfns, flag)
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
  def getAvailableDataQuality(self):
    server = self.__getServer()
    result = server.getAvailableDataQuality()
    return result
  
  #############################################################################
  def getSimulationConditions(self, configName, configVersion, realdata = 0):
    server = self.__getServer()
    result = server.getSimulationConditions(configName, configVersion, realdata)
    return result

  #############################################################################
  def getProPassWithSimCond(self, configName, configVersion, simcondid):
    server = self.__getServer()
    result = server.getProPassWithSimCond(configName, configVersion, simcondid)
    return result

  #############################################################################
  def getProPassWithSimCondNew(self, configName, configVersion, simcondid):
    server = self.__getServer()
    result = server.getProPassWithSimCondNew(configName, configVersion, simcondid)
    return result

  #############################################################################
  def getEventTypeWithSimcond(self,configName, configVersion, simcondid, procPass):
    server = self.__getServer()
    result = server.getEventTypeWithSimcond(configName, configVersion, simcondid, procPass)
    return result

  #############################################################################
  def getProductionsWithSimcond(self, configName, configVersion, simcondid, procPass, evtId):
    server = self.__getServer()
    result = server.getProductionsWithSimcond(configName, configVersion, simcondid, procPass, evtId)
    return result

  #############################################################################
  def getFileTypesWithSimcond(self, configName, configVersion, simcondid, procPass, evtId, prod):
    server = self.__getServer()
    result = server.getFileTypesWithSimcond(configName, configVersion, simcondid, procPass, evtId, prod)
    return result

  #############################################################################
  def getProgramNameWithSimcond(self, configName, configVersion, simcondid, procPass, evtId, prod, ftype):
    server = self.__getServer()
    result = server.getProgramNameWithSimcond(configName, configVersion, simcondid, procPass, evtId, prod, ftype)
    return result
  
  #############################################################################
  def getProductionFiles(self, prod, fileType, replica='ALL'):
    server = self.__getServer()
    result = server.getProductionFiles(int(prod), fileType, replica)
    return result
  
  #############################################################################
  def getProductionFilesWithAGivenDate(self, dataSet):
    server = self.__getServer()
    result = server.getProductionFilesWithAGivenDate(dataSet)
    return result
  
  #############################################################################
  def getProcessingPassDesc(self, totalproc, passid, simid='ALL'):
    server = self.__getServer()
    result = server.getProcessingPassDesc(totalproc, int(passid), simid)
    return result
  
  #############################################################################
  def getProcessingPassDesc_new(self, totalproc, simid='ALL'):
    server = self.__getServer()
    result = server.getProcessingPassDesc_new(totalproc, simid)
    return result

  #############################################################################
  def getFilesWithSimcondAndDataQuality(self, configName, configVersion, simcondid, procPass, evtId, prod, ftype, progName, progVersion, quality = []):
    qualities = ''
    for i in quality:
      qualities +=';'+i
    bkk = TransferClient('Bookkeeping/BookkeepingManager')
    s = ''+configName+'>'+configVersion+'>'+str(simcondid)+'>'+str(procPass)+'>'+str(evtId)+'>'+str(prod)+'>'+str(ftype)+'>'+str(progName)+'>'+str(progVersion)+'>'+qualities
    print s
    file = tempfile.NamedTemporaryFile()
    result = bkk.receiveFile(file.name, s)
    if not result['OK']:
      return result
    else:
      value = cPickle.load(open(file.name))
      file.close()
      return S_OK(value)
    return S_ERROR()

    
  #############################################################################
  def getFilesWithSimcond(self, configName, configVersion, simcondid, procPass, evtId, prod, ftype, progName, progVersion):
    '''
    server = self.__getServer()
    result = server.getFilesWithSimcond(configName, configVersion, simcondid, procPass, evtId, prod, ftype, progName, progVersion)
    return result

    '''
    bkk = TransferClient('Bookkeeping/BookkeepingManager')
    s = ''+configName+'>'+configVersion+'>'+str(simcondid)+'>'+str(procPass)+'>'+str(evtId)+'>'+str(prod)+'>'+str(ftype)+'>'+str(progName)+'>'+str(progVersion)
    file = tempfile.NamedTemporaryFile()
    result = bkk.receiveFile(file.name, s)
    if not result['OK']:
      return result
    else:
      value = cPickle.load(open(file.name))
      file.close()
      return S_OK(value)
    return S_ERROR()

  #############################################################################
  def getSimConditions(self):
    server = self.__getServer()
    result = server.getSimConditions()
    return result
  
  #############################################################################
  def getProductionSummary(self, dict):
    server = self.__getServer()
    result = server.getProductionSummary(dict)
    return result

                           
  #############################################################################
  def getLimitedFilesWithSimcond(self, configName, configVersion, simcondid, procPass, evtId, prod, ftype, progName, progVersion, startitem, maxitems):
    server = self.__getServer()
    result = server.getLimitedFilesWithSimcond(configName, configVersion, simcondid, procPass, evtId, prod, ftype, progName, progVersion, long(startitem), long(maxitems))
    return result
  
  #############################################################################
  def getLimitedNbOfFiles(self,configName, configVersion, simcondid, procPass, evtId, prod, ftype, progName, progVersion):
    server = self.__getServer()
    result = server.getLimitedNbOfFiles(configName, configVersion, simcondid, procPass, evtId, prod, ftype, progName, progVersion)
    return result
  
  #############################################################################
  def getAvailableFileTypes(self):
    server = self.__getServer()
    result = server.getAvailableFileTypes()
    return result
  
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
  def getSimCondWithEventType(self, configName, configVersion, eventType, realdata=0):
    server = self.__getServer()
    result = server.getSimCondWithEventType(configName, configVersion, eventType, realdata)
    return result

  #############################################################################
  def getProPassWithEventType(self, configName, configVersion, eventType, simcond):
    server = self.__getServer()
    result = server.getProPassWithEventType(configName, configVersion, eventType, simcond)
    return result

  #############################################################################
  def getProPassWithEventTypeNew(self, configName, configVersion, eventType, simcond):
    server = self.__getServer()
    result = server.getProPassWithEventTypeNew(configName, configVersion, eventType, simcond)
    return result

  #############################################################################
  def getJobInfo(self, lfn):
    server = self.__getServer()
    result = server.getJobInfo(lfn)
    return result

  #############################################################################
  def updateFileMetaData(self, filename, filesAttr):
    server = self.__getServer()
    result = server.updateFileMetaData(filename, filesAttr)
    return result

  
  #############################################################################
  def getAvailableConfigNames(self):
    server = self.__getServer()
    result = server.getAvailableConfigNames()
    return result
  
  #############################################################################
  def getConfigVersions(self, configname):
    server = self.__getServer()
    result = server.getConfigVersions(configname)
    return result
  
  

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
  def getAllAncestorsWithFileMetaData(self, lfns, depth=1):
    server = self.__getServer()
    result = None
    if type(lfns) == types.StringType:
      result = server.getAllAncestorsWithFileMetaData([lfns], depth)
    else:
      result = server.getAllAncestorsWithFileMetaData(lfns, depth)
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
  def getDescendents(self, lfns, depth=0):
    server = self.__getServer()
    result = None
    if type(lfns) == types.StringType:
      result = server.getDescendents([lfns], depth)
    else:
      result = server.getDescendents(lfns, depth)
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
  def getAvailableEventTypes(self):
    server = self.__getServer()
    result = server.getAvailableEventTypes()
    return result

  #############################################################################
  def getEventTypes(self, configName, configVersion):
    server = self.__getServer()
    result = server.getEventTypes(configName, configVersion)
    return result

  #############################################################################
  def getFullEventTypesAndNumbers(self, configName, configVersion, eventTypeId):
    server = self.__getServer()
    result = server.getFullEventTypesAndNumbers(configName, configVersion, long(eventTypeId))
    return result

  #############################################################################
  def getSpecificFiles(self, configName, configVersion, programName, programVersion, fileType, eventTypeId, production):
    server = self.__getServer()
    result = server.getSpecificFiles(configName, configVersion, programName, programVersion, fileType, long(eventTypeId), long(production))
    return result

  #############################################################################
  def getPass_index(self):
    server = self.__getServer()
    result = server.getPass_index()
    return result

  #############################################################################
  def insert_pass_index(self, groupdesc, step0, step1, step2, step3, step4, step5, step6):
    server = self.__getServer()
    result = server.insert_pass_index(groupdesc, step0, step1, step2, step3, step4, step5, step6)
    return result
  
  #############################################################################
  def insert_pass_index_new(self, groupdesc, step0, step1, step2, step3, step4, step5, step6):
    server = self.__getServer()
    result = server.insert_pass_index_new(groupdesc, step0, step1, step2, step3, step4, step5, step6)
    return result
  
  #############################################################################
  def insert_aplications(self, appName, appVersion, option, dddb, condb, extrapack):
    server = self.__getServer()
    result = server.insert_aplications(appName, appVersion, option, dddb, condb, extrapack)
    return result

  #############################################################################
  def insert_pass_index_migration(self, passid, descr, groupid, step0,step1, step2,step3,step4,step5,step6):
    server = self.__getServer()
    result = server.insert_pass_index_migration(passid, descr, groupid, step0, step1, step2, step3, step4, step5, step6)
    return result
    
  #############################################################################
  def insertProcessing(self, production, passdessc, inputprod, simcondsesc):
    server = self.__getServer()
    result = server.insertProcessing(long(production), passdessc, inputprod, simcondsesc)
    return result
  
  #############################################################################
  def checkAddProduction(self, infos):
    server = self.__getServer()
    result = server.checkAddProduction(infos)
    return result
    
  #############################################################################
  def getProductionInformations_new(self, prodid):
    server = self.__getServer()
    result = server.getProductionInformations_new(prodid)
    return result 
  
  #############################################################################
  def listProcessingPass(self, prod=None):
    server = self.__getServer()
    result = None
    if prod != None:
      result = server.listProcessingPass(prod)
    else:
      result = server.listProcessingPass()
    return result

  #############################################################################
  def getProductionsWithPocessingPass(self, processingPass):
    server = self.__getServer()
    result = server.getProductionsWithPocessingPass(processingPass)
    return result

  #############################################################################
  def getFilesByProduction(self, production, eventtype, filetype):
    server = self.__getServer()
    result = server.getFilesByProduction(long(production), long(eventtype), filetype)
    return result

  #############################################################################
  def getProductions(self, configName, configversion, eventTypeId):
    server = self.__getServer()
    result = server.getProductions(configName, configversion, long(eventTypeId))
    return result

  #############################################################################
  def getNumberOfEvents(self, configName, configversion, eventTypeId, production):
    server = self.__getServer()
    result = server.getNumberOfEvents(configName, configversion, long(eventTypeId), long(production))
    return result

   #############################################################################
  def getEventTyesWithProduction(self, production):
    server = self.__getServer()
    result = server.getEventTyesWithProduction(long(production))
    return result

  #############################################################################
  def getFileTypesWithProduction(self, production, eventtype):
    server = self.__getServer()
    result = server.getFileTypesWithProduction(long(production), long(eventtype))
    return result

  #############################################################################
  def getSpecificFilesWithoutProd(self, configName, configVersion, pname, pversion, filetype, eventType):
    server = self.__getServer()
    result = server.getSpecificFilesWithoutProd(configName, configVersion, pname, pversion, filetype, long(eventType))
    return result

  #############################################################################
  def getFileTypes(self, configName, configVersion, eventType, prod):
    server = self.__getServer()
    result = server.getFileTypes(configName, configVersion, long(eventType), long(prod))
    return result

  #############################################################################
  def getProgramNameAndVersion(self, configName, configVersion, eventType, prod, fileType):
    server = self.__getServer()
    result = server.getProgramNameAndVersion(configName, configVersion, long(eventType), long(prod), fileType)
    return result

  #############################################################################
  def addProduction(self, infos):
    server = self.__getServer()
    result = server.addProduction(infos)
    return result

  #############################################################################
  def getLogfile(self, lfn):
    server = self.__getServer()
    result = server.getLogfile(lfn)
    return result

  #############################################################################
  def getRunFiles(self, runid):
    server = self.__getServer()
    result = server.getRunFiles(runid)
    return result

  #-----------------------------------Event Types------------------------------------------------------------------

  #############################################################################
  def getAvailableEventTypes(self):
    server = self.__getServer()
    return server.getAvailableEventTypes()

  #############################################################################
  def getConfigNameAndVersion(self, eventTypeId):
    server = self.__getServer()
    return server.getConfigNameAndVersion(long(eventTypeId))

  #############################################################################
  def getAvailableProcessingPass(self, configName, configVersion, eventTypeId):
    server = self.__getServer()
    return server.getAvailableProcessingPass(configName, configVersion, long(eventTypeId))

  #############################################################################
  def getFileTypesWithEventType(self, configName, configVersion, eventTypeId, production):
    server = self.__getServer()
    return server.getFileTypesWithEventType(configName, configVersion, long(eventTypeId), long(production))

  #############################################################################
  def getFileTypesWithEventTypeALL(self, configName, configVersion, eventTypeId):
    server = self.__getServer()
    return server.getFileTypesWithEventTypeALL(configName, configVersion, long(eventTypeId))

  #############################################################################
  def getFilesByEventType(self, configName, configVersion, fileType, eventTypeId, production):
    server = self.__getServer()
    return server.getFilesByEventType(configName, configVersion, fileType, long(eventTypeId), long(production))

  #############################################################################
  def getFilesByEventTypeALL(self, configName, configVersion, fileType, eventTypeId):
    server = self.__getServer()
    return server.getFilesByEventTypeALL(configName, configVersion, fileType, long(eventTypeId))

  #############################################################################
  def getProductionsWithEventTypes(self, eventType, configName,  configVersion, processingPass):
    server = self.__getServer()
    return server.getProductionsWithEventTypes(long(eventType), configName,  configVersion, processingPass)

  #############################################################################
  def getAvailableRunNumbers(self):
    server = self.__getServer()
    return server.getAvailableRunNumbers()
  
  #############################################################################
  def getProPassWithRunNumber(self, runnumber):
    server = self.__getServer()
    return server.getProPassWithRunNumber(str(runnumber))
  
  #############################################################################
  def getEventTypeWithAgivenRuns(self, runnumber, processing):
    server = self.__getServer()
    return server.getEventTypeWithAgivenRuns(str(runnumber), str(processing))
  
  #############################################################################
  def getFileTypesWithAgivenRun(self, runnumber, procPass, evtId):
    server = self.__getServer()
    return server.getFileTypesWithAgivenRun(str(runnumber), str(procPass), str(evtId))
  
  #############################################################################
  def getLimitedNbOfRunFiles(self,  procPass, evtId, runnumber, ftype):
    server = self.__getServer()
    return server.getLimitedNbOfRunFiles(str(procPass), str(evtId), str(runnumber), str(ftype))
  
  #############################################################################
  def getLimitedFilesWithAgivenRun(self, procPass, evtId, runnumber, ftype, startitem, maxitems):
    server = self.__getServer()
    return server.getLimitedFilesWithAgivenRun(str(procPass), str(evtId), str(runnumber), str(ftype), startitem, maxitems)
  
  #############################################################################
  def getRunFilesWithAgivenRun(self, procPass, evtId, runnumber, ftype):
    server = self.__getServer()
    return server.getRunFilesWithAgivenRun(str(procPass), str(evtId), str(runnumber), str(ftype))
   
  
  #############################################################################
  def addReplica(self, fileName):
    server = self.__getServer()
    return server.addReplica(fileName)

  #############################################################################
  def removeReplica(self, fileName):
    server = self.__getServer()
    return server.removeReplica(fileName)

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
  def getFileMetadata(self, lfns):
    server = self.__getServer()
    return server.getFileMetadata(lfns)

  #############################################################################
  def getFilesInformations(self,lfns):
    server = self.__getServer()
    return server.getFilesInformations(lfns)

  #############################################################################
  def exists(self, lfns):
    server = self.__getServer()
    return server.exists(lfns)

  #############################################################################
  def getLFNsByProduction(self, prodid):
    server = self.__getServer()
    return server.getLFNsByProduction(long(prodid))
  
  #############################################################################
  def getProductionsWithPrgAndEvt(self, programName='ALL', programversion='ALL', evt='ALL'):
    server = self.__getServer()
    return server.getProductionsWithPrgAndEvt(programName, programversion, evt)
  
  #############################################################################
  def checkProduction(self,prodid):
    server = self.__getServer()
    return server.checkProduction(long(prodid))

  #############################################################################
  def getProcessingPassGroups(self):
     server = RPCClient('Bookkeeping/BookkeepingManager')
     return server.getProcessingPassGroups()

  #############################################################################
  def insert_pass_group(self, gropupdesc):
    server = RPCClient('Bookkeeping/BookkeepingManager')
    return server.insert_pass_group(gropupdesc)

  #############################################################################
  def renameFile(self, oldLFN, newLFN):
    server = RPCClient('Bookkeeping/BookkeepingManager')
    return server.renameFile(oldLFN, newLFN)

  #############################################################################
  def getJobsIds(self, filelist):
    server = RPCClient('Bookkeeping/BookkeepingManager')
    return server.getJobsIds(filelist)

  #############################################################################
  def getInputAndOutputJobFiles(self, jobids):
    server = RPCClient('Bookkeeping/BookkeepingManager')
    return server.getInputAndOutputJobFiles(jobids)

  #############################################################################
  def updateFileMetaData(self, filename, filesAttr):
    server = RPCClient('Bookkeeping/BookkeepingManager')
    return server.updateFileMetaData( filename, filesAttr)

  #############################################################################
  def getFilesWithGivenDataSets(self, values):
    server = RPCClient('Bookkeeping/BookkeepingManager')
    return server.getFilesWithGivenDataSets(values)
  
  #############################################################################
  def getFileHistory(self, lfn):
    server = RPCClient('Bookkeeping/BookkeepingManager')
    return server.getFileHistory(lfn)
  
  #############################################################################
  def getProductionInformationsFromView(self, prodid):
    server = RPCClient('Bookkeeping/BookkeepingManager')
    return server.getProductionInformationsFromView(long(prodid))
    
  #############################################################################
  def getProcessingPassDescfromProduction(self, prod):
    server = RPCClient('Bookkeeping/BookkeepingManager')
    return server.getProcessingPassDescfromProduction(int(prod))
  
  #############################################################################
  def getAvailableFileTypes(self):
    server = RPCClient('Bookkeeping/BookkeepingManager')
    return server.getAvailableFileTypes()
  
  #############################################################################
  def insertFileTypes(self, ftype, desc):
    server = RPCClient('Bookkeeping/BookkeepingManager')
    return server.insertFileTypes(ftype, desc)
  
  '''
  Monitoring
  '''

  #############################################################################
  def getProductionInformations(self, prodid):
    server = self.__getServer()
    return server.getProductionInformations(long(prodid))

  #############################################################################
  def getNbOfJobsBySites(self, prodid):
    server = self.__getServer()
    return server.getNbOfJobsBySites(long(prodid))

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
  
  
  
   #----------------------------------- END Event Types------------------------------------------------------------------
