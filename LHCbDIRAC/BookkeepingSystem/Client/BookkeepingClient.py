########################################################################
# $Id: BookkeepingClient.py,v 1.46 2008/09/12 16:18:10 zmathe Exp $
########################################################################

"""

"""
import DIRAC
from DIRAC import S_OK, S_ERROR, gLogger, gConfig
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.Core.Base import Script
import types
Script.parseCommandLine()


__RCSID__ = "$Id: BookkeepingClient.py,v 1.46 2008/09/12 16:18:10 zmathe Exp $"

class BookkeepingClient:

  def __init__(self):
    pass

  #############################################################################
  def echo(self,string):
    server = RPCClient('Bookkeeping/BookkeepingManager')
    res = server.echo(string)
    print res

  #############################################################################
  def sendBookkeeping(self, name, data):
      """
      Send XML file to BookkeepingManager.
      name- XML file name
      data - XML file
      """
      server = RPCClient('Bookkeeping/BookkeepingManager')
      result = server.sendBookkeeping(name, data)
      return result

  #############################################################################
  def filetransfer(self, name, data):
    server = RPCClient('Bookkeeping/BookkeepingManager')
    result = server.filetransfer(name, data)

  #############################################################################
  def deleteJob(self, job):
    server = RPCClient('Bookkeeping/BookkeepingManager')
    result = server.deleteJob(long(job))  
    return result
  
  #############################################################################
  def deleteFiles(self, lfns):
    server = RPCClient('Bookkeeping/BookkeepingManager')
    result = server.deleteFiles(lfns)  
    return result
  
  #############################################################################
  def checkfile(self, fileName):
     server = RPCClient('Bookkeeping/BookkeepingManager')
     result = server.checkfile(fileName)  
     return result
  
  
  #############################################################################
  def checkFileTypeAndVersion(self, type, version):
     server = RPCClient('Bookkeeping/BookkeepingManager')
     result = server.checkFileTypeAndVersion(type, version)
     return result
  
  
  #############################################################################
  def checkEventType(self, eventTypeId): 
     server = RPCClient('Bookkeeping/BookkeepingManager')
     result = server.checkEventType(lonhg(eventTypeId)) 
     return result
   
  
  #############################################################################
  def insertJob(self, job):
     server = RPCClient('Bookkeeping/BookkeepingManager')
     result = server.insertJob(job)  
     return result
    
  #############################################################################
  def insertInputFile(self, jobID, FileId):
     server = RPCClient('Bookkeeping/BookkeepingManager')
     result = server.insertInputFile(long(jobID), long(FileId))
     return result
  
  #############################################################################
  def insertOutputFile(self, file):
     server = RPCClient('Bookkeeping/BookkeepingManager')
     result = server.insertOutputFile(self, file)
     return result
  
  #############################################################################
  def deleteInputFiles(self, jobid):
     server = RPCClient('Bookkeeping/BookkeepingManager')
     result = server.deleteInputFiles(long(jobid))  
     return result
  
  #############################################################################
  def getSimulationCondID(self, BeamCond, BeamEnergy, Generator, MagneticField, DetectorCond, Luminosity):
    server = RPCClient('Bookkeeping/BookkeepingManager')
    result = server.getSimulationCondID(BeamCond, BeamEnergy, Generator, MagneticField, DetectorCond, Luminosity)
    return result
  
  #############################################################################
  def insertSimConditions(self, simdesc, BeamCond, BeamEnergy, Generator, MagneticField, DetectorCond, Luminosity):
    server = RPCClient('Bookkeeping/BookkeepingManager')
    result = server.insertSimConditions(simdesc, BeamCond, BeamEnergy, Generator, MagneticField, DetectorCond, Luminosity)
    return result
  
  #############################################################################
  def getSimCondIDWhenFileName(self, fileName):
    server = RPCClient('Bookkeeping/BookkeepingManager')
    result = server.getSimCondIDWhenFileName(fileName)
    return result
    
  #############################################################################
  def updateReplicaRow(self, fileID, replica):
     server = RPCClient('Bookkeeping/BookkeepingManager')
     result = server.updateReplicaRow(long(fileID), replica)
     return result
    
  #############################################################################
  def getAvailableConfigurations(self):
    server = RPCClient('Bookkeeping/BookkeepingManager')
    result = server.getAvailableConfigurations()
    return result

  #############################################################################
  def getSimulationConditions(self, configName, configVersion):
    server = RPCClient('Bookkeeping/BookkeepingManager')
    result = server.getSimulationConditions(configName, configVersion)
    return result
  
  #############################################################################
  def getProPassWithSimCond(self, configName, configVersion, simcondid):
    server = RPCClient('Bookkeeping/BookkeepingManager')
    result = server.getProPassWithSimCond(configName, configVersion, long(simcondid))
    return result
  
  #############################################################################
  def getEventTypeWithSimcond(self,configName, configVersion, simcondid, procPass):
    server = RPCClient('Bookkeeping/BookkeepingManager')
    result = server.getEventTypeWithSimcond(configName, configVersion, long(simcondid), procPass)
    return result
  
  #############################################################################
  def getProductionsWithSimcond(self, configName, configVersion, simcondid, procPass, evtId):
    server = RPCClient('Bookkeeping/BookkeepingManager')
    result = server.getProductionsWithSimcond(configName, configVersion, long(simcondid), procPass, long(evtId))
    return result
  
  #############################################################################
  def getFileTypesWithSimcond(self, configName, configVersion, simcondid, procPass, evtId, prod):
    server = RPCClient('Bookkeeping/BookkeepingManager')
    result = server.getFileTypesWithSimcond(configName, configVersion, long(simcondid), procPass, long(evtId), long(prod))
    return result
  
  #############################################################################  
  def getProgramNameWithSimcond(self, configName, configVersion, simcondid, procPass, evtId, prod, ftype):
    server = RPCClient('Bookkeeping/BookkeepingManager')
    result = server.getProgramNameWithSimcond(configName, configVersion, long(simcondid), procPass, long(evtId), long(prod), ftype)
    return result
  
  #############################################################################  
  def getFilesWithSimcond(self, configName, configVersion, simcondid, procPass, evtId, prod, ftype, progName, progVersion):
    server = RPCClient('Bookkeeping/BookkeepingManager')
    result = server.getFilesWithSimcond(configName, configVersion, long(simcondid), procPass, long(evtId), long(prod), ftype, progName, progVersion)
    return result
 
  def getSimConditions(self):
    server = RPCClient('Bookkeeping/BookkeepingManager')
    result = server.getSimConditions()
    return result

  
  #############################################################################
  def getSimCondWithEventType(self, configName, configVersion, eventType):
    server = RPCClient('Bookkeeping/BookkeepingManager')
    result = server.getSimCondWithEventType(configName, configVersion, long(eventType))
    return result
    
  #############################################################################
  def getProPassWithEventType(self, configName, configVersion, eventType, simcond):
    server = RPCClient('Bookkeeping/BookkeepingManager')
    result = server.getProPassWithEventType(configName, configVersion, long(eventType), long(simcond))
    return result
        




  #############################################################################  
  def getAncestors(self, lfns, depth=1):
    server = RPCClient('Bookkeeping/BookkeepingManager')
    result = None
    if type(lfns) == types.StringType:
      result = server.getAncestors([lfns], long(depth))
    else:
      result = server.getAncestors(lfns, long(depth))
    return result
  
  #############################################################################
  def getAvailableEventTypes(self):
    server = RPCClient('Bookkeeping/BookkeepingManager')
    result = server.getAvailableEventTypes()
    return result

  #############################################################################
  def getEventTypes(self, configName, configVersion):
    server = RPCClient('Bookkeeping/BookkeepingManager')
    result = server.getEventTypes(configName, configVersion)
    return result

  #############################################################################
  def getFullEventTypesAndNumbers(self, configName, configVersion, eventTypeId):
    server = RPCClient('Bookkeeping/BookkeepingManager')
    result = server.getFullEventTypesAndNumbers(configName, configVersion, long(eventTypeId))
    return result

  #############################################################################
  def getSpecificFiles(self, configName, configVersion, programName, programVersion, fileType, eventTypeId, production):
    server = RPCClient('Bookkeeping/BookkeepingManager')
    result = server.getSpecificFiles(configName, configVersion, programName, programVersion, fileType, long(eventTypeId), long(production))
    return result

  #############################################################################
  def getProcessingPass(self):
    server = RPCClient('Bookkeeping/BookkeepingManager')
    result = server.getProcessingPass()
    return result

  #############################################################################
  def getProductionsWithPocessingPass(self, processingPass):
    server = RPCClient('Bookkeeping/BookkeepingManager')
    result = server.getProductionsWithPocessingPass(processingPass)
    return result

  #############################################################################
  def getFilesByProduction(self, production, eventtype, filetype):
    server = RPCClient('Bookkeeping/BookkeepingManager')
    result = server.getFilesByProduction(long(production), long(eventtype), filetype)
    return result

  #############################################################################
  def getProductions(self, configName, configversion, eventTypeId):
    server = RPCClient('Bookkeeping/BookkeepingManager')
    result = server.getProductions(configName, configversion, long(eventTypeId))
    return result

  #############################################################################
  def getNumberOfEvents(self, configName, configversion, eventTypeId, production):
    server = RPCClient('Bookkeeping/BookkeepingManager')
    result = server.getNumberOfEvents(configName, configversion, long(eventTypeId), long(production))
    return result

   #############################################################################
  def getEventTyesWithProduction(self, production):
    server = RPCClient('Bookkeeping/BookkeepingManager')
    result = server.getEventTyesWithProduction(long(production))
    return result

  #############################################################################
  def getFileTypesWithProduction(self, production, eventtype):
    server = RPCClient('Bookkeeping/BookkeepingManager')
    result = server.getFileTypesWithProduction(long(production), long(eventtype))
    return result

  #############################################################################
  def getSpecificFilesWithoutProd(self, configName, configVersion, pname, pversion, filetype, eventType):
    server = RPCClient('Bookkeeping/BookkeepingManager')
    result = server.getSpecificFilesWithoutProd(configName, configVersion, pname, pversion, filetype, long(eventType))
    return result

  #############################################################################
  def getFileTypes(self, configName, configVersion, eventType, prod):
    server = RPCClient('Bookkeeping/BookkeepingManager')
    result = server.getFileTypes(configName, configVersion, long(eventType), long(prod))
    return result

  #############################################################################
  def getProgramNameAndVersion(self, configName, configVersion, eventType, prod, fileType):
    server = RPCClient('Bookkeeping/BookkeepingManager')
    result = server.getProgramNameAndVersion(configName, configVersion, long(eventType), long(prod), fileType)
    return result

  #-----------------------------------Event Types------------------------------------------------------------------

  #############################################################################
  def getAvailableEventTypes(self):
    server = RPCClient('Bookkeeping/BookkeepingManager')
    return server.getAvailableEventTypes()

  #############################################################################
  def getConfigNameAndVersion(self, eventTypeId):
    server = RPCClient('Bookkeeping/BookkeepingManager')
    return server.getConfigNameAndVersion(long(eventTypeId))

  #############################################################################
  def getAvailableProcessingPass(self, configName, configVersion, eventTypeId):
    server = RPCClient('Bookkeeping/BookkeepingManager')
    return server.getAvailableProcessingPass(configName, configVersion, long(eventTypeId))

  #############################################################################
  def getFileTypesWithEventType(self, configName, configVersion, eventTypeId, production):
    server = RPCClient('Bookkeeping/BookkeepingManager')
    return server.getFileTypesWithEventType(configName, configVersion, long(eventTypeId), long(production))

  #############################################################################
  def getFileTypesWithEventTypeALL(self, configName, configVersion, eventTypeId):
    server = RPCClient('Bookkeeping/BookkeepingManager')
    return server.getFileTypesWithEventTypeALL(configName, configVersion, long(eventTypeId))

  #############################################################################
  def getFilesByEventType(self, configName, configVersion, fileType, eventTypeId, production):
    server = RPCClient('Bookkeeping/BookkeepingManager')
    return server.getFilesByEventType(configName, configVersion, fileType, long(eventTypeId), long(production))

  #############################################################################
  def getFilesByEventTypeALL(self, configName, configVersion, fileType, eventTypeId):
    server = RPCClient('Bookkeeping/BookkeepingManager')
    return server.getFilesByEventTypeALL(configName, configVersion, fileType, long(eventTypeId))

  #############################################################################
  def getProductionsWithEventTypes(self, eventType, configName,  configVersion, processingPass):
    server = RPCClient('Bookkeeping/BookkeepingManager')
    return server.getProductionsWithEventTypes(long(eventType), configName,  configVersion, processingPass)

  #############################################################################
  def addReplica(self, fileName):
    server = RPCClient('Bookkeeping/BookkeepingManager')
    return server.addReplica(fileName)
  
  #############################################################################
  def removeReplica(self, fileName):
    server = RPCClient('Bookkeeping/BookkeepingManager')
    return server.removeReplica(fileName)
  
  #############################################################################
  def addEventType(self, evid, desc, primary):
    server = RPCClient('Bookkeeping/BookkeepingManager')
    return server.addEventType(long(evid), desc, primary)
  
  #############################################################################
  def updateEventType(self, evid, desc, primary):
    server = RPCClient('Bookkeeping/BookkeepingManager')
    return server.updateEventType(long(evid), desc, primary)
  
  #############################################################################
  def addFiles(self, lfns):
    server = RPCClient('Bookkeeping/BookkeepingManager')
    return server.addFiles(lfns)
  
  #############################################################################
  def removeFiles(self, lfns):
    server = RPCClient('Bookkeeping/BookkeepingManager')
    return server.removeFiles(lfns)
  
  #############################################################################
  def getFileMetadata(self, lfns):
    server = RPCClient('Bookkeeping/BookkeepingManager')
    return server.getFileMetadata(lfns)
  
  #############################################################################
  def exists(self, lfns):
    server = RPCClient('Bookkeeping/BookkeepingManager')
    return server.exists(lfns)
  
  #############################################################################
  def getLFNsByProduction(self, prodid):
    server = RPCClient('Bookkeeping/BookkeepingManager')
    return server.getLFNsByProduction(long(prodid))
  
  #############################################################################
  def checkProduction(self,prodid):  
    server = RPCClient('Bookkeeping/BookkeepingManager')
    return server.checkProduction(long(prodid))
  
  '''
  Monitoring
  '''
  
  #############################################################################
  def getProductionInformations(self, prodid):
    server = RPCClient('Bookkeeping/BookkeepingManager')
    return server.getProductionInformations(long(prodid))
  
  #############################################################################
  def getNbOfJobsBySites(self, prodid):
    server = RPCClient('Bookkeeping/BookkeepingManager')
    return server.getNbOfJobsBySites(long(prodid))
    
  #############################################################################
  def getJobsNb(self, prodid):
    server = RPCClient('Bookkeeping/BookkeepingManager')
    return server.getJobsNb(long(prodid))
  
  #############################################################################
  def getNumberOfEvents(self, prodid):
    server = RPCClient('Bookkeeping/BookkeepingManager')
    return server.getNumberOfEvents(long(prodid))
  
  #############################################################################
  def getSizeOfFiles(self, prodid):
    server = RPCClient('Bookkeeping/BookkeepingManager')
    return server.getSizeOfFiles(long(prodid))
  
  #############################################################################
  def getNbOfFiles(self, prodid):
    server = RPCClient('Bookkeeping/BookkeepingManager')
    return server.getNbOfFiles(long(prodid))
  
  #############################################################################
  def getProductionInformation(self, prodid):
    server = RPCClient('Bookkeeping/BookkeepingManager')
    return server.getProductionInformation(long(prodid))
    
  
  '''
  END Monitoring
  '''

  #----------------------------------- END Event Types------------------------------------------------------------------