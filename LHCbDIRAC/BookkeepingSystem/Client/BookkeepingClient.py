########################################################################
# $Id: BookkeepingClient.py,v 1.36 2008/07/04 14:33:13 zmathe Exp $
########################################################################

"""

"""
import DIRAC
from DIRAC import S_OK, S_ERROR, gLogger, gConfig
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.Core.Base import Script
Script.parseCommandLine()


__RCSID__ = "$Id: BookkeepingClient.py,v 1.36 2008/07/04 14:33:13 zmathe Exp $"

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
  def getAvailableConfigurations(self):
    server = RPCClient('Bookkeeping/BookkeepingManager')
    result = server.getAvailableConfigurations()
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


  #----------------------------------- END Event Types------------------------------------------------------------------