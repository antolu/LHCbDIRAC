"""

"""
import DIRAC
from DIRAC import S_OK, S_ERROR, gLogger, gConfig
from DIRAC.Core.DISET.RPCClient import RPCClient



class BookkeepingClient:

  def __init__(self):
    self.server = RPCClient('Bookkeeping/BookkeepingManager')

  #############################################################################
  def echo(self,string):
    res = self.server.echo(string)
    print res
  
  #############################################################################
  def sendBookkeeping(self, name, data):
      """
      Send XML file to BookkeepingManager.
      name- XML file name
      data - XML file
      """
      result = self.server.sendBookkeeping(name, data)
      return result
  
  #############################################################################
  def getAviableConfiguration(self):
    result = self.server.getAviableConfiguration()
    return result
  
  #############################################################################
  def getAviableEventTypes(self):
    result = self.server.getAviableEventTypes()
    return result
  
  #############################################################################
  def getEventTypes(self, configName, configVersion):
    result = self.server.getEventTypes(configName, configVersion)
    return result
  
  #############################################################################
  def getFullEventTypesAndNumbers(self, configName, configVersion, eventTypeId):
    result = self.server.getFullEventTypesAndNumbers(configName, configVersion, long(eventTypeId))
    return result
  
  #############################################################################
  def getFiles(self, configName, configVersion, fileType, eventTypeId, production):
    result = self.server.getFiles(configName, configVersion, fileType, long(eventTypeId), long(production)) 
    return result
  
  #############################################################################
  def getSpecificFiles(self, configName, configVersion, programName, programVersion, fileType, eventTypeId, production):
    result = self.server.getSpecificFiles(configName, configVersion, programName, programVersion, fileType, long(eventTypeId), long(production))
    return result
  
  #############################################################################
  def getProcessingPass(self):
    result = self.server.getProcessingPass()
    return result
  
  #############################################################################  
  def getProductionsWithPocessingPass(self, processingPass):
    result = self.server.getProductionsWithPocessingPass(processingPass)
    return result
  
  #############################################################################  
  def getFilesByProduction(self, production, eventtype, filetype):
    result = self.server.getFilesByProduction(long(production), long(eventtype), filetype)
    return result
  
  #############################################################################  
  def getProductions(self, configName, configversion, eventTypeId):
    result = self.server.getProductions(configName, configversion, long(eventTypeId))
    return result
  
  #############################################################################  
  def getNumberOfEvents(self, configName, configversion, eventTypeId, production):
    result = self.server.getNumberOfEvents(configName, configversion, long(eventTypeId), long(production))
    return result
   
   #############################################################################  
  def getEventTyesWithProduction(self, production):
    result = self.server.getEventTyesWithProduction(long(production))
    return result
  
  #############################################################################  
  def getFileTypesWithProduction(self, production, eventtype):
    result = self.server.getFileTypesWithProduction(long(production), long(eventtype))
    return result
  
  #############################################################################  
  def getSpecificFilesWithoutProd(self, configName, configVersion, pname, pversion, filetype, eventType):
    result = self.server.getSpecificFilesWithoutProd(configName, configVersion, pname, pversion, filetype, long(eventType))
    return result
  
  #############################################################################  
  def getFileTypes(self, configName, configVersion, eventType, prod):
    result = self.server.getFileTypes(configName, configVersion, long(eventType), long(prod))
    return result
  
  #############################################################################  
  def getProgramNameAndVersion(self, configName, configVersion, eventType, prod, fileType):
    result = self.server.getProgramNameAndVersion(configName, configVersion, long(eventType), long(prod), fileType)
    return result
  