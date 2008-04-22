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
    result = self.server.getSpecificFiles(configName, configVersion, programName, programVersion, fileType, eventTypeId, production)
    return result
  
  #############################################################################