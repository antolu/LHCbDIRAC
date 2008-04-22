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
    result = self.server.getFullEventTypesAndNumbers(configName, configVersion, eventTypeId)
    return result
  
  #############################################################################
  def getFiles(self, configName, configVersion, eventTypeId):
    result = self.server.getFiles(configName, configVersion, eventTypeId)
    return result
  
  #############################################################################