########################################################################
# $Id: BookkeepingManagerHandler.py,v 1.21 2008/04/22 13:51:31 zmathe Exp $
########################################################################

""" BookkeepingManaher service is the front-end to the Bookkeeping database 
"""

__RCSID__ = "$Id: BookkeepingManagerHandler.py,v 1.21 2008/04/22 13:51:31 zmathe Exp $"

from types                                                      import *
from DIRAC.Core.DISET.RequestHandler                            import RequestHandler
from DIRAC                                                      import gLogger, S_OK, S_ERROR
from DIRAC.BookkeepingSystem.Service.copyFiles                  import copyXMLfile
from DIRAC.ConfigurationSystem.Client.Config                    import gConfig
from DIRAC.BookkeepingSystem.Agent.DataMgmt.OracleBookkeepingDB import OracleBookkeepingDB
import time,sys,os


def initializeBookkeepingManagerHandler( serviceInfo ):
  """ Put here necessary initializations needed at the service start
  """
  global dataMGMT_
  dataMGMT_ = OracleBookkeepingDB()
  return S_OK()

ToDoPath = gConfig.getValue("stuart","/opt/bookkeeping/XMLProcessing/ToDo")
    
class BookkeepingManagerHandler(RequestHandler):

  ###########################################################################
  # types_<methodname> global variable is a list which defines for each exposed 
  # method the types of its arguments, the argument types are ignored if the list is empty.

  types_echo = [StringType]
  def export_echo(self,input):
    """ Echo input to output
    """
    return S_OK(input)

  #############################################################################
  types_sendBookkeeping = [StringType, StringType]
  def export_sendBookkeeping(self, name, data):
      """
      This method send XML file to the ToDo directory
      """
      try:
          stamp = time.strftime('%Y.%m.%d-%H.%M.%S',time.gmtime())
          
          fileID=int(repr(time.time()).split('.')[1])
          
          filePath ="%s%s.%08d.%s"%(ToDoPath+os.sep, stamp, fileID, name)  
          update_file = open(filePath, "w")
          print >>update_file, data
          update_file.close()
          #copyXML(filePath)
          return S_OK("The send bookkeeping finished successfully!")
      except Exception, x:
          print str(x)
          return S_ERROR('Error during processing '+name)
  
  #############################################################################
  types_getAviableConfiguration = []
  def export_getAviableConfiguration(self):
    return dataMGMT_.getAviableConfigNameAndVersion()
  
  #############################################################################
  types_getAviableEventTypes = []
  def export_getAviableEventTypes(self):
    return dataMGMT_.getAviableEventTypes()
  
  #############################################################################
  types_getEventTypes = [StringType, StringType]
  def export_getEventTypes(self, configName, configVersion):
    return dataMGMT_.getEventTypes(configName, configVersion)
  
  #############################################################################
  types_getFullEventTypesAndNumbers = [StringType, StringType, LongType]
  def export_getFullEventTypesAndNumbers(self, configName, configVersion, eventTypeId):
    return dataMGMT_.getFullEventTypesAndNumbers(configName, configVersion, eventTypeId)
  
  #############################################################################
  types_getFiles = [StringType, StringType, StringType, LongType, LongType]
  def export_getFiles(self, configName, configVersion, fileType, eventTypeId, production):
    return dataMGMT_.getFiles(configName, configVersion, fileType, eventTypeId, production)
  
  #############################################################################