########################################################################
# $Id: BookkeepingManagerHandler.py,v 1.46 2008/06/16 09:47:18 zmathe Exp $
########################################################################

""" BookkeepingManaher service is the front-end to the Bookkeeping database 
"""

__RCSID__ = "$Id: BookkeepingManagerHandler.py,v 1.46 2008/06/16 09:47:18 zmathe Exp $"

from types                                                      import *
from DIRAC.Core.DISET.RequestHandler                            import RequestHandler
from DIRAC                                                      import gLogger, S_OK, S_ERROR
from DIRAC.BookkeepingSystem.Service.copyFiles                  import copyXMLfile
from DIRAC.ConfigurationSystem.Client.Config                    import gConfig
from DIRAC.BookkeepingSystem.Agent.DataMgmt.OracleBookkeepingDB import OracleBookkeepingDB
import time,sys,os

global dataMGMT_
dataMGMT_ = OracleBookkeepingDB()
  
def initializeBookkeepingManagerHandler( serviceInfo ):
  """ Put here necessary initializations needed at the service start
  """
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
  types_getSpecificFiles = [StringType, StringType, StringType, StringType, StringType, LongType, LongType]
  def export_getSpecificFiles(self, configName, configVersion, programName, programVersion, fileType, eventTypeId, production):
    return dataMGMT_.getSpecificFiles(configName, configVersion, programName, programVersion, fileType, eventTypeId, production)
  
  #############################################################################  
  types_getProcessingPass = []
  def export_getProcessingPass(self):
    return dataMGMT_.getProcessingPass()
  
  #############################################################################  
  types_getProductionsWithPocessingPass = [StringType]
  def export_getProductionsWithPocessingPass(self, processingPass):
    return dataMGMT_.getProductionsWithPocessingPass(processingPass)
  
  #############################################################################  
  types_getFilesByProduction = [LongType, LongType, StringType]
  def export_getFilesByProduction(self, production, eventtype, filetype):
    return dataMGMT_.getFilesByProduction(production, eventtype, filetype)
  
  #############################################################################  
  types_getProductions = [StringType, StringType, LongType]
  def export_getProductions(self, configName, configversion, eventTypeId):
    return dataMGMT_.getProductions(configName, configversion, long(eventTypeId))
  
  #############################################################################  
  types_getNumberOfEvents = [StringType, StringType, LongType, LongType]
  def export_getNumberOfEvents(self,configName, configversion, eventTypeId, production):
    return dataMGMT_.getNumberOfEvents(configName, configversion, eventTypeId, production)
  
  #############################################################################  
  types_getEventTyesWithProduction = [LongType]
  def export_getEventTyesWithProduction(self, production):
    return dataMGMT_.getEventTyesWithProduction(production)
  
  #############################################################################  
  types_getFileTypesWithProduction = [LongType, LongType]
  def export_getFileTypesWithProduction(self, production, eventType):
    return dataMGMT_.getFileTypesWithProduction(production, eventType)
  
  #############################################################################  
  types_getSpecificFilesWithoutProd = [StringType, StringType, StringType, StringType, StringType, LongType]
  def export_getSpecificFilesWithoutProd(self, configName, configVersion, pname, pversion, filetype, eventType):
    return dataMGMT_.getSpecificFilesWithoutProd(configName, configVersion, pname, pversion, filetype, eventType)
  
  #############################################################################  
  types_getFileTypes = [StringType, StringType, LongType, LongType]
  def export_getFileTypes(self, configName, configVersion, eventType, prod):
    return dataMGMT_.getFileTypes( configName, configVersion, eventType, prod)

  #############################################################################  
  types_getProgramNameAndVersion = [StringType, StringType, LongType, LongType, StringType]
  def export_getProgramNameAndVersion(self, configName, configVersion, eventType, prod, fileType):
    return dataMGMT_.getProgramNameAndVersion(configName, configVersion, eventType, prod, fileType)