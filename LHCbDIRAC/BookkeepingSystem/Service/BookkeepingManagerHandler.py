########################################################################
# $Id: BookkeepingManagerHandler.py,v 1.58 2008/07/22 14:14:51 zmathe Exp $
########################################################################

""" BookkeepingManaher service is the front-end to the Bookkeeping database 
"""

__RCSID__ = "$Id: BookkeepingManagerHandler.py,v 1.58 2008/07/22 14:14:51 zmathe Exp $"

from types                                                                        import *
from DIRAC.Core.DISET.RequestHandler                                              import RequestHandler
from DIRAC                                                                        import gLogger, S_OK, S_ERROR
from DIRAC.BookkeepingSystem.Service.copyFiles                                    import copyXMLfile
from DIRAC.ConfigurationSystem.Client.Config                                      import gConfig
from DIRAC.BookkeepingSystem.DB.BookkeepingDatabaseClient                         import BookkeepingDatabaseClient
from DIRAC.BookkeepingSystem.Agent.XMLReader.XMLFilesReaderManager                import XMLFilesReaderManager
import time,sys,os

global dataMGMT_
dataMGMT_ = BookkeepingDatabaseClient()

global reader_
reader_ = XMLFilesReaderManager()
  
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
          
          result  = reader_.readXMLfromString(data)
          if not result['OK']:
            return S_ERROR(result['Message'])
          """
          stamp = time.strftime('%Y.%m.%d-%H.%M.%S',time.gmtime())
          
          fileID=int(repr(time.time()).split('.')[1])
          
          filePath ="%s%s.%08d.%s"%(ToDoPath+os.sep, stamp, fileID, name)  
          update_file = open(filePath, "w")
          
          print >>update_file, data
          update_file.close()
          #copyXML(filePath)
          """
          return S_OK("The send bookkeeping finished successfully!")
      except Exception, x:
          print str(x)
          return S_ERROR('Error during processing '+name)
  
  
  #############################################################################
  types_filetransfer = [StringType, StringType]
  def export_filetransfer(self, name, data):
    try:
      gLogger.info("File Transfer: ", name)
      f = open(ToDoPath+'/'+name,'w')
      f.write(data)
      f.close()
      return S_OK("File Transfer fhinished successfully!")
    except Exception, x:
          print str(x)
          return S_ERROR('Error during processing '+name)
 

  #############################################################################
  types_getAvailableConfigurations = []
  def export_getAvailableConfigurations(self):
    return dataMGMT_.getAvailableConfigurations()
  
  #############################################################################
  types_getEventTypes = [StringType, StringType]
  def export_getEventTypes(self, configName, configVersion):
    return dataMGMT_.getEventTypes(configName, configVersion)
   
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
  
  #-----------------------------------Event Types------------------------------------------------------------------
  #############################################################################  
  types_getAvailableEventTypes = []
  def export_getAvailableEventTypes(self):
    return dataMGMT_.getAvailableEventTypes()
  
  #############################################################################  
  types_getConfigNameAndVersion = [LongType]
  def export_getConfigNameAndVersion(self, eventTypeId):
    return dataMGMT_.getConfigNameAndVersion(eventTypeId)
  
  #############################################################################  
  types_getAvailableProcessingPass = [StringType, StringType, LongType]
  def export_getAvailableProcessingPass(self, configName, configVersion, eventTypeId):
    return dataMGMT_.getAvailableProcessingPass(configName, configVersion, eventTypeId)

  #############################################################################
  types_getFileTypesWithEventType = [StringType, StringType, LongType, LongType]
  def export_getFileTypesWithEventType(self, configName, configVersion, eventTypeId, production):
    return dataMGMT_.getFileTypesWithEventType(configName, configVersion, eventTypeId, production)
  #############################################################################
  types_getFileTypesWithEventType = [StringType, StringType, LongType]
  def export_getFileTypesWithEventTypeALL(self, configName, configVersion, eventTypeId):
    return dataMGMT_.getFileTypesWithEventTypeALL(configName, configVersion, eventTypeId)

  #############################################################################
  types_getFilesByEventType = [StringType, StringType, StringType, LongType, LongType]
  def export_getFilesByEventType(self, configName, configVersion, fileType, eventTypeId, production):
    return dataMGMT_.getFilesByEventType(configName, configVersion, fileType, eventTypeId, production)
  
  #############################################################################
  types_getFilesByEventType = [StringType, StringType, StringType, LongType]
  def export_getFilesByEventTypeALL(self, configName, configVersion, fileType, eventTypeId):
    return dataMGMT_.getFilesByEventTypeALL(configName, configVersion, fileType, eventTypeId)
  
  #############################################################################
  types_getProductionsWithEventTypes = [LongType, StringType, StringType, StringType]
  def export_getProductionsWithEventTypes(self, eventType, configName,  configVersion, processingPass):
    return dataMGMT_.getProductionsWithEventTypes(eventType, configName,  configVersion, processingPass)
  
  #############################################################################
  types_addReplica = [StringType, StringType, StringType, StringType]
  def export_addReplica(self, File, Name, Locations, SE):
    return dataMGMT_.addReplica(File, Name, Locations, SE)
  
  #############################################################################
  types_removeReplica = [StringType, StringType, StringType, StringType]
  def export_removeReplica(self, File, Name, Locations, SE):
    return dataMGMT_.removeReplica(File, Name, Locations, SE)
  
  #############################################################################
  types_addEventType = [LongType, StringType, StringType]
  def export_addEventType(self,evid, desc, primary):
    return dataMGMT_.insertEventTypes(evid, desc, primary)
  
  '''
  Monitoring
  '''
  
  #############################################################################
  types_getProductionInformations = [LongType]
  def export_getProductionInformations(self, prodid):
    
    nbjobs = None
    nbOfFiles = None
    sizeofFiles = None
    nbOfEvents = None
    prodinfo = None
    
    value = dataMGMT_.getJobsNb(prodid)
    if value['OK']==True:
      nbjobs = value['Value']
      
    value = dataMGMT_.getNbOfFiles(prodid)
    if value['OK']==True:
      nbOfFiles =value['Value']
      
    value = dataMGMT_.getSizeOfFiles(prodid)
    if value['OK']==True:
      sizeofFiles =  value['Value']
                 
    value = dataMGMT_.getNumberOfEvents(prodid)
    if value['OK']==True:
      nbOfEvents = value['Value']
      
    value = dataMGMT_.getProductionInformation(prodid)
    if value['OK']==True:
      prodinfo = value['Value']
  
    result = {"Production Info":prodinfo,"Number Of jobs":nbjobs,"Number Of files":nbOfFiles,"Number of Events":nbOfEvents}
    return result
  
  #############################################################################
  types_getJobsNb = [LongType]
  def export_getJobsNb(self, prodid):
    return dataMGMT_.getJobsNb(prodid)
  #############################################################################
  types_getNumberOfEvents = [LongType]
  def export_getNumberOfEvents(self, prodid):
    return dataMGMT_.getNumberOfEvents(prodid)
    
  #############################################################################
  types_getSizeOfFiles = [LongType]
  def export_getSizeOfFiles(self, prodid):
    return dataMGMT_.getSizeOfFiles(prodid)
  
  #############################################################################
  types_getNbOfFiles = [LongType]
  def export_getNbOfFiles(self, prodid):
    return dataMGMT_.getNbOfFiles(prodid)
  
  #############################################################################
  types_getProductionInformation = [LongType]
  def export_getProductionInformation(self, prodid):
    return dataMGMT_.getProductionInformation(prodid)
  
  #############################################################################
  types_getNbOfJobsBySites = [LongType]
  def export_getNbOfJobsBySites(self, prodid):
    return dataMGMT_.getNbOfJobsBySites(prodid)
  
  '''
  End Monitoring
  '''
    #-----------------------------------END Event Types------------------------------------------------------------------