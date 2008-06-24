########################################################################
# $Id: OracleBookkeepingDB.py,v 1.1 2008/06/24 11:29:23 zmathe Exp $
########################################################################
"""

"""

__RCSID__ = "$Id: OracleBookkeepingDB.py,v 1.1 2008/06/24 11:29:23 zmathe Exp $"

from DIRAC.BookkeepingSystem.DB.IBookkeepingDB                       import IBookkeepingDB
from DIRAC                                                           import gLogger, S_OK, S_ERROR
from DIRAC.ConfigurationSystem.Client.Config                         import gConfig
from DIRAC.Core.Utilities.OracleDB                                   import OracleDB
class OracleBookkeepingDB(IBookkeepingDB):
  
  #############################################################################
  def __init__(self):
    """
    """
    super(OracleBookkeepingDB, self).__init__()
    #self.user_ = gConfig.getValue("userName", "LHCB_BOOKKEEPING_INT")
    #self.password_ = gConfig.getValue("password", "Ginevra2008")
    #self.tns_ = gConfig.getValue("tns", "int12r")
   
    self.db_ = OracleDB("LHCB_BOOKKEEPING_INT", "Ginevra2008", "int12r")
  
  #############################################################################
  def getAviableConfigNameAndVersion(self):
    """
    """
    return self.db_.executeStoredProcedure('BKK_ORACLE.getAviableConfiguration',[])
  
  #############################################################################
  def getAviableEventTypes(self):
    return self.db_.executeStoredProcedure('BKK_ORACLE.getAviableEventTypes', [])
  
  #############################################################################
  def getEventTypes(self, configName, configVersion):
    return self.db_.executeStoredProcedure('BKK_ORACLE.getEventTypes', [configName, configVersion])
  
  #############################################################################
  def getFullEventTypesAndNumbers(self, configName, configVersion, eventTypeId):
    return self.db_.executeStoredProcedure('BKK_ORACLE.getEventTypeAndNumberAll', [configName, configVersion, eventTypeId])
  
  #############################################################################
  def getFullEventTypesAndNumbers1(self, configName, configVersion, fileType, eventTypeId):
    return self.db_.executeStoredProcedure('BKK_ORACLE.getFullEventTypeAndNumber1', [configName, configVersion, fileType,  eventTypeId])
  
  #############################################################################
  def getFiles(self, configName, configVersion, fileType, eventTypeId, production):
    return self.db_.executeStoredProcedure('BKK_ORACLE.getFiles', [configName, configVersion, fileType, eventTypeId, production])
  
  #############################################################################
  def getSpecificFiles(self,configName, configVersion, programName, programVersion, fileType, eventTypeId, production):
    return self.db_.executeStoredProcedure('BKK_ORACLE.getSpecificFiles', [configName, configVersion, programName, programVersion, fileType, eventTypeId, production])
  
  #############################################################################
  def getAviableFileTypesAndEventTypesAndNumberOfEvents(self,fileType, eventTypeId):
    return self.db_.executeStoredProcedure('BKK_ORACLE.getEventTypeAndNumber', [fileType, eventTypeId])
  
  #############################################################################
  def getAviableEventTypesAndNumberOfEvents(self, configName, configVersion, eventTypeId):
    return self.db_.executeStoredProcedure('BKK_ORACLE.getEventTypeAndNumberAll', [configName, configVersion, eventTypeId])
  
  #############################################################################
  def getProcessingPass(self):
    return self.db_.executeStoredProcedure('BKK_ORACLE.getProcessingPass', [])
  
  #############################################################################  
  def getProductionsWithPocessingPass(self, processingPass):
    return self.db_.executeStoredProcedure('BKK_ORACLE.getProductions', [processingPass])
  
  #############################################################################  
  def getFilesByProduction(self, production, eventtype, filetype):
    return self.db_.executeStoredProcedure('BKK_ORACLE.getFilesByProduction', [production, eventtype, filetype])
  
  #############################################################################
  def getProductions(self, configName, configVersion, eventTypeId):
    return self.db_.executeStoredProcedure('BKK_ORACLE.getProductions', [configName, configVersion, eventTypeId])
  
  #############################################################################
  def getNumberOfEvents(self, configName, configversion, eventTypeId, production):
    return self.db_.executeStoredProcedure('BKK_ORACLE.getNumberOfEvents', [configName, configversion, eventTypeId, production])
  
  #############################################################################
  def getEventTyesWithProduction(self, production):
    return self.db_.executeStoredProcedure('BKK_ORACLE.getEventTyesWithProduction', [production])
  
  #############################################################################  
  def getFileTypesWithProduction(self, production, eventType):
    return self.db_.executeStoredProcedure('BKK_ORACLE.getFileTypesWithProduction', [production, eventType])
  
  #############################################################################  
  def getSpecificFilesWithoutProd(self, configName, configVersion, pname, pversion, filetype, eventType):
    return self.db_.executeStoredProcedure('BKK_ORACLE.getSpecificFilesWithoutProd',[configName,configVersion, pname, pversion, filetype, eventType])
  
  #############################################################################  
  def getFileTypes(self, configName, configVersion, eventType, prod):
    return self.db_.executeStoredProcedure('BKK_ORACLE.getFileTypes',[configName, configVersion, eventType, prod]) 
  
  #############################################################################  
  def getProgramNameAndVersion(self, configName, configVersion, eventType, prod, fileType):
    return self.db_.executeStoredProcedure('BKK_ORACLE.getProgramNameAndVersion', [configName, configVersion, eventType, prod, fileType])
  
  #############################################################################  
  def getAvailableEventTypes(self):
    return self.db_.executeStoredProcedure('BKK_ORACLE.getAvailableEventTypes', [])
  
  #############################################################################  
  def getConfigNameAndVersion(self, eventTypeId):
    return self.db_.executeStoredProcedure('BKK_ORACLE.getConfigNameAndVersion', [eventTypeId])
  
  #############################################################################  
  def getAvailableProcessingPass(self, configName, configVersion, eventTypeId):
    return self.db_.executeStoredProcedure('BKK_ORACLE.getAvailableProcessingPass', [configName, configVersion, eventTypeId])

  #############################################################################
  def getFileTypesWithEventType(self, configName, configVersion, eventTypeId, production):
    return self.db_.executeStoredProcedure('BKK_ORACLE.getFileTypesWithEventType', [configName, configVersion, eventTypeId, production])
  
  #############################################################################
  def getFileTypesWithEventTypeALL(self, configName, configVersion, eventTypeId):
    return self.db_.executeStoredProcedure('BKK_ORACLE.getFileTypesWithEventType', [configName, configVersion, eventTypeId])
  
  #############################################################################
  def getFilesByEventType(self, configName, configVersion, fileType, eventTypeId, production):
    return self.db_.executeStoredProcedure('BKK_ORACLE.getFilesByEventType', [configName, configVersion, fileType, eventTypeId, production])
  
  #############################################################################
  def getFilesByEventTypeALL(self, configName, configVersion, fileType, eventTypeId):
    return self.db_.executeStoredProcedure('BKK_ORACLE.getFilesByEventType', [configName, configVersion, fileType, eventTypeId])
  
  #############################################################################
  def getProductionsWithEventTypes(self, eventType, configName,  configVersion, processingPass):
    return self.db_.executeStoredProcedure('BKK_ORACLE.getProductionsWithEventTypes', [eventType, configName,  configVersion, processingPass])
  
    
  
  """
  data insertation into the database
  """
  #############################################################################
  def checkfile(self, fileName): #file

    result = self.db_.executeStoredProcedure('BKK_ORACLE.checkfile',[fileName])
    if len(result)!=0:
      return S_OK(result)
    else:
      gLogger.error("File not found! ",str(fileName))
      return S_ERROR("File not found!",str(fileName))
  
  #############################################################################
  def checkFileTypeAndVersion(self, type, version): #fileTypeAndFileTypeVersion(self, type, version):
    
    result = self.db_.executeStoredProcedure('BKK_ORACLE.checkFileTypeAndVersion',[type, version])
    if len(result)!=0:
      return S_OK(result)
    else:
      gLogger.error("File not found! ",str(fileName))
      return S_ERROR("File not found!",str(fileName))
  
  #############################################################################
  def checkEventType(self, eventTypeId):  #eventType(self, eventTypeId):
    
    result = self.db_.executeStoredProcedure('BKK_ORACLE.checkEventType',[eventTypeId])
    if len(result)!=0:
      return S_OK(result)
    else:
      gLogger.error("File not found! ",str(fileName))
      return S_ERROR("File not found!",str(fileName))
  
  #############################################################################
  def insertJob(self, config, jobParams):
      
    attrList = {'ConfigName':config.getConfigName(), \
                 'ConfigVersion':config.getConfigVersion(), \
                 'DAQPeriodId':"NULL", \
                 'DiracJobId':"NULL", \
                 'DiracVersion':"NULL", \
                 'EventInputStat':"NULL", \
                 'ExecTime':"NULL", \
                 'FirstEventNumber':"NULL", \
                 'Generator':"NULL", \
                 'GeometryVersion':"NULL", \
                 'GridJobId':"NULL", \
                 'JobEnd':"NULL", \
                 'JobStart':"NULL", \
                 'LocalJobId':"NULL", \
                 'Location':"NULL", \
                 'LuminosityEnd':"NULL", \
                 'LuminosityStart':"NULL", \
                 'Name':"NULL", \
                 'NumberOfEvents':"NULL", \
                 'Production':"NULL", \
                 'ProgramName':"NULL", \
                 'ProgramVersion':"NULL", \
                 'StatisticsRequested':"NULL", \
                 'WNCPUPower':"NULL", \
                 'WNCPUTime':"NULL", \
                 'WNCache':"NULL", \
                 'WNMemory':"NULL", \
                 'WNModel':"NULL", \
                 'WorkerNode':"NULL"}
    
    for param in jobParams:
      attrList[str(param.getName())] = param.getValue()
      
    result = self.db_.executeStoredProcedure('BKK_ORACLE.insertJobsRow',[ attrList['ConfigName'], attrList['ConfigVersion'], \
                  attrList['DAQPeriodId'], \
                  attrList['DiracJobId'], \
                  attrList['DiracVersion'], \
                  attrList['EventInputStat'], \
                  attrList['ExecTime'], \
                  attrList['FirstEventNumber'], \
                  attrList['Generator'], \
                  attrList['GeometryVersion'], \
                  attrList['GridJobId'], \
                  attrList['JobEnd'], \
                  attrList['JobStart'], \
                  attrList['LocalJobId'], \
                  attrList['Location'], \
                  attrList['LuminosityEnd'], \
                  attrList['LuminosityStart'], \
                  attrList['Name'], \
                  attrList['NumberOfEvents'], \
                  attrList['Production'], \
                  attrList['ProgramName'], \
                  attrList['ProgramVersion'], \
                  attrList['StatisticsRequested'], \
                  attrList['WNCPUPower'], \
                  attrList['WNCPUTime'], \
                  attrList['WNCache'], \
                  attrList['WNMemory'], \
                  attrList['WNModel'], \
                  attrList['WorkerNode'] ])           
    return result
    
    
  #############################################################################
  def insertInputFile(self, jobID, FileId):
    result = self.db_.executeStoredProcedure('BKK_ORACLE.insertInputFilesRow',[jobID, FileId])
    return result
  #############################################################################
  def insertOutputFile(self, job, file):
  
      attrList = {  'Adler32':"NULL", \
                    'CreationDate':"NULL", \
                    'EventStat':"NULL", \
                    'EventTypeId':"NULL", \
                    'FileName':"NULL",  \
                    'FileTypeId':outputFile.getTypeID(), \
                    'GotReplica':"NULL", \
                    'Guid':"NULL",  \
                    'JobId':job.getJobId(), \
                    'MD5Sum':"NULL", \
                    'FileSize':"NULL" }
      
      
      fileParams = outputFile.getFileParams()
      for param in fileParams:
        attrList[str(param.getName())] = param.getValue()
      
      result = self.db_.executeStoredProcedure('BKK_ORACLE.insertFilesRow',[  attrList['Adler32'], \
                    attrList['CreationDate'], \
                    attrList['EventStat'], \
                    attrList['EventTypeId'], \
                    attrList['FileName'],  \
                    attrList['FileTypeId'], \
                    attrList['GotReplica'], \
                    attrList['Guid'],  \
                    attrList['JobId'], \
                    attrList['MD5Sum'], \
                    attrList['FileSize'] ] ) 
      return result
      
  #############################################################################
  def insertReplica(self, fileID, name, location):
    gLogger.warn("not implemented")
    return S_ERROR()
  
  #############################################################################
  def deleteJob(self, job):
    gLogger.warn("not implemented")
    return S_ERROR()
  
  #############################################################################
  def deleteFile(self, file):
    gLogger.warn("not implemented")
    return S_ERROR()
  
  #############################################################################
  def insertQuality(self, fileID, group, flag ):
    gLogger.warn("not implemented")
    return S_ERROR()
  
  #############################################################################
  def insertQualityParam(self, fileID, qualityID, name, value):
    gLogger.warn("not implemented")
    return S_ERROR()
  
  #############################################################################
  def modifyReplica(self, fileID , name, value):
    gLogger.warn("not implemented")
    return S_ERROR()
  
  #############################################################################
