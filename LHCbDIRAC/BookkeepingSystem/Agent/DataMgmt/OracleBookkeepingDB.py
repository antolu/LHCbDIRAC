########################################################################
# $Id: OracleBookkeepingDB.py,v 1.20 2008/05/30 10:58:10 zmathe Exp $
########################################################################
"""

"""

__RCSID__ = "$Id: OracleBookkeepingDB.py,v 1.20 2008/05/30 10:58:10 zmathe Exp $"

from DIRAC.BookkeepingSystem.Agent.DataMgmt.IBookkeepingDB           import IBookkeepingDB
from DIRAC                                                           import gLogger, S_OK, S_ERROR
from DIRAC.ConfigurationSystem.Client.Config                         import gConfig
from DIRAC.BookkeepingSystem.Agent.DataMgmt.OracleDB                 import OracleDB

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
    return self.db_.execute('select distinct jobs.ConfigName, jobs.ConfigVersion from jobs')
  
  #############################################################################
  def getAviableEventTypes(self):
    return self.db_.executeAviableEventNbCursor()
  
  #############################################################################
  def getEventTypes(self, configName, configVersion):
    return self.db_.executeEventTypesCursor(configName, configVersion)
  
  #############################################################################
  def getFullEventTypesAndNumbers(self, configName, configVersion, eventTypeId):
    return self.db_.executeFullEventTypeAndNumberCursor(configName, configVersion, eventTypeId)
  
  #############################################################################
  def getFullEventTypesAndNumbers1(self, configName, configVersion, fileType, eventTypeId):
    return self.db_.executeFullEventTypeAndNumberCursor1(configName, configVersion, fileType,  eventTypeId)
  
  #############################################################################
  def getFiles(self, configName, configVersion, fileType, eventTypeId, production):
    return self.db_.executeGetFiles(configName, configVersion, fileType, eventTypeId, production)
  
  #############################################################################
  def getSpecificFiles(self,configName, configVersion, programName, programVersion, fileType, eventTypeId, production):
    return self.db_.executeGetSpecificFiles(configName, configVersion, programName, programVersion, fileType, eventTypeId, production)
  #############################################################################
  def getAviableFileTypesAndEventTypesAndNumberOfEvents(self,fileType, eventTypeId):
    return self.db_.executegetEventTypeAndNumber(fileType, eventTypeId)
  
  #############################################################################
  def getAviableEventTypesAndNumberOfEvents(self, configName, configVersion, eventTypeId):
    return self.db_.executegetAviableEventTypesAndNumberOfEvents(configName, configVersion, eventTypeId)
  
  def getProcessingPass(self):
    return self.db_.executegetProcessingPass()
  
  #############################################################################  
  def getProductions(self, processingPass):
    return self.db_.executegetProductions(processingPass)
  
  #############################################################################  
  def getFilesByProduction(self, production):
    return self.db_.executegetFilesByProduction(production)
  
  #############################################################################  
  """
  data insertation into the database
  """
  #############################################################################
  def file(self, fileName):
    gLoogger.warn("not implemented")
    return S_ERROR()
  
  #############################################################################
  def fileTypeAndFileTypeVersion(self, type, version):
    gLoogger.warn("not implemented")
    return S_ERROR()
  
  #############################################################################
  def eventType(self, eventTypeId):
    gLoogger.warn("not implemented")
    return S_ERROR()
  
  #############################################################################
  def insertJob(self, jobName, jobConfVersion, date):
    gLoogger.warn("not implemented")
    return S_ERROR()
  
  #############################################################################
  def insertJob(self, config, jobParams):
    gLoogger.warn("not implemented")
    return S_ERROR()
  
  #############################################################################
  def insertJobParameter(self, jobID, name, value, type):
    gLoogger.warn("not implemented")
    return S_ERROR()
  
  #############################################################################
  def insertInputFile(self, jobID, inputfile):
    gLoogger.warn("not implemented")
    return S_ERROR()
  
  #############################################################################
  def insertOutputFile(self, jobID, name, value):
    gLoogger.warn("not implemented")
    return S_ERROR()
  
  #############################################################################
  def insertOutputFile(self, job, file):
    gLoogger.warn("not implemented")
    return S_ERROR()
    
  #############################################################################
  def insertFileParam(self, fileID, name, value):
    gLoogger.warn("not implemented")
    return S_ERROR()
  
  #############################################################################
  def insertReplica(self, fileID, name, location):
    gLoogger.warn("not implemented")
    return S_ERROR()
  
  #############################################################################
  def deleteJob(self, job):
    gLoogger.warn("not implemented")
    return S_ERROR()
  
  #############################################################################
  def deleteFile(self, file):
    gLoogger.warn("not implemented")
    return S_ERROR()
  
  #############################################################################
  def insertQuality(self, fileID, group, flag ):
    gLoogger.warn("not implemented")
    return S_ERROR()
  
  #############################################################################
  def insertQualityParam(self, fileID, qualityID, name, value):
    gLoogger.warn("not implemented")
    return S_ERROR()
  
  #############################################################################
  def modifyReplica(self, fileID , name, value):
    gLoogger.warn("not implemented")
    return S_ERROR()
  
  #############################################################################
