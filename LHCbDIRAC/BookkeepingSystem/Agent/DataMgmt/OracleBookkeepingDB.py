########################################################################
# $Id: OracleBookkeepingDB.py,v 1.2 2008/04/18 13:38:50 zmathe Exp $
########################################################################
"""

"""

__RCSID__ = "$Id: OracleBookkeepingDB.py,v 1.2 2008/04/18 13:38:50 zmathe Exp $"

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
    self.user_ = gConfig.getValue("userName", "LHCB_BOOKKEEPING_INT")
    self.password_ = gConfig.getValue("password", Ginevra2008)
    self.tns_ = gConfig.getValue("tns", "int12r")
   
    self.db_ = OracleDB(self.user_, self.password_, self.tns_)
  
  #############################################################################
  def getAviableConfigNameAndVersion(self):
    """
    """
    return db_.execute('select distinct dir3."user:ConfigName", dir3."user:ConfigVersion" from dir3')
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
