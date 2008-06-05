########################################################################
# $Id: OracleDB.py,v 1.21 2008/06/05 13:26:57 zmathe Exp $
########################################################################

"""

"""

from DIRAC                 import gLogger, S_OK, S_ERROR
import cx_Oracle

__RCSID__ = "$Id: OracleDB.py,v 1.21 2008/06/05 13:26:57 zmathe Exp $"

class OracleDB:
  
  #############################################################################
  def __init__(self, userName, password = '', tnsEntry=''):
    self.userName_ = userName
    self.password_ = password
    self.tnsEntry_ = tnsEntry
    self.connection_ = self._createConnection()

  #############################################################################
  def _createConnection(self):
    try:
      return cx_Oracle.Connection(self.userName_, self.password_, self.tnsEntry_)
    except Exception, ex:
      gLogger.error(ex)
    return None    
  
  #############################################################################
  def execute(self, sql, *params):
    results = None
    try:
      cursor = cx_Oracle.Cursor(self.connection_)
      cursor.execute(sql,*params)
      results = cursor.fetchall()
    except Exception, ex:
      gLogger.error(ex)
    return results
  
  #############################################################################
  def executeStoredProcedure(self, packageName, parameters):
    results = None
    try:
      result = cx_Oracle.Cursor(self.connection_)
      cursor = cx_Oracle.Cursor(self.connection_)
      parameters +=[result]
      cursor.callproc(packageName, parameters)
      results = result.fetchall()
    except Exception, ex:
      gLogger.error(ex)    
    return results
  #############################################################################
  """
  def executeAviableEventNbCursor(self):
    results = None
    try:
      result = cx_Oracle.Cursor(self.connection_)
      cursor = cx_Oracle.Cursor(self.connection_)
      cursor.callproc('BKK_ORACLE.getAviableEventTypes', [result])
      results = result.fetchall()
    except Exception, ex:
      gLogger.error(ex)    
    return results;
  
  #############################################################################
  def executeFullEventTypeAndNumberCursor(self, configName, configVersion, eventTypeId):
    results = None
    try:
      result = cx_Oracle.Cursor(self.connection_)
      cursor = cx_Oracle.Cursor(self.connection_)
      cursor.callproc('BKK_ORACLE.getEventTypeAndNumberAll', [configName, configVersion, eventTypeId, result])
      #cursor.callproc('BKK_ORACLE.getFullEventTypeAndNumber', [configName, configVersion, eventTypeId, result])
      results = result.fetchall()
    except Exception, ex:
      gLogger.error(ex)    
    return results;
  
  #############################################################################
  def executeFullEventTypeAndNumberCursor1(self, configName, configVersion, fileType, eventTypeId):
    results = None
    try:
      result = cx_Oracle.Cursor(self.connection_)
      cursor = cx_Oracle.Cursor(self.connection_)
      cursor.callproc('BKK_ORACLE.getFullEventTypeAndNumber1', [configName, configVersion, fileType, eventTypeId, result])
      results = result.fetchall()
    except Exception, ex:
      gLogger.error(ex)    
    return results;
  
  #############################################################################
  def executeGetFiles(self, configName, configVersion, fileType, eventTypeId, production):
    results = None
    try:
      result = cx_Oracle.Cursor(self.connection_)
      cursor = cx_Oracle.Cursor(self.connection_)
      cursor.callproc('BKK_ORACLE.getFiles', [configName, configVersion, fileType, eventTypeId, production, result])
      results = result.fetchall()
    except Exception, ex:
      gLogger.error(ex)    
    return results;
  
  def executeGetSpecificFiles(self, configName, configVersion, programName, programVersion, fileType, eventTypeId, production):
    results = None
    try:
      result = cx_Oracle.Cursor(self.connection_)
      cursor = cx_Oracle.Cursor(self.connection_)
      cursor.callproc('BKK_ORACLE.getSpecificFiles', [configName, configVersion,  programName, programVersion, fileType, eventTypeId, production, result])
      results = result.fetchall()
    except Exception, ex:
      gLogger.error(ex)    
    return results;
  
  #############################################################################
  def executegetEventTypeAndNumber(self, fileType, eventTypeId):
    results = None
    try:
      result = cx_Oracle.Cursor(self.connection_)
      cursor = cx_Oracle.Cursor(self.connection_)
      cursor.callproc('BKK_ORACLE.getEventTypeAndNumber', [fileType, eventTypeId, result])
      results = result.fetchall()
    except Exception, ex:
      gLogger.error(ex)    
    return results;
  
  #############################################################################
  def executegetAviableEventTypesAndNumberOfEvents(self, configName, configVersion, eventTypeId):
    results = None
    try:
      result = cx_Oracle.Cursor(self.connection_)
      cursor = cx_Oracle.Cursor(self.connection_)
      cursor.callproc('BKK_ORACLE.getEventTypeAndNumberAll', [configName, configVersion, eventTypeId, result])
      results = result.fetchall()
    except Exception, ex:
      gLogger.error(ex)    
    return results;
  
  #############################################################################
  def executegetProcessingPass(self):
    results = None
    try:
      result = cx_Oracle.Cursor(self.connection_)
      cursor = cx_Oracle.Cursor(self.connection_)
      cursor.callproc('BKK_ORACLE.getProcessingPass', [result])
      results = result.fetchall()
    except Exception, ex:
      gLogger.error(ex)    
    return results;
    
  #############################################################################  
  
  def executegetProductions(self, processingPass):
    results = None
    try:
      result = cx_Oracle.Cursor(self.connection_)
      cursor = cx_Oracle.Cursor(self.connection_)
      cursor.callproc('BKK_ORACLE.getProductions', [processingPass, result])
      results = result.fetchall()
    except Exception, ex:
      gLogger.error(ex)    
    return results;
  """
  """
  #############################################################################  
  def executegetFilesByProduction(self, production):
    results = None
    try:
      result = cx_Oracle.Cursor(self.connection_)
      cursor = cx_Oracle.Cursor(self.connection_)
      cursor.callproc('BKK_ORACLE.getFilesByProduction', [production, result])
      results = result.fetchall()
    except Exception, ex:
      gLogger.error(ex)    
    return results;
  """
  #############################################################################  
  
