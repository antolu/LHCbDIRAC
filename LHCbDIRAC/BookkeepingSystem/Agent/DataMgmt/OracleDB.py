########################################################################
# $Id: OracleDB.py,v 1.10 2008/04/22 11:21:02 zmathe Exp $
########################################################################

"""

"""

from DIRAC                 import gLogger, S_OK, S_ERROR
import cx_Oracle

__RCSID__ = "$Id: OracleDB.py,v 1.10 2008/04/22 11:21:02 zmathe Exp $"

class OracleDB:
  
  #############################################################################
  def __init__(self, userName, password = '', tnsEntry=''):
    self.userName_ = userName
    self.password_ = password
    self.tnsEntry_ = tnsEntry

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
      connection = self._createConnection()
      cursor = cx_Oracle.Cursor(connection)
      cursor.execute(sql,*params)
      results = cursor.fetchall()
      connection.close()
    except Exception, ex:
      gLogger.error(ex)
    return results
  
  #############################################################################
  def executeAviableEventNbCursor(self):
    results = None
    try:
      connection = self._createConnection()
      result = cx_Oracle.Cursor(connection)
      cursor = cx_Oracle.Cursor(connection)
      cursor.callproc('BKK.getAviableEventTypes', [result])
      results = result.fetchall()
      connection.close()
    except Exception, ex:
      gLogger.error(ex)    
    return results;
  
  #############################################################################
  def executeEventTypesCursor(self, configName, configVersion):
    results = None
    try:
      connection = self._createConnection()
      result = cx_Oracle.Cursor(connection)
      cursor = cx_Oracle.Cursor(connection)
      cursor.callproc('BKK.getEventTypes', [configName, configVersion, result])
      results = result.fetchall()
      connection.close()
    except Exception, ex:
      gLogger.error(ex)    
    return results;
  
  #############################################################################
  def executeFullEventTypeAndNumberCursor(self, configName, configVersion, eventTypeId):
    results = None
    try:
      connection = self._createConnection()
      result = cx_Oracle.Cursor(connection)
      cursor = cx_Oracle.Cursor(connection)
      cursor.callproc('BKK.getFullEventTypeAndNumber', [configName, configVersion, eventTypeId, result])
      results = result.fetchall()
      connection.close()
    except Exception, ex:
      gLogger.error(ex)    
    return results;
  
  #############################################################################
  def executeFullEventTypeAndNumberCursor1(self, configName, configVersion, fileType, eventTypeId):
    results = None
    try:
      connection = self._createConnection()
      result = cx_Oracle.Cursor(connection)
      cursor = cx_Oracle.Cursor(connection)
      cursor.callproc('BKK.getFullEventTypeAndNumber1', [configName, configVersion, fileType, eventTypeId, result])
      results = result.fetchall()
      connection.close()
    except Exception, ex:
      gLogger.error(ex)    
    return results;
  
  #############################################################################
  def executeGetFiles(self, configName, configVersion, fileType, eventTypeId, production):
    results = None
    try:
      connection = self._createConnection()
      result = cx_Oracle.Cursor(connection)
      cursor = cx_Oracle.Cursor(connection)
      cursor.callproc('BKK.getFiles', [configName, configVersion, fileType, eventTypeId, production, result])
      results = result.fetchall()
      connection.close()
    except Exception, ex:
      gLogger.error(ex)    
    return results;
  
  #############################################################################
  def executegetEventTypeAndNumber(self, fileType, eventTypeId):
    results = None
    try:
      connection = self._createConnection()
      result = cx_Oracle.Cursor(connection)
      cursor = cx_Oracle.Cursor(connection)
      cursor.callproc('BKK.getEventTypeAndNumber', [fileType, eventTypeId, result])
      results = result.fetchall()
      connection.close()
    except Exception, ex:
      gLogger.error(ex)    
    return results;
  
  #############################################################################
  def executegetAviableEventTypesAndNumberOfEvents(self, configName, configVersion, eventTypeId):
    results = None
    try:
      connection = self._createConnection()
      result = cx_Oracle.Cursor(connection)
      cursor = cx_Oracle.Cursor(connection)
      cursor.callproc('BKK.getEventTypeAndNumberAll', [configName, configVersion, eventTypeId, result])
      results = result.fetchall()
      connection.close()
    except Exception, ex:
      gLogger.error(ex)    
    return results;
  
  
