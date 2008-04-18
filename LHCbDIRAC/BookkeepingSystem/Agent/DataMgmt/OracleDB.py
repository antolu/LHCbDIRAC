########################################################################
# $Id: OracleDB.py,v 1.3 2008/04/18 14:39:18 zmathe Exp $
########################################################################

"""

"""

from DIRAC                 import gLogger, S_OK, S_ERROR
import cx_Oracle

__RCSID__ = "$Id: OracleDB.py,v 1.3 2008/04/18 14:39:18 zmathe Exp $"

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
  def executeCursor(self, name, parameters):
    results = None
    try:
      connection = self._createConnection()
      result = cx_Oracle.Cursor(connection)
      cursor = cx_Oracle.Cursor(connection)
      cursor.callproc(name, parameters)
      results = result.fetchall()
      connection.close()
    except Exception, ex:
      gLogger.error(ex)    
    return results;

  
