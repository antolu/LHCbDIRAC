########################################################################
# $Id: OracleDB.py,v 1.22 2008/06/05 13:31:10 zmathe Exp $
########################################################################

"""

"""

from DIRAC                 import gLogger, S_OK, S_ERROR
import cx_Oracle

__RCSID__ = "$Id: OracleDB.py,v 1.22 2008/06/05 13:31:10 zmathe Exp $"

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
  