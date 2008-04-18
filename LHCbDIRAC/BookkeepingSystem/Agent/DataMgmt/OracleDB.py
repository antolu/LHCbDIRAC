########################################################################
# $Id: OracleDB.py,v 1.2 2008/04/18 12:56:04 zmathe Exp $
########################################################################

"""

"""

from DIRAC                 import gLogger, S_OK, S_ERROR
import cx_Oracle

__RCSID__ = "$Id: OracleDB.py,v 1.2 2008/04/18 12:56:04 zmathe Exp $"

class OracleDB:
  
  #############################################################################
  def __init__(self, userName, password = '', tnsEntry=''):
    self.userName_ = userName
    self.password_ = password
    self.tnsEntry_ = tnsEntry
    self.db_ = None
    self.cursor_ = None
    
    try:
      self.db_ = cx_Oracle.Connection(self.userName_, self.password_, self.tnsEntry_)
      self.cursor_ = cx_Oracle.Cursor(self.db_)
    except Exception, ex:
      gLogger.error(ex)    
  
  #############################################################################
  def getDataBase(self):
    return self.db_
  
  #############################################################################
  def execute(self, sql, *params):
    results = None
    try:
      self.cursor_.execute(sql,*params)
      results = self.cursor_.fetchall()
    except Exception, ex:
      gLogger.error(ex)
    return results
  
