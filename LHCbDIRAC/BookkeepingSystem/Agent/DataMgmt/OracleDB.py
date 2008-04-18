########################################################################
# $Id: OracleDB.py,v 1.1 2008/04/18 12:17:50 zmathe Exp $
########################################################################

"""

"""

from DIRAC                 import gLogger, S_OK, S_ERROR
import cx_Oracle

__RCSID__ = "$Id: OracleDB.py,v 1.1 2008/04/18 12:17:50 zmathe Exp $"

class OracleDB:
  
  #############################################################################
  def __init__(self, userName, password = '', tnsEntry=''):
    self.userName_ = userName
    self.password_ = password
    self.tnsEntry_ = tnsEntry
    self.db_ = None
    
    try:
      self.db_ = self.cx_Oracle.Connection(self.userName_, self.password_, self.tnsEntry_)
    except Exception, ex:
      gLogger.error(ex)    
  
  #############################################################################
  def getDataBase(self):
    return self.db_
  
  #############################################################################
  