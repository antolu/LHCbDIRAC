########################################################################
# $Id: BookkeepingDatabaseClient.py,v 1.1 2008/06/24 11:29:23 zmathe Exp $
########################################################################

"""

"""

from DIRAC.BookkeepingSystem.DB.IBookkeepingDatabaseClient             import IBookkeepingDatabaseClient 
from DIRAC.BookkeepingSystem.DB.OracleBookkeepingDB                    import OracleBookkeepingDB
from DIRAC                                                             import gLogger, S_OK, S_ERROR

__RCSID__ = "$Id: BookkeepingDatabaseClient.py,v 1.1 2008/06/24 11:29:23 zmathe Exp $"

class BookkeepingDatabaseClient(IBookkeepingDatabaseClient):
  
  #############################################################################
  def __init__(self, DatabaseManager = OracleBookkeepingDB()):
    super(BookkeepingDatabaseClient, self).__init__(DatabaseManager)
  