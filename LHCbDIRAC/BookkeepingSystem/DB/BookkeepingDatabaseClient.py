########################################################################
# $Id$
########################################################################

"""

"""

from LHCbDIRAC.BookkeepingSystem.DB.IBookkeepingDatabaseClient             import IBookkeepingDatabaseClient 
from LHCbDIRAC.BookkeepingSystem.DB.OracleBookkeepingDB                    import OracleBookkeepingDB
from DIRAC                                                                 import gLogger, S_OK, S_ERROR

__RCSID__ = "$Id$"

class BookkeepingDatabaseClient(IBookkeepingDatabaseClient):
  
  #############################################################################
  def __init__(self, DatabaseManager = OracleBookkeepingDB()):
    super(BookkeepingDatabaseClient, self).__init__(DatabaseManager)
  