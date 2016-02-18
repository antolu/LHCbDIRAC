"""interface for the database"""

from LHCbDIRAC.BookkeepingSystem.DB.IBookkeepingDatabaseClient             import IBookkeepingDatabaseClient
from LHCbDIRAC.BookkeepingSystem.DB.OracleBookkeepingDB                    import OracleBookkeepingDB

__RCSID__ = "$Id$"

class BookkeepingDatabaseClient( IBookkeepingDatabaseClient ):
  """simple class"""
  #############################################################################
  def __init__(self, databaseManager=OracleBookkeepingDB()):
    super(BookkeepingDatabaseClient, self).__init__(databaseManager)

