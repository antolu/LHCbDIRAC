"""interface for the database"""
########################################################################
# $Id: BookkeepingDatabaseClient.py 55878 2012-09-05 16:08:28Z zmathe $
########################################################################

from LHCbDIRAC.BookkeepingSystem.DB.IBookkeepingDatabaseClient             import IBookkeepingDatabaseClient
from LHCbDIRAC.BookkeepingSystem.DB.OracleBookkeepingDB                    import OracleBookkeepingDB

__RCSID__ = "$Id: BookkeepingDatabaseClient.py 55878 2012-09-05 16:08:28Z zmathe $"

class BookkeepingDatabaseClient( IBookkeepingDatabaseClient ):
  """simple class"""
  #############################################################################
  def __init__(self, databaseManager=OracleBookkeepingDB()):
    super(BookkeepingDatabaseClient, self).__init__(databaseManager)

