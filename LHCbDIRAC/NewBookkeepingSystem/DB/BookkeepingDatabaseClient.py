########################################################################
# $Id$
########################################################################

"""

"""

from LHCbDIRAC.NewBookkeepingSystem.DB.IBookkeepingDatabaseClient             import IBookkeepingDatabaseClient
from LHCbDIRAC.NewBookkeepingSystem.DB.OracleBookkeepingDB                    import OracleBookkeepingDB

__RCSID__ = "$Id$"

class BookkeepingDatabaseClient( IBookkeepingDatabaseClient ):

  #############################################################################
  def __init__( self, DatabaseManager = OracleBookkeepingDB() ):
    super( BookkeepingDatabaseClient, self ).__init__( DatabaseManager )
