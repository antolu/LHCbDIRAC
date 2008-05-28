########################################################################
# $Id: BookkeepingDatabaseClient.py,v 1.1 2008/05/28 11:00:35 zmathe Exp $
########################################################################

"""

"""

from DIRAC.BookkeepingSystem.Agent.DataMgmt.IBookkeepingDatabaseClient import IBookkeepingDatabaseClient 
from DIRAC.BookkeepingSystem.Agent.DataMgmt.AMGABookkeepingDB          import AMGABookkeepingDB
from DIRAC                                                             import gLogger, S_OK, S_ERROR

__RCSID__ = "$Id: BookkeepingDatabaseClient.py,v 1.1 2008/05/28 11:00:35 zmathe Exp $"

class BookkeepingDatabaseClient(IBookkeepingDatabaseClient):
  
  #############################################################################
  def __init__(self, DatabaseManager = AMGABookkeepingDB()):
    super(BookkeepingDatabaseClient, self).__init__(DatabaseManager)
  