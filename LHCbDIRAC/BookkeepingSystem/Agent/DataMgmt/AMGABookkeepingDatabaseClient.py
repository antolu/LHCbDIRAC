########################################################################
# $Id: AMGABookkeepingDatabaseClient.py,v 1.1 2008/02/29 11:52:24 zmathe Exp $
########################################################################

"""

"""

from DIRAC.BookkeepingSystem.Agent.DataMgmt.IBookkeepingDatabaseClient import IBookkeepingDatabaseClient 
from DIRAC.BookkeepingSystem.Agent.DataMgmt.AMGABookkeepingDB          import AMGABookkeepingDB
from DIRAC                                                             import gLogger, S_OK, S_ERROR

__RCSID__ = "$Id: AMGABookkeepingDatabaseClient.py,v 1.1 2008/02/29 11:52:24 zmathe Exp $"

class AMGABookkeepingDatabaseClient(IBookkeepingDatabaseClient):
  
  #############################################################################
  def __init__(self, DatabaseManager = AMGABookkeepingDB()):
    super(AMGABookkeepingDatabaseClient, self).__init__(DatabaseManager)
  