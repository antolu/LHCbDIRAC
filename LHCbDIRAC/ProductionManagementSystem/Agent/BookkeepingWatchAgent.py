########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/ProductionManagementSystem/Agent/BookkeepingWatchAgent.py,v 1.7 2009/09/14 21:05:52 acsmith Exp $
########################################################################

__RCSID__ = "$Id: BookkeepingWatchAgent.py,v 1.7 2009/09/14 21:05:52 acsmith Exp $"

from DIRAC                                            import S_OK, S_ERROR, gConfig, gLogger, gMonitor
from DIRAC.LHCbSystem.Agent.BookkeepingWatchAgent     import BookkeepingWatchAgent


AGENT_NAME = 'ProductionManagement/BookkeepingWatchAgent'

class BookkeepingWatchAgent(BookkeepingWatchAgent):
  pass
