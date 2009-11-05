########################################################################
# $HeadURL$
########################################################################

__RCSID__ = "$Id$"

from DIRAC                                            import S_OK, S_ERROR, gConfig, gLogger, gMonitor
from DIRAC.LHCbSystem.Agent.BookkeepingWatchAgent     import BookkeepingWatchAgent


AGENT_NAME = 'ProductionManagement/BookkeepingWatchAgent'

class BookkeepingWatchAgent(BookkeepingWatchAgent):
  pass
