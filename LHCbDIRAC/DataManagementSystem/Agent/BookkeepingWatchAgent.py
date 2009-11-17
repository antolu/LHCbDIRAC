########################################################################
# $HeadURL: svn+ssh://svn.cern.ch/reps/dirac/DIRAC/trunk/DIRAC/DataManagementSystem/Agent/BookkeepingWatchAgent.py $
########################################################################

__RCSID__ = "$Id: BookkeepingWatchAgent.py 18192 2009-11-11 16:51:47Z paterson $"

from DIRAC                                            import S_OK, S_ERROR, gConfig, gLogger, gMonitor
from LHCbDIRAC.LHCbSystem.Agent.BookkeepingWatchAgent     import BookkeepingWatchAgent


AGENT_NAME = 'DataManagement/BookkeepingWatchAgent'

class BookkeepingWatchAgent(BookkeepingWatchAgent):
  pass
