########################################################################
# $Id: BookkeepingRecreateViewAgent.py,v 1.2 2008/03/04 17:25:34 zmathe Exp $
########################################################################

""" 

"""

__RCSID__ = "$Id: BookkeepingRecreateViewAgent.py,v 1.2 2008/03/04 17:25:34 zmathe Exp $"

AGENT_NAME = 'Bookkeeping/BookkeepingRecreateViewAgent'

from DIRAC.Core.Base.Agent                                                import Agent
from DIRAC                                                                import S_OK, S_ERROR, gConfig

class BookkeepingRecreateViewAgent(Agent):

  #############################################################################
  def __init__(self):
    """ Constructor, takes system flag as argument.
    """
    Agent.__init__(self, AGENT_NAME)

  #############################################################################
  def initialize(self):
    """Initialize specific parameters for BookkeepingManageAgent.
    """
    result           = Agent.initialize(self)
    self.pollingTime = gConfig.getValue(self.section+'/PollingTime', 70)
    return result

  #############################################################################
  def execute(self):
    self.log.info("Bookkeeping Recreate View Agent running!!!")
        
    return S_OK()
