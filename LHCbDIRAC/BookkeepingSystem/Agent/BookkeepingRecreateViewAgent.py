########################################################################
# $Id: BookkeepingRecreateViewAgent.py,v 1.1 2008/03/04 11:40:54 zmathe Exp $
########################################################################

""" 

"""

__RCSID__ = "$Id: BookkeepingRecreateViewAgent.py,v 1.1 2008/03/04 11:40:54 zmathe Exp $"

AGENT_NAME = 'Bookkeeping/BookkeepingRecreateViewAgent'

from DIRAC.Core.Base.Agent                                                import Agent
from DIRAC                                                                import S_OK, S_ERROR

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
    result = Agent.initialize(self)
    
    return result

  #############################################################################
  def execute(self):
    self.log.info("Bookkeeping Recreate View Agent running!!!")
        
    return S_OK()
