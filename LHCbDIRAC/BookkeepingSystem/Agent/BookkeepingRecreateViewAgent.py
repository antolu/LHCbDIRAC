########################################################################
# $Id$
########################################################################

""" 

"""

__RCSID__ = "$Id$"

AGENT_NAME = 'Bookkeeping/BookkeepingRecreateViewAgent'

from DIRAC.Core.Base.Agent                                                import Agent
from DIRAC                                                                import S_OK, S_ERROR, gConfig
import recreateAMGAViews

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
    self.pollingTime = gConfig.getValue(self.section+'/PollingTime', 14400)
    return result

  #############################################################################
  def execute(self):
    self.log.info("Bookkeeping Recreate View Agent running!!!")
   
    self.log.info("Recreation viwews started!")
    recreateAMGAViews.execute()
    self.log.info("Recreation views fhinished!")
        
    return S_OK()
