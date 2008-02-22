########################################################################
# $Id: BookkeepingManagerAgent.py,v 1.1 2008/02/22 17:21:22 zmathe Exp $
########################################################################

""" 
BookkeepingManager agent process the ToDo directory and put the data to Oracle database.   
"""

from DIRAC.Core.Base.Agent                                 import Agent
from DIRAC                                                 import S_OK, S_ERROR

__RCSID__ = "$Id: BookkeepingManagerAgent.py,v 1.1 2008/02/22 17:21:22 zmathe Exp $"

AGENT_NAME = 'BookkeepingSystem/BookkeepingManagerAgent'

class BookkeepingManagerAgent(Agent):

  #############################################################################
  def __init__(self):
    """ Constructor, takes system flag as argument.
    """
    Agent.__init__(self, AGENT_NAME)
    

  #############################################################################
  def initialize(self):
    """Initialize specific parameters for BookkeepingManageAgent.
    """
    pass

  #############################################################################
  def execute(self):
    self.log.info("BookkeepingAgent running!!!")
    
  #EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#