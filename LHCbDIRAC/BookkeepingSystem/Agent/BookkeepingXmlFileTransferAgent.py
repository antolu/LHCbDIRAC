########################################################################
# $Id: BookkeepingXmlFileTransferAgent.py,v 1.1 2008/07/01 08:58:51 zmathe Exp $
########################################################################

""" 

"""


AGENT_NAME = 'Bookkeeping/BookkeepingXmlFileTransferAgent'

from DIRAC.Core.Base.Agent                                                import Agent
from DIRAC                                                                import S_OK, S_ERROR, gConfig
from DIRAC.BookkeepingSystem.Agent.XMLReader.XMLFilesReaderManager        import XMLFilesReaderManager

__RCSID__ = "$Id: BookkeepingXmlFileTransferAgent.py,v 1.1 2008/07/01 08:58:51 zmathe Exp $"

class BookkeepingXmlFileTransferAgent(Agent):

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
    self.pollingTime = gConfig.getValue(self.section+'/PollingTime', 60)
    return result

  #############################################################################
  def execute(self):
    self.log.info("Bookkeeping XML File transfer Agent running!!!")
    
    self.xmlMgmt_.initialize()
    jobs = self.xmlMgmt_.getJobs()
    
    for job in jobs:
      pass
    replicas = self.xmlMgmt_.getReplicas()
    for replica in replicas:
      pass
    
    self.xmlMgmt_.destroy()
    
   
    self.log.info("Bookkeeping XML File transfer Agent finished!!!")
        
    return S_OK()
