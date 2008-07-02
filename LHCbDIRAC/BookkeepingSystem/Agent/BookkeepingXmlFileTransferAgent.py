########################################################################
# $Id: BookkeepingXmlFileTransferAgent.py,v 1.2 2008/07/02 09:49:11 zmathe Exp $
########################################################################

""" 

"""


AGENT_NAME = 'Bookkeeping/BookkeepingXmlFileTransferAgent'

from DIRAC.Core.Base.Agent                                                import Agent
from DIRAC                                                                import S_OK, S_ERROR, gConfig
from DIRAC.BookkeepingSystem.Agent.XMLReader.XMLFilesReaderManager        import XMLFilesReaderManager
from DIRAC.BookkeepingSystem.Client.BookkeepingClient                     import BookkeepingClient

__RCSID__ = "$Id: BookkeepingXmlFileTransferAgent.py,v 1.2 2008/07/02 09:49:11 zmathe Exp $"

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
    self.bkkClient_ = BookkeepingClient()
    return result

  #############################################################################
  def execute(self):
    self.log.info("Bookkeeping XML File transfer Agent running!!!")
    
    self.xmlMgmt_.initialize()
    jobs = self.xmlMgmt_.getJobs()
    
    for job in jobs:
      result = self.__translateJobAttributes(job)
      if result['Ok']:
        name = job.getFileName().split("/")[5]
        gLogger.info("Send"+str(name)+"to volhcb07!!")
        self.bkkClient_.sendBookkeeping(name, result['Value'])
    
    replicas = self.xmlMgmt_.getReplicas()
    for replica in replicas:
      result = self.__translateReplicaAttributes(replica)
      if result['Ok']:
        name = replicas.getFileName().split("/")[5]
        gLogger.info("Send"+str(name)+"to volhcb07!!")
        self.bkkClient_.sendBookkeeping(name, result['Value'])
    
    self.xmlMgmt_.destroy()
    
   
    self.log.info("Bookkeeping XML File transfer Agent finished!!!")
        
    return S_OK()
  
  #############################################################################
  def __translateJobAttributes(self, job):
    configs = jobs.getJobConfiguration()
    if configs.getConfigName() == 'DC06':
      gLogger.info("Send file to VOOOOOO")
    else:
      return S_ERROR()
  
  #############################################################################
  def isOld(self):
    pass
  
  #############################################################################
  def __translateReplicaAttributes(self, replica):
    return S_ERROR()