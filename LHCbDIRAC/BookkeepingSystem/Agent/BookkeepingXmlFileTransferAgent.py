########################################################################
# $Id$
########################################################################

""" 

"""


AGENT_NAME = 'Bookkeeping/BookkeepingXmlFileTransferAgent'

from DIRAC.Core.Base.AgentModule                                                   import AgentModule
from DIRAC                                                                         import S_OK, S_ERROR, gConfig

from LHCbDIRAC.BookkeepingSystem.Agent.XMLReader.XMLFilesReaderManagerForTransfer  import XMLFilesReaderManagerForTransfer
#from LHCbDIRAC.BookkeepingSystem.Agent.XMLReader.Job.SimulationConditions          import SimulationConditions
#from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient                          import BookkeepingClient

__RCSID__ = "$Id$"

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
    #self.bkkClient_ = BookkeepingClient()
    self.xmlMgmt_ = XMLFilesReaderManagerForTransfer()
    return result

  #############################################################################
  def execute(self):
    self.log.info("Bookkeeping XML File transfer Agent running!!!")
    
    '''
    self.xmlMgmt_.initialize()
    jobs = self.xmlMgmt_.getJobs()
    
    for job in jobs:
      result = self.__translateJobAttributes(job)
      if result['OK']:
        name = job.getFileName().split("/")[5]
        self.log.info(job.writeToXML())
        self.log.info("Send "+str(name)+"to volhcb07!!")
        self.bkkClient_.filetransfer(name, job.writeToXML())
    
    replicas = self.xmlMgmt_.getReplicas()
    for replica in replicas:
      result = self.__translateReplicaAttributes(replica)
      if result['OK']:
        name = replicas.getFileName().split("/")[5]
        self.log.info("Send"+str(name)+"to volhcb07!!")
        self.bkkClient_.filetransfer(name, replica.writeToXML())
    
    self.xmlMgmt_.destroy()
    '''
    self.xmlMgmt_.directTransfer()
    self.log.info("Bookkeeping XML File transfer Agent finished!!!")
        
    return S_OK()
  
  