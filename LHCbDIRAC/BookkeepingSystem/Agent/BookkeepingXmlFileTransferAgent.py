########################################################################
# $Id: BookkeepingXmlFileTransferAgent.py,v 1.6 2008/07/02 14:22:00 zmathe Exp $
########################################################################

""" 

"""


AGENT_NAME = 'Bookkeeping/BookkeepingXmlFileTransferAgent'

from DIRAC.Core.Base.Agent                                                     import Agent
from DIRAC                                                                     import S_OK, S_ERROR, gConfig
from DIRAC.BookkeepingSystem.Agent.XMLReader.XMLFilesReaderManagerForTransfer  import XMLFilesReaderManagerForTransfer
from DIRAC.BookkeepingSystem.Client.BookkeepingClient                          import BookkeepingClient

__RCSID__ = "$Id: BookkeepingXmlFileTransferAgent.py,v 1.6 2008/07/02 14:22:00 zmathe Exp $"

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
    self.xmlMgmt_ = XMLFilesReaderManagerForTransfer()
    return result

  #############################################################################
  def execute(self):
    self.log.info("Bookkeeping XML File transfer Agent running!!!")
    
    self.xmlMgmt_.initialize()
    jobs = self.xmlMgmt_.getJobs()
    
    for job in jobs:
      result = self.__translateJobAttributes(job)
      if result['OK']:
        name = job.getFileName().split("/")[5]
        gLogger.info("Send"+str(name)+"to volhcb07!!")
        self.bkkClient_.sendBookkeeping(name, result['Value'])
    
    replicas = self.xmlMgmt_.getReplicas()
    for replica in replicas:
      result = self.__translateReplicaAttributes(replica)
      if result['OK']:
        name = replicas.getFileName().split("/")[5]
        gLogger.info("Send"+str(name)+"to volhcb07!!")
        self.bkkClient_.sendBookkeeping(name, result['Value'])
    
    self.xmlMgmt_.destroy()
    
   
    self.log.info("Bookkeeping XML File transfer Agent finished!!!")
        
    return S_OK()
  
  #############################################################################
  def __translateJobAttributes(self, job):

    attrlist = { 'DIRAC_JOBID':'DiracJobId', \
                 'DIRAC_VERSION':'DiracVersion', \
                 'EXECTIME':'ExecTime', \
                 'XMLDDDBVERSION':'GeometryVersion', \
                 'EDG_WL_JOBID':'GridJobId', \
                 'JOBDATE':'JobStart', \
                 'LOCALJOBID':'LocalJobId', \
                 'LOCATION':'Location', \
                 'NAME':'Name', \
                 'NUMBEROFEVENTS':'NumberOfEvents', \
                 'PRODUCTION':'Production', \
                 'PROGRAMNAME':'ProgramName', \
                 'PROGRAMVERSION':'ProgramVersion', \
                 'STATISTICSREQUESTED':'StatisticsRequested', \
                 'CPU':'WNCPUPower', \
                 'CPUTIME':'WNCPUTime', \
                 'CACHE':'WNCache', \
                 'MEMORY':'WNMemory', \
                 'MODEL':'WNModel', \
                 'HOST':'WorkerNode' }

    
    configs = job.getJobConfiguration()
    if configs.getConfigName() == 'DC06':
      configs.setConfigName('MC')
      configs.setConfigVersion('2008')
      self.log.info(job.writeToXML())
    else:
      return S_ERROR()
    return S_ERROR()
  
  #############################################################################
  def __translateReplicaAttributes(self, replica):
    return S_ERROR()