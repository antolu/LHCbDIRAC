########################################################################
# $Id: BookkeepingXmlFileTransferAgent.py,v 1.9 2008/07/02 15:51:14 zmathe Exp $
########################################################################

""" 

"""


AGENT_NAME = 'Bookkeeping/BookkeepingXmlFileTransferAgent'

from DIRAC.Core.Base.Agent                                                     import Agent
from DIRAC                                                                     import S_OK, S_ERROR, gConfig
from DIRAC.BookkeepingSystem.Agent.XMLReader.XMLFilesReaderManagerForTransfer  import XMLFilesReaderManagerForTransfer
from DIRAC.BookkeepingSystem.Agent.XMLReader.Job.SimulationConditions          import SimulationConditions
from DIRAC.BookkeepingSystem.Client.BookkeepingClient                          import BookkeepingClient

__RCSID__ = "$Id: BookkeepingXmlFileTransferAgent.py,v 1.9 2008/07/02 15:51:14 zmathe Exp $"

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
        self.log.info(job.writeToXML())
        self.log.info("Send "+str(name)+"to volhcb07!!")
        self.bkkClient_.sendBookkeeping(name, job.writeToXML())
    
    replicas = self.xmlMgmt_.getReplicas()
    for replica in replicas:
      result = self.__translateReplicaAttributes(replica)
      if result['OK']:
        name = replicas.getFileName().split("/")[5]
        self.log.info("Send"+str(name)+"to volhcb07!!")
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

 

    fileattr = {'EVENTSTAT':'EventStat', 
                'EVENTTYPE':'EventTypeId' , 
                'LOGNAME':'FileName', 
                'GOT_REPLICA':'GotReplica',
                'GUID':'Guid',
                'MD5SUM':'MD5Sum', 
                'SIZE':'FileSize' }

    
    configs = job.getJobConfiguration()
    if configs.getConfigName() == 'DC06':
      configs.setConfigName('MC')
      configs.setConfigVersion('2008')
      
      for param in  job.getJobParams():
        name = param.getName()
        if attrlist.has_key(name.upper()):
          param.setName(attrlist[name.upper()])
        else:
          del param
      
      for file in job.getJobOutputFiles():
        params = file.getFileParams()
        for param in params:
          name = param.getParamName()
          if fileattr.has_key(name.upper()):
            param.setParamName(fileattr[name.upper()])
      
      sim = SimulationConditions()
      sim.addParam("BeamCond", "Collisions")
      sim.addParam("BeamEnergy" ,"7 TeV")
      sim.addParam("Generator", "Pythia 6.325.2")
      sim.addParam("MagneticField", "-100%")
      sim.addParam("DetectorCond", "Normal")
      sim.addParam("Luminosity", "Fixed 2 10**32")
      job.addSimulationCond(sim)  
    else:
      return S_ERROR()
    return S_OK()
  
  #############################################################################
  def __translateReplicaAttributes(self, replica):
    return S_ERROR()