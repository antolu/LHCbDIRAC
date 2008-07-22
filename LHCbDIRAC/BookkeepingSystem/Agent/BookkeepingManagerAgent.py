########################################################################
# $Id: BookkeepingManagerAgent.py,v 1.27 2008/07/22 14:14:50 zmathe Exp $
########################################################################

""" 
BookkeepingManager agent process the ToDo directory and put the data to Oracle database.   
"""

__RCSID__ = "$Id: BookkeepingManagerAgent.py,v 1.27 2008/07/22 14:14:50 zmathe Exp $"

AGENT_NAME = 'Bookkeeping/BookkeepingManagerAgent'

from DIRAC.Core.Base.Agent                                                        import Agent
from DIRAC                                                                        import S_OK, S_ERROR
from DIRAC.BookkeepingSystem.Agent.XMLReader.XMLFilesReaderManager                import XMLFilesReaderManager
from DIRAC.BookkeepingSystem.DB.BookkeepingDatabaseClient                         import BookkeepingDatabaseClient
from DIRAC.BookkeepingSystem.Agent.XMLReader.Replica.Replica                      import Replica
from DIRAC.BookkeepingSystem.Agent.XMLReader.Replica.ReplicaParam                 import ReplicaParam
from DIRAC.ConfigurationSystem.Client.Config                                      import gConfig
from DIRAC.BookkeepingSystem.Agent.FileSystem.FileSystemClient                    import FileSystemClient
from DIRAC.DataManagementSystem.Client.Catalog.LcgFileCatalogCombinedClient       import LcgFileCatalogCombinedClient
import os

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
    result = Agent.initialize(self)
    
    self.xmlMgmt_ = XMLFilesReaderManager()
    self.dataManager_ =  BookkeepingDatabaseClient()
    self.fileClient_ = FileSystemClient()
    self.lcgFileCatalogClient_ = LcgFileCatalogCombinedClient()
    baseDir = gConfig.getValue(self.section+"/XMLProcessing", "/opt/bookkeeping/XMLProcessing/")
    self.done_ = baseDir + "Done/"
  
    return result

  #############################################################################
  def execute(self):
    self.log.info("BookkeepingAgent running!!!")
    
    self.xmlMgmt_.initialize()
    jobs = self.xmlMgmt_.getJobs()
    
    for job in jobs:
      result = self.xmlMgmt_.processJob(job, True)
      if result['OK']:
        self.__moveFileToDoneDirectory(job.getFileName())
    
    replicas = self.xmlMgmt_.getReplicas()
    for replica in replicas:
      result = self.xmlMgmt_.processReplicas(replica, True) 
      if result['OK']:
        self.__moveFileToDoneDirectory(replica.getFileName())
    
    self.xmlMgmt_.destroy()
    
    return S_OK()

  
  #############################################################################
  def __moveFileToDoneDirectory(self, fileName):
    self.log.info("Moving file to Done directory: ",fileName)
    name = os.path.split(fileName)[1]
    self.fileClient_.rename(fileName, self.done_+name)
        
      
              
      
    
  
