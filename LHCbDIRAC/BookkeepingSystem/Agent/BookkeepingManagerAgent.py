########################################################################
# $Id$
########################################################################

""" 
BookkeepingManager agent process the ToDo directory and put the data to Oracle database.   
"""

__RCSID__ = "$Id$"

AGENT_NAME = 'Bookkeeping/BookkeepingManagerAgent'

from LHCbDIRAC.BookkeepingSystem.Agent.XMLReader.XMLFilesReaderManager                import XMLFilesReaderManager
from LHCbDIRAC.BookkeepingSystem.DB.BookkeepingDatabaseClient                         import BookkeepingDatabaseClient
from LHCbDIRAC.BookkeepingSystem.Agent.XMLReader.Replica.Replica                      import Replica
from LHCbDIRAC.BookkeepingSystem.Agent.XMLReader.Replica.ReplicaParam                 import ReplicaParam
from LHCbDIRAC.BookkeepingSystem.Agent.FileSystem.FileSystemClient                    import FileSystemClient

from DIRAC.DataManagementSystem.Client.Catalog.LcgFileCatalogCombinedClient           import LcgFileCatalogCombinedClient
from DIRAC.ConfigurationSystem.Client.Config                                          import gConfig
from DIRAC.Core.Base.Agent                                                            import Agent
from DIRAC                                                                            import S_OK, S_ERROR

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
    #self.dataManager_ =  BookkeepingDatabaseClient()
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
      else:
        self.log.error(result['Message'])
    
    replicas = self.xmlMgmt_.getReplicas()
    for replica in replicas:
      result = self.xmlMgmt_.processReplicas(replica, True) 
      if result['OK']:
        self.__moveFileToDoneDirectory(replica.getFileName())
      else:
        self.log.error(result['Message'])
    
    self.xmlMgmt_.destroy()
    
    return S_OK()

  
  #############################################################################
  def __moveFileToDoneDirectory(self, fileName):
    self.log.info("Moving file to Done directory: ",fileName)
    name = os.path.split(fileName)[1]
    self.fileClient_.rename(fileName, self.done_+name)
        
      
              
      
    
  
