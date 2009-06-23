########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/ProductionManagementSystem/Agent/BookkeepingWatchAgent.py,v 1.5 2009/06/23 13:25:56 atsareg Exp $
########################################################################

"""  The Transformation Agent prepares production jobs for processing data
     according to transformation definitions in the Production database.
"""

__RCSID__ = "$Id: BookkeepingWatchAgent.py,v 1.5 2009/06/23 13:25:56 atsareg Exp $"

from DIRAC.Core.Base.Agent    import Agent
from DIRAC                    import S_OK, S_ERROR, gConfig, gLogger, gMonitor
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.ProductionManagementSystem.DB.ProductionDB import ProductionDB
import os, time, datetime


AGENT_NAME = 'ProductionManagement/BookkeepingWatchAgent'

class BookkeepingWatchAgent(Agent):

  #############################################################################
  def __init__(self):
    """ Standard constructor for Agent
    """
    Agent.__init__(self,AGENT_NAME)
    self.fileLog = {}
    self.timeLog = {}
    self.fullTimeLog = {}

  #############################################################################
  def initialize(self):
    """ Make the necessary initilizations
    """
    result = Agent.initialize(self)
    self.pollingTime = gConfig.getValue(self.section+'/PollingTime',120)

    self.prodDB = ProductionDB()
    gMonitor.registerActivity("Iteration","Agent Loops",self.name,"Loops/min",gMonitor.OP_SUM)
    return result

  ##############################################################################
  def execute(self):
    """ Main execution method
    """

    gMonitor.addMark('Iteration',1)
    server = RPCClient('ProductionManagement/ProductionManager')
    bkserver = RPCClient('Bookkeeping/BookkeepingManager')
    
    result = server.getAllProductions()
    
    activeTransforms = []
    if not result['OK']:
      gLogger.error("BookkeepingWatchAgent.execute: Failed to get productions.", result['Message'])
      return S_OK()

    for transDict in result['Value']:    
      transID = long(transDict['TransID'])
      transStatus = transDict['Status']
      bkQueryID = transDict['BkQueryID']
      
      if transStatus in ["Active"] and bkQueryID:
        result = server.getBookkeepingQuery(bkQueryID)
        if not result['OK']:
          gLogger.warn("BookkeepingWatchAgent.execute: Failed to get BkQuery", result['Message'])
          continue
          
        bkDict = result['Value']  
        
        # Make sure that default values are not passed and int values are converted to strings
        for name,value in bkDict.items():
          if name == "ProductionID" or name == "EventType" or name == "BkQueryID" :
            if value == 0:
              del bkDict[name]
            else:
              bkDict[name] = str(value)
          else:      
            if value.lower() == "all":
              del bkDict[name]
              
        start = time.time()              
        if self.timeLog.has_key(transID):
          if self.fullTimeLog.has_key(transID):
            # If it is more than a day since the last reduced query, make a full query just in case
            if (datetime.datetime.utcnow() - self.fullTimeLog[transID]) < datetime.timedelta(days=1):
              timeStamp = self.timeLog[transID]
              bkDict['StartDate'] = (timeStamp - datetime.timedelta(seconds=10)).strftime('%Y-%m-%d %H:%M:%S')
            else:
              self.fullTimeLog[transID] = datetime.datetime.utcnow()    
                 
        self.timeLog[transID] = datetime.datetime.utcnow()
        if not self.fullTimeLog.has_key(transID):
          self.fullTimeLog[transID] = datetime.datetime.utcnow()     
            
        result = bkserver.getFilesWithGivenDataSets(bkDict)    
        rtime = time.time()-start    
        gLogger.verbose('Bk query time: %.2f sec for query %d, transformation %d' % (rtime,bkQueryID,transID) )
        if not result['OK']:
          gLogger.error("BookkeepingWatchAgent.execute: Failed to get response from the Bookkeeping", result['Message'])
          continue
          
        lfnList = result['Value']   
        
        # Check if the number of files has changed since the last cycle
        nlfns = len(lfnList)
        gLogger.info('%d files returned for production %d from the BK DB' % (nlfns,int(transID)) )
        if self.fileLog.has_key(transID):
          if nlfns == self.fileLog[transID]:
            gLogger.verbose('No new files in BK DB since last check')
        #    continue
        self.fileLog[transID] = nlfns       
             
        if lfnList:
          gLogger.verbose('Processing %d lfns for transformation %d' % (len(lfnList),transID) )
          fileList = []
          for lfn in lfnList:
            fileList.append((lfn,'Unknown',0,'Unknown','0000',0))

          result = server.addFile(fileList,True)
          if not result['OK']:
            gLogger.error("BookkeepingWatchAgent.execute: Failed to add files", result['Message'])
            self.fileLog[transID] = 0
            continue  

          successfulList = result['Value']['Successful']
          failedList = result['Value']['Failed']
          if failedList:
            gLogger.warn("BookkeepingWatchAgent.execute: adding of %d files failed" % len(failedList), result['Message'])  
            self.fileLog[transID] = 0

          lfns = successfulList.keys() 
          gLogger.verbose('Adding %d lfns for transformation %d' % (len(lfns),transID) )
          result = server.addLFNsToTransformation(lfns,transID)
          if not result['OK']:
            gLogger.warn("BookkeepingWatchAgent.execute: failed to add lfns to transformation", result['Message'])   
            self.fileLog[transID] = 0
    
    return S_OK() 
