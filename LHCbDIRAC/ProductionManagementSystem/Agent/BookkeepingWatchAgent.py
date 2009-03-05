########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/ProductionManagementSystem/Agent/BookkeepingWatchAgent.py,v 1.3 2009/03/05 15:11:59 atsareg Exp $
########################################################################

"""  The Transformation Agent prepares production jobs for processing data
     according to transformation definitions in the Production database.
"""

__RCSID__ = "$Id: BookkeepingWatchAgent.py,v 1.3 2009/03/05 15:11:59 atsareg Exp $"

from DIRAC.Core.Base.Agent    import Agent
from DIRAC                    import S_OK, S_ERROR, gConfig, gLogger, gMonitor
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.ProductionManagementSystem.DB.ProductionDB import ProductionDB
import os, time


AGENT_NAME = 'ProductionManagement/BookkeepingWatchAgent'

class BookkeepingWatchAgent(Agent):

  #############################################################################
  def __init__(self):
    """ Standard constructor for Agent
    """
    Agent.__init__(self,AGENT_NAME)

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
      
      if transStatus in ["Active","Stopped","Completed"] and bkQueryID:
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
        result = bkserver.getFilesWithGivenDataSets(bkDict)    
        rtime = time.time()-start    
        gLogger.verbose('Bk query time: %.2f sec for query %d, transformation %d' % (rtime,bkQueryID,transID) )
        if not result['OK']:
          gLogger.error("BookkeepingWatchAgent.execute: Failed to get response from the Bookkeeping", result['Message'])
          continue
          
        lfnList = result['Value']        
        if lfnList:
          gLogger.verbose('Processing %d lfns for transformation %d' % (len(lfnList),transID) )
          fileList = []
          for lfn in lfnList:
            fileList.append((lfn,'Unknown',0,'Unknown','0000',0))

          result = server.addFile(fileList,True)
          if not result['OK']:
            gLogger.error("BookkeepingWatchAgent.execute: Failed to add files", result['Message'])
            continue  

          successfulList = result['Value']['Successful']
          failedList = result['Value']['Failed']
          if failedList:
            gLogger.warn("BookkeepingWatchAgent.execute: adding of %d files failed" % len(failedList), result['Message'])  

          lfns = successfulList.keys() 
          gLogger.verbose('Adding %d lfns for transformation %d' % (len(lfns),transID) )
          result = server.addLFNsToTransformation(lfns,transID)
          if not result['OK']:
            gLogger.warn("BookkeepingWatchAgent.execute: failed to add lfns to transformation", result['Message'])   
    
    return S_OK() 
