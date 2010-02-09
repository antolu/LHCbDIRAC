########################################################################
# $HeadURL$
########################################################################

__RCSID__ = "$Id$"

from DIRAC                                                          import S_OK, S_ERROR, gConfig, gLogger, gMonitor
from DIRAC.Core.Base.AgentModule                                    import AgentModule
from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient           import BookkeepingClient
from LHCbDIRAC.TransformationSystem.Client.TransformationDBClient   import TransformationDBClient
import os, time, datetime

AGENT_NAME = 'ProductionSystem/BookkeepingWatchAgent'

class BookkeepingWatchAgent(AgentModule):

  #############################################################################
  def initialize(self):
    """ Make the necessary initializations """
    self.fileLog = {}
    self.timeLog = {}
    self.fullTimeLog = {}
    self.pollingTime = self.am_getOption('PollingTime',120)
    self.fullUpdatePeriod = self.am_getOption('FullUpdatePeriod',86400)
    gMonitor.registerActivity("Iteration","Agent Loops",AGENT_NAME,"Loops/min",gMonitor.OP_SUM)
    self.transClient = TransformationDBClient('TransformationDB')
    self.transClient.setServer('ProductionManagement/ProductionManager')
    # Create the BK client
    self.bkClient = BookkeepingClient()
    return S_OK()

  ##############################################################################
  def execute(self):
    """ Main execution method
    """

    gMonitor.addMark('Iteration',1)
    # Get all the transformations
    result = self.transClient.getTransformations(condDict={'Status':'Active'},extraParams=True)
    activeTransforms = []
    if not result['OK']:
      gLogger.error("BookkeepingWatchAgent.execute: Failed to get transformations.", result['Message'])
      return S_OK()

    # Process each transformation
    for transDict in result['Value']:    
      transID = long(transDict['TransformationID'])
      if not transDict.has_key('BkQueryID'):
        gLogger.info("BookkeepingWatchAgent.execute: Transformation %d did not have associated BK query" % transID)
      else:
        res = self.transClient.getBookkeepingQueryForTransformation(transID)
        if not res['OK']:
          gLogger.warn("BookkeepingWatchAgent.execute: Failed to get BkQuery", res['Message'])
          continue
        bkDict = res['Value'] 
        
        # Make sure that we include only the required elements and format them correctly
        for name,value in bkDict.items():  
          if name == "BkQueryID" :
            del bkDict[name]
          elif name == "ProductionID" or name == "EventType":
            if int(value) == 0:
              del bkDict[name]
            else:
              bkDict[name] = str(value)
          else:
            if value.lower() == "all": 
              del bkDict[name]

        # Determine the correct time stamp to use for this transformation
        if self.timeLog.has_key(transID):
          if self.fullTimeLog.has_key(transID):
            # If it is more than a day since the last reduced query, make a full query just in case
            if (datetime.datetime.utcnow() - self.fullTimeLog[transID]) < datetime.timedelta(seconds=self.fullUpdatePeriod):
              timeStamp = self.timeLog[transID]
              bkDict['StartDate'] = (timeStamp - datetime.timedelta(seconds=10)).strftime('%Y-%m-%d %H:%M:%S')
            else:
              self.fullTimeLog[transID] = datetime.datetime.utcnow()    
        self.timeLog[transID] = datetime.datetime.utcnow()
        if not self.fullTimeLog.has_key(transID):
          self.fullTimeLog[transID] = datetime.datetime.utcnow()
        gLogger.debug("Using BK query for transformation %d: %s" % (transID,str(bkDict)))
                 
        # Perform BK query
        start = time.time()              
        result = self.bkClient.getFilesWithGivenDataSets(bkDict)    
        rtime = time.time()-start    
        gLogger.verbose("BK query time: %.2f seconds." % (rtime))
        if not result['OK']:
          gLogger.error("BookkeepingWatchAgent.execute: Failed to get response from the Bookkeeping", result['Message'])
          continue
        lfnList = result['Value']   
        
        # Check if the number of files has changed since the last cycle
        nlfns = len(lfnList)
        gLogger.info("%d files returned for transformation %d from the BK" % (nlfns,int(transID)) )
        if self.fileLog.has_key(transID):
          if nlfns == self.fileLog[transID]:
            gLogger.verbose('No new files in BK DB since last check')
        self.fileLog[transID] = nlfns

        # Add any new files to the transformation
        if lfnList:
          gLogger.verbose('Processing %d lfns for transformation %d' % (len(lfnList),transID) )
          fileDict = {}
          for lfn in lfnList:
            fileDict[lfn] = {'PFN':'Unknown','Size':0,'SE':'Unknown','GUID':'0000','Checksum':'0000'}
          result = self.transClient.addFile(fileDict,True)
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
          result = self.transClient.addFilesToTransformation(transID,lfns)
          if not result['OK']:
            gLogger.warn("BookkeepingWatchAgent.execute: failed to add lfns to transformation", result['Message'])   
            self.fileLog[transID] = 0
          else:
            if result['Value']['Failed']:
              for lfn,error in res['Value']['Failed'].items():
                gLogger.warn("BookkeepingWatchAgent.execute: Failed to add %s to transformation" % lfn,error)
            if result['Value']['Successful']:
              added = 0
              for lfn,status in result['Value']['Successful'].items():
                if status == 'Added':
                  added+=1
              gLogger.info("BookkeepingWatchAgent.execute: Added %d files to transformation" % added)   
    
    return S_OK() 
