########################################################################
# $HeadURL$
########################################################################

__RCSID__ = "$Id$"

from DIRAC                                                                import S_OK, S_ERROR, gConfig, gLogger, gMonitor
from DIRAC.Core.Base.AgentModule                                          import AgentModule
from LHCbDIRAC.NewBookkeepingSystem.Client.BookkeepingClient              import BookkeepingClient
from LHCbDIRAC.TransformationSystem.Client.TransformationClient           import TransformationClient
from DIRAC.Core.Utilities.List                                            import sortList, breakListIntoChunks
import os, time, datetime

AGENT_NAME = 'Transformation/BookkeepingWatchAgent'

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
    self.transClient = TransformationClient('TransformationDB')
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
        continue
      res = self.transClient.getBookkeepingQueryForTransformation(transID)
      if not res['OK']:
        gLogger.warn("BookkeepingWatchAgent.execute: Failed to get BkQuery", res['Message'])
        continue
      bkDict = res['Value']
        
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

      # Perform the query to the Bookkeeping
      gLogger.verbose("Using BK query for transformation %d: %s" % (transID,str(bkDict)))
      start = time.time()              
      result = self.bkClient.getFilesWithGivenDataSets(bkDict)    
      rtime = time.time()-start    
      gLogger.verbose("BK query time: %.2f seconds." % (rtime))
      if not result['OK']:
        gLogger.error("BookkeepingWatchAgent.execute: Failed to get response from the Bookkeeping", result['Message'])
        continue
      lfnListBK = result['Value']   

      # Check if the number of files has changed since the last cycle
      nlfns = len(lfnListBK)
      gLogger.info("%d files returned for transformation %d from the BK" % (nlfns,int(transID)) )
      if self.fileLog.has_key(transID):
        if nlfns == self.fileLog[transID]:
          gLogger.verbose('No new files in BK DB since last check')
      self.fileLog[transID] = nlfns

      # Add any new files to the transformation
      addedLfns = []
      if lfnListBK:
        lfnChunks = breakListIntoChunks(lfnListBK,1000)
        for lfnList in lfnChunks:
          addedLfns = []
          gLogger.verbose('Processing %d lfns for transformation %d' % (len(lfnList),transID) )
          # Add the files to the transformation
          gLogger.verbose('Adding %d lfns for transformation %d' % (len(lfnList),transID) )
          result = self.transClient.addFilesToTransformation(transID,sortList(lfnList))
          if not result['OK']:
            gLogger.warn("BookkeepingWatchAgent.execute: failed to add lfns to transformation", result['Message'])   
            self.fileLog[transID] = 0
          else:
            if result['Value']['Failed']:
              for lfn,error in res['Value']['Failed'].items():
                gLogger.warn("BookkeepingWatchAgent.execute: Failed to add %s to transformation" % lfn,error)
            if result['Value']['Successful']:
              for lfn,status in result['Value']['Successful'].items():
                if status == 'Added':
                  addedLfns.append(lfn)
              gLogger.info("BookkeepingWatchAgent.execute: Added %d files to transformation %d" % (len(addedLfns),transID) )
              #addedLfns = result['Value']['Successful'].keys() # TO BE COMMENTED OUT ONCE BACK POPULATION IS DONE

          # Add the RunNumber to the newly inserted files
          if addedLfns:
            gLogger.info("BookkeepingWatchAgent.execute: Obtaining metadata for %d files" % len(addedLfns))
            start = time.time()
            res = self.bkClient.getFileMetadata(addedLfns)
            rtime = time.time()-start
            gLogger.verbose("BK query time: %.2f seconds." % (rtime))
            if not res['OK']:
              gLogger.error("BookkeepingWatchAgent.execute: Failed to get BK metadata",res['Message']) 
            else:
              runDict = {}
              for lfn,metadata in res['Value'].items():
                runID = metadata.get('RunNumber',0)
                if runID:
                  runID = int(runID)
                  if not runDict.has_key(runID):
                    runDict[runID] = []
                  runDict[runID].append(lfn)
              for runID in sortList(runDict.keys()):
                lfns = runDict[runID]
                gLogger.verbose("BookkeepingWatchAgent.execute: Associating %d files to run %d" % (len(lfns),runID)) 
                res = self.transClient.addTransformationRunFiles(transID,runID,sortList(lfns))
                if not res['OK']:
                  gLogger.warn("BookkeepingWatchAgent.execute: Failed to associated files to run",res['Message'])
    return S_OK() 
