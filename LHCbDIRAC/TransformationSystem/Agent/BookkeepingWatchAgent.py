########################################################################
# $HeadURL$
########################################################################

__RCSID__ = "$Id$"

from DIRAC                                                                import S_OK, gLogger, gMonitor
from DIRAC.Core.Base.AgentModule                                          import AgentModule
from DIRAC.Core.Utilities.ThreadPool                                      import ThreadPool
from LHCbDIRAC.NewBookkeepingSystem.Client.BookkeepingClient              import BookkeepingClient
from LHCbDIRAC.TransformationSystem.Client.TransformationClient           import TransformationClient
from DIRAC.Core.Utilities.List                                            import sortList, breakListIntoChunks
import os, time, datetime, pickle, Queue

AGENT_NAME = 'Transformation/BookkeepingWatchAgent'

class BookkeepingWatchAgent( AgentModule ):

  #############################################################################
  def initialize( self ):
    """ Make the necessary initializations """
    
    self.pickleFile = os.path.join( self.am_getWorkDirectory(), "BookkeepingWatchAgent.pkl" )
    
    try:
      f                = open( self.pickleFile, 'r' )
      self.timeLog     = pickle.load( f )
      self.fullTimeLog = pickle.load( f )
      self.bkQueries   = pickle.load( f )
      f.close()
      self.__logInfo( "successfully loaded Log from %s", self.pickleFile, "initialize" )
    except:
      self.__logInfo( "failed loading Log from %s", self.pickleFile, "initialize" )
      self.timeLog     = {}
      self.fullTimeLog = {}
      self.bkQueries   = {}
      
    self.maxNumberOfThreads   = self.am_getOption( 'maxThreadsInPool', 1 )
    self.threadPool           = ThreadPool( self.maxNumberOfThreads,
                                            self.maxNumberOfThreads )  
    self.bkQueriesToBeChecked = Queue.Queue()  
       
    self.pollingTime      = self.am_getOption( 'PollingTime', 120 )
    self.fullUpdatePeriod = self.am_getOption( 'FullUpdatePeriod', 86400 )
    gMonitor.registerActivity( "Iteration", "Agent Loops", AGENT_NAME, "Loops/min", gMonitor.OP_SUM )
    self.transClient      = TransformationClient( 'TransformationDB' )
    # Create the BK client
    self.bkClient         = BookkeepingClient()
    return S_OK()

  def __logVerbose( self, message, param = '', method = "execute" ):
    gLogger.verbose( AGENT_NAME + "." + method + ": " + message, param )

  def __logDebug( self, message, param = '', method = "execute" ):
    gLogger.debug( AGENT_NAME + "." + method + ": " + message, param )

  def __logInfo( self, message, param = '', method = "execute" ):
    gLogger.info( AGENT_NAME + "." + method + ": " + message, param )

  def __logWarn( self, message, param = '', method = "execute" ):
    gLogger.warn( AGENT_NAME + "." + method + ": " + message, param )

  def __logError( self, message, param = '', method = "execute" ):
    gLogger.error( AGENT_NAME + "." + method + ": " + message, param )

  def __dumpLog( self ):
    if self.pickleFile:
      try:
        f = open( self.pickleFile, 'w' )
        pickle.dump( self.timeLog, f )
        pickle.dump( self.fullTimeLog, f )
        pickle.dump( self.bkQueries, f )
        f.close()
        self.__logVerbose( "successfully dumped Log into %s" %self.pickleFile )
      except Exception, e:
        self.__logError( "fail to dump Log into %s: %s" %(self.pickleFile, e) )

  ##############################################################################
  def execute( self ):
    """ Main execution method
    """

    gMonitor.addMark( 'Iteration', 1 )
    # Get all the transformations
    result = self.transClient.getTransformations( condDict = {'Status':'Active'}, extraParams = True )
    if not result['OK']:
      self.__logError( "Failed to get transformations.", result['Message'] )
      return S_OK()

    # Process each transformation
    for transDict in result['Value']:
      transID = long( transDict['TransformationID'] )
      self.bkQueriesToBeChecked.put( ( transID, transDict ) )
    
    for i in xrange( self.maxNumberOfThreads ):
      self.threadPool.generateJobAndQueueIt( self._execute )  
        
    while not self.bkQueriesToBeChecked.empty():
      time.sleep( 1 )
        
    self.__dumpLog()
    return S_OK()

  def _execute( self ): 
       
    while not self.bkQueriesToBeChecked.empty():
    
      transID, transDict = self.bkQueriesToBeChecked.get()

      if 'BkQueryID' not in transDict:
        self.__logInfo( "[%d]Transformation %d did not have associated BK query" % ( transID, transID ) )
        continue
      res = self.transClient.getBookkeepingQueryForTransformation( transID )
      if not res['OK']:
        self.__logWarn( "[%d]Failed to get BkQuery" % transID, res['Message'] )
        continue
    
      bkQuery = res[ 'Value' ]
    
      # Determine the correct time stamp to use for this transformation
      now         = datetime.datetime.utcnow()
      fullTimeLog = self.fullTimeLog.setdefault( transID, now )
      bkQueryLog  = self.bkQueries.setdefault( transID, {} )
 
      if 'StartDate' in bkQueryLog:
        bkQueryLog.pop( 'StartDate' )
      self.bkQueries[transID] = bkQuery.copy()
      if transID in self.timeLog and bkQueryLog == bkQuery and  ( now - fullTimeLog ) < datetime.timedelta( seconds = self.fullUpdatePeriod ):
        # If it is more than a day since the last reduced query, make a full query just in case
        timeStamp = self.timeLog[transID]
        bkQuery['StartDate'] = ( timeStamp - datetime.timedelta( seconds = 10 ) ).strftime( '%Y-%m-%d %H:%M:%S' )
      if 'StartDate' not in bkQuery:
        self.fullTimeLog[transID] = now
        self.__dumpLog()
      self.timeLog[transID] = now
      # Perform the query to the Bookkeeping
      self.__logInfo( "[%d]Using BK query for transformation %d: %s" % ( transID, transID, str( bkQuery ) ) )
      start = time.time()
      result = self.bkClient.getFilesWithGivenDataSets( bkQuery )
      rtime = time.time() - start
#    self.__logInfo( "[%d]BK query time: %.2f seconds." % ( transID,rtime ) )
      self.__logVerbose( "[%d]BK query time: %.2f seconds." % ( transID, rtime ) )
      if not result['OK']:
        self.__logError( "[%d]Failed to get response from the Bookkeeping" % transID, result['Message'] )
      #return S_OK()
      #continue
      else:  
        lfnListBK = result['Value']

        # Add any new files to the transformation
        if lfnListBK:
          lfnChunks = breakListIntoChunks( lfnListBK, 1000 )
          addedLfns = []
          for lfnList in lfnChunks:
            # Add the files to the transformation
            self.__logVerbose( '[%d]Adding %d lfns for transformation %d' % ( transID, len( lfnList ), transID ) )
            result = self.transClient.addFilesToTransformation( transID, sortList( lfnList ) )
            if not result['OK']:
              self.__logWarn( "[%d]failed to add lfns to transformation" % transID, result['Message'] )
            else:
              if result['Value']['Failed']:
                for lfn, error in res['Value']['Failed'].items():
                  self.__logWarn( "[%d]Failed to add %s to transformation" % ( transID, lfn ), error )
              if result['Value']['Successful']:
                for lfn, status in result['Value']['Successful'].items():
                  if status == 'Added':
                    addedLfns.append( lfn )
          if addedLfns:
            self.__logInfo( "[%d]Added %d files to transformation %d, now including run information" % ( transID, len( addedLfns ), transID ) )
#      else:
#        #continue
#        return S_OK()

        # Add the RunNumber to the newly inserted files
            lfnChunks = breakListIntoChunks( addedLfns, 1000 )
            for addedLfns in lfnChunks:
              start = time.time()
              res = self.bkClient.getFileMetadata( addedLfns )
              rtime = time.time() - start
              self.__logVerbose( "[%d]BK query time for metadata: %.2f seconds." % ( transID, rtime ) )
              if not res['OK']:
                self.__logError( "[%d]Failed to get BK metadata" % transID, res['Message'] )
              else:
                runDict = {}
                for lfn, metadata in res['Value'].items():
                  runID = metadata.get( 'RunNumber', None )
                  if runID:
                    runDict.setdefault( int( runID ), [] ).append( lfn )
                for runID in sortList( runDict.keys() ):
                  lfns = runDict[runID]
                  self.__logVerbose( "[%d]Associating %d files to run %d" % ( transID, len( lfns ), runID ) )
                  res = self.transClient.addTransformationRunFiles( transID, runID, sortList( lfns ) )
                  if not res['OK']:
                    self.__logWarn( "[%d]Failed to associate files to run" % transID, res['Message'] )

      gLogger.info( '[%s]done' % transID )
#  self.__dumpLog()
#    return S_OK()

    return S_OK()
