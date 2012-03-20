################################################################################
# $HeadURL$
################################################################################

__RCSID__ = "$Id$"

from DIRAC                                                                import S_OK, gLogger, gMonitor
from DIRAC.Core.Base.AgentModule                                          import AgentModule
from DIRAC.Core.Utilities.ThreadPool                                      import ThreadPool
from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient              import BookkeepingClient
from LHCbDIRAC.TransformationSystem.Client.TransformationClient           import TransformationClient
from DIRAC.Core.Utilities.List                                            import sortList, breakListIntoChunks
import os, time, datetime, pickle, Queue

AGENT_NAME = 'Transformation/BookkeepingWatchAgent'

class BookkeepingWatchAgent( AgentModule ):

################################################################################
  def initialize( self ):
    """ Make the necessary initializations """

    self.pickleFile = os.path.join( self.am_getWorkDirectory(), "BookkeepingWatchAgent.pkl" )

    try:
      f = open( self.pickleFile, 'r' )
      self.timeLog = pickle.load( f )
      self.fullTimeLog = pickle.load( f )
      self.bkQueries = pickle.load( f )
      f.close()
      self.__logInfo( "successfully loaded Log from", self.pickleFile, "initialize" )
    except:
      self.__logInfo( "failed loading Log from", self.pickleFile, "initialize" )
      self.timeLog = {}
      self.fullTimeLog = {}
      self.bkQueries = {}

    self.maxNumberOfThreads = self.am_getOption( 'maxThreadsInPool', 1 )
    self.threadPool = ThreadPool( self.maxNumberOfThreads,
                                            self.maxNumberOfThreads )
    self.chunkSize = self.am_getOption( 'maxFilesPerChunk', 1000 )

    self.bkQueriesToBeChecked = Queue.Queue()
    self.bkQueriesInCheck = []

    for i in xrange( self.maxNumberOfThreads ):
      self.threadPool.generateJobAndQueueIt( self._execute )

    self.pollingTime = self.am_getOption( 'PollingTime', 120 )
    self.fullUpdatePeriod = self.am_getOption( 'FullUpdatePeriod', 86400 )
    gMonitor.registerActivity( "Iteration", "Agent Loops", AGENT_NAME, "Loops/min", gMonitor.OP_SUM )
    self.transClient = TransformationClient( 'TransformationDB' )
    # Create the BK client
    self.bkClient = BookkeepingClient()
    return S_OK()

  def __logVerbose( self, message, param='', method="execute", transID='None' ):
    gLogger.verbose( AGENT_NAME + "." + method + ": [%s] " % str( transID ) + message, param )

  def __logDebug( self, message, param='', method="execute", transID='None' ):
    gLogger.debug( AGENT_NAME + "." + method + ": [%s] " % str( transID ) + message, param )

  def __logInfo( self, message, param='', method="execute", transID='None' ):
    gLogger.info( AGENT_NAME + "." + method + ": [%s] " % str( transID ) + message, param )

  def __logWarn( self, message, param='', method="execute", transID='None' ):
    gLogger.warn( AGENT_NAME + "." + method + ": [%s] " % str( transID ) + message, param )

  def __logError( self, message, param='', method="execute", transID='None' ):
    gLogger.error( AGENT_NAME + "." + method + ": [%s] " % str( transID ) + message, param )

  def __dumpLog( self ):
    if self.pickleFile:
      try:
        f = open( self.pickleFile, 'w' )
        pickle.dump( self.timeLog, f )
        pickle.dump( self.fullTimeLog, f )
        pickle.dump( self.bkQueries, f )
        f.close()
        self.__logVerbose( "successfully dumped Log into %s" % self.pickleFile )
      except Exception, e:
        self.__logError( "fail to dump Log into %s: %s" % ( self.pickleFile, e ) )

################################################################################
  def execute( self ):
    """ Main execution method
    """

    gMonitor.addMark( 'Iteration', 1 )
    # Get all the transformations
    result = self.transClient.getTransformations( condDict={'Status':'Active'}, extraParams=True )
    if not result['OK']:
      self.__logError( "Failed to get transformations.", result['Message'] )
      return S_OK()

    _count = 0
    # Process each transformation
    for transDict in result['Value']:
      transID = long( transDict['TransformationID'] )

      if transID in self.bkQueriesInCheck:
        continue
      if 'BkQueryID' not in transDict:
        self.__logVerbose( "Transformation does not have associated BK query", transID=transID )
        continue

      self.bkQueriesInCheck.append( transID )
      self.bkQueriesToBeChecked.put( ( transID, transDict ) )
      _count += 1

    self.__logInfo( "Out of %d transformations, %d put in thread queue" % ( len( result['Value'] ), _count ) )

    self.__dumpLog()
    return S_OK()

  def _execute( self ):

    while True:#not self.bkQueriesToBeChecked.empty():

      transID = None

      try:

        transID, transDict = self.bkQueriesToBeChecked.get()

        startTime = time.time()
        self.__logInfo( "Processing transformation %s." % transID, transID=transID )
        res = self.transClient.getBookkeepingQueryForTransformation( transID )
        if not res['OK']:
          self.__logError( "Failed to get BkQuery", res['Message'], transID=transID )
          continue

        bkQuery = res[ 'Value' ]

        # Determine the correct time stamp to use for this transformation
        now = datetime.datetime.utcnow()
        fullTimeLog = self.fullTimeLog.setdefault( transID, now )
        bkQueryLog = self.bkQueries.setdefault( transID, {} )

        if 'StartDate' in bkQueryLog:
          bkQueryLog.pop( 'StartDate' )
        self.bkQueries[transID] = bkQuery.copy()
        if transID in self.timeLog and bkQueryLog == bkQuery and  ( now - fullTimeLog ) < datetime.timedelta( seconds=self.fullUpdatePeriod ):
          # If it is more than a day since the last reduced query, make a full query just in case
          timeStamp = self.timeLog[transID]
          bkQuery['StartDate'] = ( timeStamp - datetime.timedelta( seconds=10 ) ).strftime( '%Y-%m-%d %H:%M:%S' )
        if 'StartDate' not in bkQuery:
          self.fullTimeLog[transID] = now
          self.__dumpLog()
        self.timeLog[transID] = now

        # Perform the query to the Bookkeeping
        self.__logInfo( "Using BK query for transformation: %s" % str( bkQuery ), transID=transID )
        start = time.time()
        result = self.bkClient.getFilesWithGivenDataSets( bkQuery )
        self.__logVerbose( "BK query time: %.2f seconds." % ( time.time() - start ), transID=transID )
        if not result['OK']:
          self.__logError( "Failed to get response from the Bookkeeping", result['Message'], transID=transID )
        else:

          # Add any new files to the transformation
          for lfnList in breakListIntoChunks( result['Value'], self.chunkSize ):
            # Add the files to the transformation
            self.__logVerbose( 'Adding %d lfns for transformation' % len( lfnList ), transID=transID )
            result = self.transClient.addFilesToTransformation( transID, sortList( lfnList ) )
            if not result['OK']:
              self.__logWarn( "Failed to add lfns to transformation", result['Message'], transID=transID )
            else:
              printFailed = [self.__logWarn( "Failed to add %s to transformation" % lfn, error, transID=transID ) for ( lfn, error ) in result['Value']['Failed'].items()]
              addedLfns = [lfn for ( lfn, status ) in result['Value']['Successful'].items() if status == 'Added']
              if addedLfns:
                self.__logInfo( "Added %d files to transformation, now including run information" % len( addedLfns ) , transID=transID )

                # Add the RunNumber to the newly inserted files
                start = time.time()
                res = self.bkClient.getFileMetadata( addedLfns )
                self.__logVerbose( "BK query time for metadata: %.2f seconds." % ( time.time() - start ), transID=transID )
                if not res['OK']:
                  self.__logError( "Failed to get BK metadata for %d files" % len( addedLfns ), res['Message'], transID=transID )
                else:
                  runDict = {}
                  for lfn, metadata in res['Value'].items():
                    runID = metadata.get( 'RunNumber', None )
                    if runID:
                      runDict.setdefault( int( runID ), [] ).append( lfn )
                  for runID, lfns in runDict.items():
                    self.__logVerbose( "Associating %d files to run %d" % ( len( lfns ), runID ), transID=transID )
                    res = self.transClient.addTransformationRunFiles( transID, runID, lfns )
                    if not res['OK']:
                      self.__logWarn( "Failed to associate %d files to run %d" % ( len( lfns ), runID ), res['Message'], transID=transID )
        self.__logInfo( "Processed transformation in %.1f seconds" % ( time.time() - startTime ), transID=transID )

      except Exception, x:
        gLogger.exception( '[%s] %s.execute %s' % ( str( transID ), AGENT_NAME, x ) )
      finally:
        if transID in self.bkQueriesInCheck:
          self.bkQueriesInCheck.remove( transID )

    return S_OK()

  def finalize( self ):

    if self.bkQueriesInCheck:
      self.__logInfo( "Wait for queue to get empty before terminating the agent (%d tasks)" % len( self.bkQueriesInCheck ) )
      while self.bkQueriesInCheck:
        time.sleep( 2 )
      self.log.info( "Queue is empty, terminating the agent..." )
    return S_OK()
