""" This LHCbDIRAC agent takes BkQueries from the TranformationDB,
    and issue a query to the BKK, for populating a table in the Transformation DB,
    with all the files in input to a transformation.

    A pickle file is used as a cache.
"""

__RCSID__ = "$Id: BookkeepingWatchAgent.py 82052 2015-03-25 13:39:28Z fstagni $"

import os
import time
import datetime
import pickle
import Queue

from DIRAC                                                                import S_OK, gLogger, gMonitor
from DIRAC.Core.Base.AgentModule                                          import AgentModule
from DIRAC.Core.Utilities.ThreadPool                                      import ThreadPool
from DIRAC.Core.Utilities.ThreadSafe                                      import Synchronizer
from DIRAC.TransformationSystem.Agent.TransformationAgentsUtilities       import TransformationAgentsUtilities
from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient                 import BookkeepingClient
from LHCbDIRAC.TransformationSystem.Client.TransformationClient           import TransformationClient
from DIRAC.Core.Utilities.List                                            import breakListIntoChunks

AGENT_NAME = 'Transformation/BookkeepingWatchAgent'

gSynchro = Synchronizer()

class BookkeepingWatchAgent( AgentModule, TransformationAgentsUtilities ):
  """ LHCbDIRAC only agent. A threaded agent.
  """

  def __init__( self, *args, **kwargs ):
    """ c'tor
    """
    AgentModule.__init__( self, *args, **kwargs )
    TransformationAgentsUtilities.__init__( self )

    self.bkQueriesToBeChecked = Queue.Queue()
    self.bkQueriesInCheck = []

    self.fullUpdatePeriod = self.am_getOption( 'FullUpdatePeriod', 86400 )
    self.debug = self.am_getOption( 'verbose', False )

    self.transInThread = {}

    self.pickleFile = os.path.join( self.am_getWorkDirectory(), "BookkeepingWatchAgent.pkl" )
    self.chunkSize = self.am_getOption( 'maxFilesPerChunk', 1000 )

    self.pluginsWithNoRunInfo = self.am_getOption( 'PluginsWithNoRunInfo',
                                                   ( 'LHCbStandard', 'ReplicateDataset', 'ArchiveDataset',
                                                    'LHCbMCDSTBroadcastRandom', 'ReplicateToLocalSE',
                                                    'BySize', 'Standard' ) )

    self.timeLog = {}
    self.fullTimeLog = {}
    self.bkQueries = {}

    self.transClient = None
    self.bkClient = None

  def initialize( self ):
    """ Make the necessary initializations.
        The ThreadPool is created here, the _execute() method is what each thread will execute.
    """

    self.transClient = TransformationClient()
    self.bkClient = BookkeepingClient()

    try:
      pf = open( self.pickleFile, 'r' )
      self.timeLog = pickle.load( pf )
      self.fullTimeLog = pickle.load( pf )
      self.bkQueries = pickle.load( pf )
      pf.close()
      self._logInfo( "successfully loaded Log from", self.pickleFile, "initialize" )
    except:
      self._logInfo( "failed loading Log from", self.pickleFile, "initialize" )
      self.timeLog = {}
      self.fullTimeLog = {}
      self.bkQueries = {}

    maxNumberOfThreads = self.am_getOption( 'maxThreadsInPool', 1 )
    threadPool = ThreadPool( maxNumberOfThreads, maxNumberOfThreads )

    for i in xrange( maxNumberOfThreads ):
      threadPool.generateJobAndQueueIt( self._execute, [i] )

    gMonitor.registerActivity( "Iteration", "Agent Loops", AGENT_NAME, "Loops/min", gMonitor.OP_SUM )
    return S_OK()

  @gSynchro
  def __dumpLog( self ):
    """ dump the log in the pickle file
    """
    if self.pickleFile:
      try:
        pf = open( self.pickleFile, 'w' )
        pickle.dump( self.timeLog, pf )
        pickle.dump( self.fullTimeLog, pf )
        pickle.dump( self.bkQueries, pf )
        self._logVerbose( "successfully dumped Log into %s" % self.pickleFile )
      except IOError as e:
        self._logError( "fail to open %s: %s" % ( self.pickleFile, e ) )
      except pickle.PickleError as e:
        self._logError( "fail to dump %s: %s" % ( self.pickleFile, e ) )
      except ValueError as e:
        self._logError( "fail to close %s: %s" % ( self.pickleFile, e ) )
      finally:
        try:
          pf.close()
        except UnboundLocalError:
          pass


################################################################################
  def execute( self ):
    """ Main execution method. Just fills a list, and a queue, with BKKQueries ID.
    """

    gMonitor.addMark( 'Iteration', 1 )
    # Get all the transformations
    result = self.transClient.getTransformations( condDict = {'Status':['Active', 'Idle']} )
    if not result['OK']:
      self._logError( "Failed to get transformations.", result['Message'] )
      return S_OK()
    transIDsList = [long( transDict['TransformationID'] ) for transDict in result['Value']]
    res = self.transClient.getTransformationsWithBkQueries( transIDsList )
    if not res['OK']:
      self._logError( "Failed to get transformations with Bk Queries.", res['Message'] )
      return S_OK()
    transIDsWithBkQueriesList = res['Value']

    _count = 0
    # Process each transformation
    for transID in transIDsWithBkQueriesList:
      if transID in self.bkQueriesInCheck:
        continue
      self.bkQueriesInCheck.append( transID )
      self.bkQueriesToBeChecked.put( transID )
      _count += 1

    self._logInfo( "Out of %d transformations, %d put in thread queue" % ( len( result['Value'] ), _count ) )

    self.__dumpLog()
    return S_OK()

  def _execute( self, threadID ):
    """ Real executor. This is what is executed by the single threads - so do not return here! Just continue
    """

    while True:  # not self.bkQueriesToBeChecked.empty():

      transID = None

      try:

        transID = self.bkQueriesToBeChecked.get()

        startTime = time.time()
        self._logInfo( "Processing transformation %s." % transID, transID = transID )
        self.transInThread[transID] = ' [Thread%d] [%s] ' % ( threadID, str( transID ) )

        res = self.transClient.getTransformation( transID, extraParams = False )
        if not res['OK']:
          self._logError( "Failed to get transformation", res['Message'], transID = transID )
          continue
        transType = res['Value']['Type']
        transPlugin = res['Value']['Plugin']

        res = self.transClient.getBookkeepingQuery( transID )
        if not res['OK']:
          self._logError( "Failed to get BkQuery", res['Message'], transID = transID )
          continue
        bkQuery = res[ 'Value' ]

        # Determine the correct time stamp to use for this transformation
        now = datetime.datetime.utcnow()
        self.__timeStampForTransformation( transID, bkQuery, now )

        try:
          files = self.__getFiles( transID, bkQuery, now )
        except RuntimeError as e:
          # In case we failed a full query, we should retry full query until successful
          if 'StartDate' not in bkQuery:
            self.bkQueries.pop( transID, None )
          self._logError( "Failed to get response from the Bookkeeping: %s" % e, "", "__getFiles", transID )
          continue

        # Add any new files to the transformation
        for lfnList in breakListIntoChunks( files, self.chunkSize ):

          # Add the RunNumber to the newly inserted files
          start = time.time()
          # Add the files to the transformation
          self._logVerbose( 'Adding %d lfns for transformation' % len( lfnList ), transID = transID )
          result = self.transClient.addFilesToTransformation( transID, sorted( lfnList ) )
          runDict = {}
          if not result['OK']:
            self._logWarn( "Failed to add %d lfns to transformation" % len( lfnList ), result['Message'],
                           transID = transID )
          else:
            _printFailed = [self._logWarn( "Failed to add %s to transformation\
            " % lfn, error, transID = transID ) for ( lfn, error ) in result['Value']['Failed'].items()]
            addedLfns = [lfn for ( lfn, status ) in result['Value']['Successful'].items() if status == 'Added']
            # There is no need to add the run information for a removal transformation
            if addedLfns and transType != 'Removal' and transPlugin not in self.pluginsWithNoRunInfo:
              self._logInfo( "Added %d files to transformation, now including run information"
                              % len( addedLfns ) , transID = transID )
              res = self.bkClient.getFileMetadata( addedLfns )
              self._logVerbose( "BK query time for metadata: %.2f seconds." % ( time.time() - start ),
                                 transID = transID )
              if not res['OK']:
                self._logError( "Failed to get BK metadata for %d files" % len( addedLfns ),
                                 res['Message'],
                                 transID = transID )
              else:
                for lfn, metadata in res['Value']['Successful'].items():
                  runID = metadata.get( 'RunNumber', None )
                  if runID:
                    runDict.setdefault( int( runID ), [] ).append( lfn )
              for runID, lfns in runDict.items():
                lfns = [lfn for lfn in lfns if lfn in addedLfns]
                if lfns:
                  self._logVerbose( "Associating %d files to run %d" % ( len( lfns ), runID ), transID = transID )
                  res = self.transClient.addTransformationRunFiles( transID, runID, lfns )
                  if not res['OK']:
                    self._logWarn( "Failed to associate %d files \
                    to run %d" % ( len( lfns ), runID ), res['Message'], transID = transID )


            try:
              self.__addRunsMetadata( transID, runDict.keys() )
            except RuntimeError as e:
              self._logError( "Failure adding runs metadata: %s" % e, "", "__addRunsMetadata", transID )
              continue

      except Exception as x:
        gLogger.exception( '[%s] %s._execute %s' % ( str( transID ), AGENT_NAME, x ) )
      finally:
        self._logInfo( "Processed transformation in %.1f seconds" % ( time.time() - startTime ), transID = transID )
        if transID in self.bkQueriesInCheck:
          self.bkQueriesInCheck.remove( transID )
        self.transInThread.pop( transID, None )

    return S_OK()

  @gSynchro
  def __timeStampForTransformation( self, transID, bkQuery, now ):
    """ Determine the correct time stamp to use for this transformation
    """

    fullTimeLog = self.fullTimeLog.setdefault( transID, now )
    bkQueryLog = self.bkQueries.setdefault( transID, {} )

    if 'StartDate' in bkQueryLog:
      bkQueryLog.pop( 'StartDate' )
    self.bkQueries[transID] = bkQuery.copy()
    if transID in self.timeLog \
    and bkQueryLog == bkQuery \
    and  ( now - fullTimeLog ) < datetime.timedelta( seconds = self.fullUpdatePeriod ):
      # If it is more than a day since the last reduced query, make a full query just in case
      timeStamp = self.timeLog[transID]
      bkQuery['StartDate'] = ( timeStamp - datetime.timedelta( seconds = 10 ) ).strftime( '%Y-%m-%d %H:%M:%S' )
    if 'StartDate' not in bkQuery:
      self.fullTimeLog[transID] = now

  @gSynchro
  def __getFiles( self, transID, bkQuery, now ):
    """ Perform the query to the Bookkeeping
    """
    self._logInfo( "Using BK query for transformation: %s" % str( bkQuery ), transID = transID )
    start = time.time()
    result = self.bkClient.getFiles( bkQuery )
    self._logVerbose( "BK query time: %.2f seconds." % ( time.time() - start ), transID = transID )
    if not result['OK']:
      raise RuntimeError( result['Message'] )
    else:
      self.timeLog[transID] = now
      if result['Value']:
        self._logInfo( "Obtained %d files from BK" % len( result['Value'] ) )
      return result['Value']


  def __addRunsMetadata( self, transID, runsList ):
    """ Add the run metadata
    """
    runsInCache = self.transClient.getRunsInCache( {'Name':['TCK', 'CondDb', 'DDDB']} )
    if not runsInCache['OK']:
      raise RuntimeError( runsInCache['Message'] )
    newRuns = list( set( runsList ) - set( runsInCache['Value'] ) )
    if newRuns:
      self._logVerbose( "Associating run metadata to %d runs" % len( newRuns ), transID = transID )
      res = self.bkClient.getRunInformation( {'RunNumber':newRuns, 'Fields':['TCK', 'CondDb', 'DDDB']} )
      if not res['OK']:
        raise RuntimeError( res['Message'] )
      else:
        for run, runMeta in res['Value'].items():
          res = self.transClient.addRunsMetadata( run, runMeta )
          if not res['OK']:
            raise RuntimeError( res['Message'] )

  def finalize( self ):
    """ Gracious finalization
    """
    if self.bkQueriesInCheck:
      self._logInfo( "Wait for queue to get empty before terminating the agent (%d tasks)" % len( self.transInThread ) )
      self.bkQueriesInCheck = []
      while self.transInThread:
        time.sleep( 2 )
      self.log.info( "Threads are empty, terminating the agent..." )
    return S_OK()
