""" This LHCbDIRAC agent takes BkQueries from the TranformationDB,
    and issue a query to the BKK, for populating a table in the Transformation DB,
    with all the files in input to a transformation.

    A pickle file is used as a cache.
"""

import os
import time
import datetime
import pickle
import Queue

from DIRAC                                                                import S_OK, gLogger
from DIRAC.Core.Base.AgentModule                                          import AgentModule
from DIRAC.Core.Utilities.ThreadPool                                      import ThreadPool
from DIRAC.Core.Utilities.ThreadSafe                                      import Synchronizer
from DIRAC.Core.Utilities.List                                            import breakListIntoChunks
from DIRAC.FrameworkSystem.Client.MonitoringClient                        import gMonitor
from DIRAC.TransformationSystem.Agent.TransformationAgentsUtilities       import TransformationAgentsUtilities
from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient                 import BookkeepingClient
from LHCbDIRAC.TransformationSystem.Client.TransformationClient           import TransformationClient

AGENT_NAME = 'Transformation/BookkeepingWatchAgent'

__RCSID__ = "$Id$"

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

    self.fullUpdatePeriod = 86400
    self.bkUpdateLatency = 7200
    self.debug = False

    self.transInThread = {}

    self.pickleFile = 'BookkeepingWatchAgent.pkl'
    self.chunkSize = 1000

    self.pluginsWithNoRunInfo = ( 'LHCbStandard', 'ReplicateDataset', 'ArchiveDataset',
                                  'LHCbMCDSTBroadcastRandom', 'ReplicateToLocalSE',
                                  'RemoveReplicas', 'RemoveReplicasWhenProcessed', 'ReduceReplicas',
                                  'DestroyDataset', 'DestroyDatasetWhenProcessed',
                                  'RemoveDataset',
                                  'BySize', 'Standard' )

    self.timeLog = {}
    self.fullTimeLog = {}
    self.bkQueries = {}

    self.transClient = None
    self.bkClient = None

  def initialize( self ):
    """ Make the necessary initializations.
        The ThreadPool is created here, the _execute() method is what each thread will execute.
    """

    self.fullUpdatePeriod = self.am_getOption( 'FullUpdatePeriod', self.fullUpdatePeriod )
    self.bkUpdateLatency = self.am_getOption( 'BKUpdateLatency', self.bkUpdateLatency )
    self.debug = self.am_getOption( 'verbose', self.debug )

    self.pickleFile = os.path.join( self.am_getWorkDirectory(), self.pickleFile )
    self.chunkSize = self.am_getOption( 'maxFilesPerChunk', self.chunkSize )

    self.pluginsWithNoRunInfo = self.am_getOption( 'PluginsWithNoRunInfo', self.pluginsWithNoRunInfo )

    self._logInfo( 'Full Update Period: %d seconds' % self.fullUpdatePeriod )
    self._logInfo( 'BK update latency : %d seconds' % self.bkUpdateLatency )
    self._logInfo( 'Plugins with no run info: %s' % ','.join( self.pluginsWithNoRunInfo ) )

    self.transClient = TransformationClient()
    self.bkClient = BookkeepingClient()

    try:
      with open( self.pickleFile, 'r' ) as pf:
        self.timeLog = pickle.load( pf )
        self.fullTimeLog = pickle.load( pf )
        self.bkQueries = pickle.load( pf )
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
        with open( self.pickleFile, 'w' ) as pf:
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
        self.transInThread[transID] = ' [Thread%d] [%s] ' % ( threadID, str( transID ) )

        startTime = time.time()
        self._logInfo( "Processing transformation %s." % transID, transID = transID )

        res = self.transClient.getTransformation( transID, extraParams = False )
        if not res['OK']:
          self._logError( "Failed to get transformation", res['Message'], transID = transID )
          continue
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

        runDict = {}
        # There is no need to add the run information for a removal transformation
        if transPlugin not in self.pluginsWithNoRunInfo:
          # get the run number in order to sort the files
          for lfnChunk in breakListIntoChunks( files, self.chunkSize ):
            start = time.time()
            res = self.bkClient.getFileMetadata( lfnChunk )
            self._logVerbose( "Got metadata from BK for %d files" % len( lfnChunk ), transID = transID, reftime = start )
            if not res['OK']:
              self._logError( "Failed to get BK metadata for %d files" % len( lfnChunk ),
                              res['Message'], transID = transID )
            else:
              for lfn, metadata in res['Value']['Successful'].iteritems():
                runID = metadata.get( 'RunNumber', None )
                if isinstance( runID, ( basestring, int, long ) ):
                  runDict.setdefault( int( runID ), [] ).append( lfn )
          try:
            self.__addRunsMetadata( transID, runDict.keys() )
          except RuntimeError as e:
            self._logException( "Failure adding runs metadata", method = "__addRunsMetadata", lException = e, transID = transID )
        else:
          runDict[None] = files

        # Add all new files to the transformation
        for runID in sorted( runDict ):
          lfnList = sorted( runDict[runID] )
          lfnChunks = [lfnList] if runID else breakListIntoChunks( lfnList, self.chunkSize )
          for lfnChunk in lfnChunks:
            # Add the files to the transformation
            self._logVerbose( 'Adding %d lfns for transformation' % len( lfnChunk ), transID = transID )
            result = self.transClient.addFilesToTransformation( transID, lfnChunk )
            runDict = {}
            if not result['OK']:
              self._logWarn( "Failed to add %d lfns to transformation" % len( lfnChunk ), result['Message'],
                             transID = transID )
            else:
              # Handle errors
              errors = {}
              for lfn, error in result['Value']['Failed'].iteritems():
                errors.setdefault( error, [] ).append( lfn )
              for error, lfns in errors.iteritems():
                self._logWarn( "Failed to add files to transformation", error, transID = transID )
                self._logVerbose( "\n\t".join( [''] + lfns ) )
              # Add the RunNumber to the newly inserted files
              addedLfns = [lfn for ( lfn, status ) in result['Value']['Successful'].iteritems() if status == 'Added']
              if addedLfns:
                if runID:
                  self._logInfo( "Added %d files to transformation for run %d, now including run information"
                                 % ( len( addedLfns ), runID ) , transID = transID )
                  self._logVerbose( "Associating %d files to run %d" % ( len( addedLfns ), runID ), transID = transID )
                  res = self.transClient.addTransformationRunFiles( transID, runID, addedLfns )
                  if not res['OK']:
                    self._logWarn( "Failed to associate %d files to run %d" % ( len( addedLfns ), runID ),
                                   res['Message'], transID = transID )
                else:
                  self._logInfo( "Added %d files to transformation" % len( addedLfns ) , transID = transID )

      except Exception as x:  # pylint: disable=broad-except
        self._logException( 'Exception while adding files to transformation', lException = x, method = '_execute', transID = transID )
      finally:
        self._logInfo( "Processed transformation", transID = transID, reftime = startTime )
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

    bkQueryLog.pop( 'StartDate', None )
    self.bkQueries[transID] = bkQuery.copy()
    if transID in self.timeLog \
    and bkQueryLog == bkQuery \
    and  ( now - fullTimeLog ) < datetime.timedelta( seconds = self.fullUpdatePeriod ):
      # If it is more than a day since the last reduced query, make a full query just in case
      timeStamp = self.timeLog[transID]
      bkQuery['StartDate'] = ( timeStamp - datetime.timedelta( seconds = self.bkUpdateLatency ) ).strftime( '%Y-%m-%d %H:%M:%S' )
    if 'StartDate' not in bkQuery:
      self.fullTimeLog[transID] = now

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
      self.__updateTimeStamp( transID, now )
      if result['Value']:
        self._logInfo( "Obtained %d files from BK" % len( result['Value'] ), transID = transID )
      return result['Value']

  @gSynchro
  def __updateTimeStamp( self, transID, now ):
    self.timeLog[transID] = now

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
        for run, runMeta in res['Value'].iteritems():
          res = self.transClient.addRunsMetadata( run, runMeta )
          if not res['OK']:
            raise RuntimeError( res['Message'] )
    # Add run duration to the metadata
    runsInCache = self.transClient.getRunsInCache( {'Name':['Duration']} )
    if not runsInCache['OK']:
      raise RuntimeError( runsInCache['Message'] )
    newRuns = list( set( runsList ) - set( runsInCache['Value'] ) )
    if newRuns:
      self._logVerbose( "Associating run duration to %d runs" % len( newRuns ), transID = transID )
      res = self.bkClient.getRunInformation( {'RunNumber':newRuns, 'Fields':['JobStart', 'JobEnd']} )
      if not res['OK']:
        raise RuntimeError( res['Message'] )
      else:
        for run, runMeta in res['Value'].iteritems():
          duration = ( runMeta['JobEnd'] - runMeta['JobStart'] ).seconds
          res = self.transClient.addRunsMetadata( run, {'Duration': duration} )
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
