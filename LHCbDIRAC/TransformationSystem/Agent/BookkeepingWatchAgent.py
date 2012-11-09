""" This LHCbDIRAC agent takes BkQueries from the TranformationDB,
    and issue a query to the BKK, for populating a table in the Transformation DB,
    with all the files in input to a transformation.

    A pickle file is used as a cache.
"""

__RCSID__ = "$Id$"

from DIRAC                                                                import S_OK, gLogger, gMonitor
from DIRAC.Core.Base.AgentModule                                          import AgentModule
from DIRAC.Core.Utilities.ThreadPool                                      import ThreadPool
from DIRAC.Core.Utilities.ThreadSafe                                      import Synchronizer
from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient                 import BookkeepingClient
from LHCbDIRAC.TransformationSystem.Client.TransformationClient           import TransformationClient
from DIRAC.Core.Utilities.List                                            import breakListIntoChunks
import os, time, datetime, pickle, Queue

AGENT_NAME = 'Transformation/BookkeepingWatchAgent'

gSynchro = Synchronizer()

class BookkeepingWatchAgent( AgentModule ):
  """ LHCbDIRAC only agent. A threaded agent.
  """

  def __init__( self, agentName, loadName, baseAgentName = False, properties = dict() ):
    """ c'tor

    :param self: self reference
    :param str agentName: name of agent
    :param bool baseAgentName: whatever
    :param dict properties: whatever else
    """
    AgentModule.__init__( self, agentName, loadName, baseAgentName, properties )

    self.bkQueriesToBeChecked = Queue.Queue()
    self.bkQueriesInCheck = []
    self.transClient = TransformationClient( 'TransformationDB' )
    # Create the BK client
    self.bkClient = BookkeepingClient()

    self.pollingTime = self.am_getOption( 'PollingTime', 120 )
    self.fullUpdatePeriod = self.am_getOption( 'FullUpdatePeriod', 86400 )

  def initialize( self ):
    """ Make the necessary initializations.
        The ThreadPool is created here, the _execute() method is what each thread will execute.
    """

    self.pickleFile = os.path.join( self.am_getWorkDirectory(), "BookkeepingWatchAgent.pkl" )

    try:
      pf = open( self.pickleFile, 'r' )
      self.timeLog = pickle.load( pf )
      self.fullTimeLog = pickle.load( pf )
      self.bkQueries = pickle.load( pf )
      pf.close()
      self.__logInfo( "successfully loaded Log from", self.pickleFile, "initialize" )
    except:
      self.__logInfo( "failed loading Log from", self.pickleFile, "initialize" )
      self.timeLog = {}
      self.fullTimeLog = {}
      self.bkQueries = {}

    maxNumberOfThreads = self.am_getOption( 'maxThreadsInPool', 1 )
    threadPool = ThreadPool( maxNumberOfThreads, maxNumberOfThreads )
    self.chunkSize = self.am_getOption( 'maxFilesPerChunk', 1000 )

    for _i in xrange( maxNumberOfThreads ):
      threadPool.generateJobAndQueueIt( self._execute )

    gMonitor.registerActivity( "Iteration", "Agent Loops", AGENT_NAME, "Loops/min", gMonitor.OP_SUM )
    return S_OK()

  @classmethod
  def __logVerbose( self, message, param = '', method = "execute", transID = 'None' ):
    gLogger.verbose( AGENT_NAME + "." + method + ": [%s] " % str( transID ) + message, param )

  @classmethod
  def __logDebug( self, message, param = '', method = "execute", transID = 'None' ):
    gLogger.debug( AGENT_NAME + "." + method + ": [%s] " % str( transID ) + message, param )

  @classmethod
  def __logInfo( self, message, param = '', method = "execute", transID = 'None' ):
    gLogger.info( AGENT_NAME + "." + method + ": [%s] " % str( transID ) + message, param )

  @classmethod
  def __logWarn( self, message, param = '', method = "execute", transID = 'None' ):
    gLogger.warn( AGENT_NAME + "." + method + ": [%s] " % str( transID ) + message, param )

  @classmethod
  def __logError( self, message, param = '', method = "execute", transID = 'None' ):
    gLogger.error( AGENT_NAME + "." + method + ": [%s] " % str( transID ) + message, param )

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
        self.__logVerbose( "successfully dumped Log into %s" % self.pickleFile )
      except IOError, e:
        self.__logError( "fail to open %s: %s" % ( self.pickleFile, e ) )
      except pickle.PickleError, e:
        self.__logError( "fail to dump %s: %s" % ( self.pickleFile, e ) )
      except ValueError, e:
        self.__logError( "fail to close %s: %s" % ( self.pickleFile, e ) )
      finally:
        pf.close()

################################################################################
  def execute( self ):
    """ Main execution method. Just fills a list, and a queue, with BKKQueries ID.
    """

    gMonitor.addMark( 'Iteration', 1 )
    # Get all the transformations
    result = self.transClient.getTransformations( condDict = {'Status':'Active'}, extraParams = True )
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
        self.__logVerbose( "Transformation does not have associated BK query", transID = transID )
        continue

      self.bkQueriesInCheck.append( transID )
      self.bkQueriesToBeChecked.put( transID )
      _count += 1

    self.__logInfo( "Out of %d transformations, %d put in thread queue" % ( len( result['Value'] ), _count ) )

    self.__dumpLog()
    return S_OK()

  def _execute( self ):
    """ Real executor. This is what is executed by the single threads - so do not return here! Just continue
    """

    while True:#not self.bkQueriesToBeChecked.empty():

      transID = None

      try:

        transID = self.bkQueriesToBeChecked.get()

        startTime = time.time()
        self.__logInfo( "Processing transformation %s." % transID, transID = transID )
        res = self.transClient.getBookkeepingQueryForTransformation( transID )
        if not res['OK']:
          self.__logError( "Failed to get BkQuery", res['Message'], transID = transID )
          continue

        bkQuery = res[ 'Value' ]

        # Determine the correct time stamp to use for this transformation
        now = datetime.datetime.utcnow()
        self.__timeStampForTransformation( transID, bkQuery, now )

        try:
          files = self.__getFiles( transID, bkQuery, now )
        except RuntimeError, e:
          self.__logError( "Failed to get response from the Bookkeeping: %s" % e, "", "__getFiles", transID )
          continue

        # Add any new files to the transformation
        for lfnList in breakListIntoChunks( files, self.chunkSize ):

          # Add the RunNumber to the newly inserted files
          start = time.time()
          # Add the files to the transformation
          self.__logVerbose( 'Adding %d lfns for transformation' % len( lfnList ), transID = transID )
          result = self.transClient.addFilesToTransformation( transID, sorted( lfnList ) )
          runDict = {}
          if not result['OK']:
            self.__logWarn( "Failed to add lfns to transformation", result['Message'], transID = transID )
          else:
            _printFailed = [self.__logWarn( "Failed to add %s to transformation\
            " % lfn, error, transID = transID ) for ( lfn, error ) in result['Value']['Failed'].items()]
            addedLfns = [lfn for ( lfn, status ) in result['Value']['Successful'].items() if status == 'Added']
            if addedLfns:
              self.__logInfo( "Added %d files to transformation, now including run information"
                              % len( addedLfns ) , transID = transID )
              res = self.bkClient.getFileMetadata( addedLfns )
              self.__logVerbose( "BK query time for metadata: %.2f seconds." % ( time.time() - start ),
                                 transID = transID )
              if not res['OK']:
                self.__logError( "Failed to get BK metadata for %d files" % len( addedLfns ),
                                 res['Message'],
                                 transID = transID )
              else:
                for lfn, metadata in res['Value'].items():
                  runID = metadata.get( 'RunNumber', None )
                  if runID:
                    runDict.setdefault( int( runID ), [] ).append( lfn )
              for runID, lfns in runDict.items():
                lfns = [lfn for lfn in lfns if lfn in addedLfns]
                if lfns:
                  self.__logVerbose( "Associating %d files to run %d" % ( len( lfns ), runID ), transID = transID )
                  res = self.transClient.addTransformationRunFiles( transID, runID, lfns )
                  if not res['OK']:
                    self.__logWarn( "Failed to associate %d files \
                    to run %d" % ( len( lfns ), runID ), res['Message'], transID = transID )


            try:
              self.__addRunsMetadata( transID, runDict.keys() )
            except RuntimeError, e:
              self.__logError( "Failure adding runs metadata: %s" % e, "", "__addRunsMetadata", transID )
              continue

        self.__logInfo( "Processed transformation in %.1f seconds" % ( time.time() - startTime ), transID = transID )

      except Exception, x:
        gLogger.exception( '[%s] %s._execute %s' % ( str( transID ), AGENT_NAME, x ) )
      finally:
        if transID in self.bkQueriesInCheck:
          self.bkQueriesInCheck.remove( transID )

    return S_OK()

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
      self.__dumpLog()


  def __getFiles( self, transID, bkQuery, now ):
    """ Perform the query to the Bookkeeping
    """
    self.__logInfo( "Using BK query for transformation: %s" % str( bkQuery ), transID = transID )
    start = time.time()
    result = self.bkClient.getFiles( bkQuery )
    self.__logVerbose( "BK query time: %.2f seconds." % ( time.time() - start ), transID = transID )
    if not result['OK']:
      raise RuntimeError, result['Message']
    else:
      self.timeLog[transID] = now
      return result['Value']


  def __addRunsMetadata( self, transID, runsList ):
    """ Add the run metadata
    """
    runsInCache = self.transClient.getRunsInCache( {'Name':['TCK', 'CondDb', 'DDDB']} )
    if not runsInCache['OK']:
      raise RuntimeError, runsInCache['Message']
    newRuns = list( set( runsList ) - set( runsInCache['Value'] ) )
    if newRuns:
      self.__logVerbose( "Associating run metadata to %d runs" % len( newRuns ), transID = transID )
      res = self.bkClient.getRunInformation( {'RunNumber':newRuns, 'Fields':['TCK', 'CondDb', 'DDDB']} )
      if not res['OK']:
        raise RuntimeError, res['Message']
      else:
        for run, runMeta in res['Value'].items():
          res = self.transClient.addRunsMetadata( run, runMeta )
          if not res['OK']:
            raise RuntimeError, res['Message']

  def finalize( self ):
    """ Gracious finalization
    """
    if self.bkQueriesInCheck:
      self.__logInfo( "Wait for queue to get empty before terminating the agent (%d tasks)" % len( self.bkQueriesInCheck ) )
      while self.bkQueriesInCheck:
        time.sleep( 2 )
      self.log.info( "Queue is empty, terminating the agent..." )
    return S_OK()
