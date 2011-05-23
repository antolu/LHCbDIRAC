########################################################################
# $HeadURL: svn+ssh://svn.cern.ch/reps/dirac/LHCbDIRAC/trunk/LHCbDIRAC/TransformationSystem/Agent/BookkeepingWatchAgent.py $
########################################################################

__RCSID__ = "$Id$"

from DIRAC                                                                import S_OK, gLogger, gMonitor
from DIRAC.Core.Base.AgentModule                                          import AgentModule
from LHCbDIRAC.NewBookkeepingSystem.Client.BookkeepingClient              import BookkeepingClient
from LHCbDIRAC.TransformationSystem.Client.TransformationClient           import TransformationClient
from DIRAC.Core.Utilities.List                                            import sortList, breakListIntoChunks
import os, time, datetime, pickle

AGENT_NAME = 'Transformation/BookkeepingWatchAgent'

class BookkeepingWatchAgent( AgentModule ):

  #############################################################################
  def initialize( self ):
    """ Make the necessary initializations """
    self.pickleFile = os.path.join( self.am_getWorkDirectory(), "BookkeepingWatchAgent.pkl" )
    try:
      f = open( self.pickleFile, 'r' )
      self.timeLog = pickle.load( f )
      self.fullTimeLog = pickle.load( f )
      self.bkQueries = pickle.load( f )
      f.close()
      self.__logInfo( "successfully loaded Log from %s", self.pickleFile, "initialize" )
    except:
      self.__logInfo( "failed loading Log from %s", self.pickleFile, "initialize" )
      self.timeLog = {}
      self.fullTimeLog = {}
      self.bkQueries = {}
    self.pollingTime = self.am_getOption( 'PollingTime', 120 )
    self.fullUpdatePeriod = self.am_getOption( 'FullUpdatePeriod', 86400 )
    gMonitor.registerActivity( "Iteration", "Agent Loops", AGENT_NAME, "Loops/min", gMonitor.OP_SUM )
    self.transClient = TransformationClient( 'TransformationDB' )
    # Create the BK client
    self.bkClient = BookkeepingClient()
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
        self.__logVerbose( "successfully dumped Log into %s", self.pickleFile )
      except:
        self.__logError( "fail to dump Log into %s", self.pickleFile )

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
      if 'BkQueryID' not in transDict:
        self.__logInfo( "Transformation %d did not have associated BK query" % transID )
        continue
      res = self.transClient.getBookkeepingQueryForTransformation( transID )
      if not res['OK']:
        self.__logWarn( "Failed to get BkQuery", res['Message'] )
        continue
      bkQuery = res['Value']

      # Determine the correct time stamp to use for this transformation
      now = datetime.datetime.utcnow()
      fullTimeLog = self.fullTimeLog.setdefault( transID, now )
      bkQueryLog = self.bkQueries.setdefault( transID, {} )
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
      self.__logInfo( "Using BK query for transformation %d: %s" % ( transID, str( bkQuery ) ) )
      start = time.time()
      result = self.bkClient.getFilesWithGivenDataSets( bkQuery )
      rtime = time.time() - start
      self.__logVerbose( "BK query time: %.2f seconds." % ( rtime ) )
      if not result['OK']:
        self.__logError( "Failed to get response from the Bookkeeping", result['Message'] )
        continue
      lfnListBK = result['Value']

      # Add any new files to the transformation
      if lfnListBK:
        lfnChunks = breakListIntoChunks( lfnListBK, 1000 )
        addedLfns = []
        for lfnList in lfnChunks:
          # Add the files to the transformation
          self.__logVerbose( 'Adding %d lfns for transformation %d' % ( len( lfnList ), transID ) )
          result = self.transClient.addFilesToTransformation( transID, sortList( lfnList ) )
          if not result['OK']:
            self.__logWarn( "failed to add lfns to transformation", result['Message'] )
          else:
            if result['Value']['Failed']:
              for lfn, error in res['Value']['Failed'].items():
                self.__logWarn( "Failed to add %s to transformation" % lfn, error )
            if result['Value']['Successful']:
              for lfn, status in result['Value']['Successful'].items():
                if status == 'Added':
                  addedLfns.append( lfn )
        if addedLfns:
          self.__logInfo( "Added %d files to transformation %d, now including run information" % ( len( addedLfns ), transID ) )
        else:
          continue

        # Add the RunNumber to the newly inserted files
        lfnChunks = breakListIntoChunks( addedLfns, 1000 )
        for addedLfns in lfnChunks:
            start = time.time()
            res = self.bkClient.getFileMetadata( addedLfns )
            rtime = time.time() - start
            self.__logVerbose( "BK query time for metadata: %.2f seconds." % ( rtime ) )
            if not res['OK']:
              self.__logError( "Failed to get BK metadata", res['Message'] )
            else:
              runDict = {}
              for lfn, metadata in res['Value'].items():
                runID = metadata.get( 'RunNumber', None )
                if runID:
                  runDict.setdefault( int( runID ), [] ).append( lfn )
              for runID in sortList( runDict.keys() ):
                lfns = runDict[runID]
                self.__logVerbose( "Associating %d files to run %d" % ( len( lfns ), runID ) )
                res = self.transClient.addTransformationRunFiles( transID, runID, sortList( lfns ) )
                if not res['OK']:
                  self.__logWarn( "Failed to associate files to run", res['Message'] )
    self.__dumpLog()
    return S_OK()
