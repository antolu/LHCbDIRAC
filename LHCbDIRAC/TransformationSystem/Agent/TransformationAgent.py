"""  TransformationAgent is and LHCb class just for overwriting some of the DIRAC methods
"""

__RCSID__ = "$Id: TransformationAgent.py 43068 2011-09-28 16:21:29Z phicharp $"


from DIRAC import gLogger, S_OK, S_ERROR
import os, datetime, pickle, Queue, time, threading
from DIRAC.Core.Utilities.ThreadPool import ThreadPool
from DIRAC.TransformationSystem.Agent.TransformationAgent import TransformationAgent as DIRACTransformationAgent
from DIRAC.TransformationSystem.Agent.TransformationAgent import AGENT_NAME
from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager
from DIRAC.ResourceStatusSystem.Client.ResourceStatus import ResourceStatus

from LHCbDIRAC.TransformationSystem.Client.TransformationClient import TransformationClient
from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
from LHCbDIRAC.ResourceStatusSystem.Client.ResourceManagementClient import ResourceManagementClient
from DIRAC.Core.Utilities.List import breakListIntoChunks

class TransformationAgent( DIRACTransformationAgent ):
  """ Extends base class
  """

  def __init__( self, agentName, loadName, baseAgentName=False, properties=dict() ):
    """ c'tor

    :param self: self reference
    :param str agentName: name of agent
    :param bool baseAgentName: whatever
    :param dict properties: whatever else
    """
    DIRACTransformationAgent.__init__( self, agentName, loadName, baseAgentName, properties )

    self.transQueue = Queue.Queue()
    self.transInQueue = []
    self.transInThread = {}
    self.lock = threading.Lock()

    self.rm = ReplicaManager()
    self.transClient = TransformationClient()
    self.resourceStatus = ResourceStatus()
    self.bkk = BookkeepingClient()
    self.rmClient = ResourceManagementClient()

    self.workDirectory = self.am_getWorkDirectory()
    self.cacheFile = os.path.join( self.workDirectory, 'ReplicaCache.pkl' )
    self.debug = self.am_getOption( 'verbosePlugin', False )

    # Validity of the cache in days
    self.replicaCacheValidity = 2
    self.writingCache = False
    self.__readCache()

  def initialize( self ):
    """ Augments base class initialize
    """
    DIRACTransformationAgent.initialize( self )
    # Get it threaded
    maxNumberOfThreads = self.am_getOption( 'maxThreadsInPool', 1 )
    threadPool = ThreadPool( maxNumberOfThreads, maxNumberOfThreads )
    self.log.debug( "*************************************************" )
    self.log.debug( "Hello! This is the LHCbDirac TransformationAgent!" )
    self.log.debug( "*************************************************" )
    self.log.info( "Multithreaded with %d threads" % maxNumberOfThreads )
    for _i in xrange( maxNumberOfThreads ):
      threadPool.generateJobAndQueueIt( self._execute )
    return S_OK()

  def finalize( self ):
    if self.transInQueue:
      self.log.info( "Wait for threads to get empty before terminating the agent (%d tasks)" % len( self.transInThread ) )
      self.transInQueue = []
      while self.transInThread:
        time.sleep( 2 )
      self.log.info( "Threads are empty, terminating the agent..." )
    self.__writeCache( force=True )
    return S_OK()

  def __threadForTrans( self, transID ):
    return self.transInThread.get( transID, ' [None] [None] ' ) + AGENT_NAME + '.'

  def __logVerbose( self, message, param='', method="execute", transID='None' ):
    """ verbose """
    if self.debug:
      gLogger.info( '(V) ' + self.__threadForTrans( transID ) + method + ' ' + message, param )
    else:
      gLogger.verbose( self.__threadForTrans( transID ) + method + ' ' + message, param )

  @classmethod
  def __logDebug( self, message, param='', method="execute", transID='None' ):
    """ debug """
    gLogger.debug( self.__threadForTrans( transID ) + method + ' ' + message, param )

  @classmethod
  def __logInfo( self, message, param='', method="execute", transID='None' ):
    """ info """
    gLogger.info( self.__threadForTrans( transID ) + method + ' ' + message, param )

  @classmethod
  def __logWarn( self, message, param='', method="execute", transID='None' ):
    """ warn """
    gLogger.warn( self.__threadForTrans( transID ) + method + ' ' + message, param )

  @classmethod
  def __logError( self, message, param='', method="execute", transID='None' ):
    """ error """
    gLogger.error( self.__threadForTrans( transID ) + method + ' ' + message, param )

  def __logException( self, message, param='', lException=False, method="execute", transID='None' ):
    gLogger.exception( self.__threadForTrans( transID ) + method + ' ' + message, param, lException )

  def execute( self ):
    """ Just puts threads in the queue
    """
    # Get the transformations to process
    res = self.getTransformations()
    if not res['OK']:
      self.__logError( "Failed to obtain transformations: %s" % ( res['Message'] ) )
      return S_OK()
    # Process the transformations
    count = 0
    for transDict in res['Value']:
      transID = long( transDict['TransformationID'] )
      if transDict['Status'] == 'Completing':
        # Try and move datasets from the ancestor production
        res = self.transClient.moveFilesToDerivedTransformation( transID )
        if not res['OK']:
          self.log.error( "Error moving files from an inherited transformation", res['Message'] )
        continue
      if transID not in self.transInQueue:
        count += 1
        self.transInQueue.append( transID )
        self.transQueue.put( transDict )
    self.log.info( "Out of %d transformations, %d put in thread queue" % ( len( res['Value'] ), count ) )
    return S_OK()

  def _execute( self, threadID ):
    """ thread - does the real job
    """
    while True:
      transDict = self.transQueue.get()
      try:
        transID = long( transDict['TransformationID'] )
        if transID not in self.transInQueue:
          break
        self.transInThread[transID] = ' [Thread%d] [%s] ' % ( threadID, str( transID ) )
        self.__logInfo( "Processing transformation %s." % transID, transID=transID )
        startTime = time.time()
        res = self.processTransformation( transDict )
        if not res['OK']:
          self.__logInfo( "Failed to process transformation: %s" % res['Message'], transID=transID )
      except Exception:
        self.__logException( '', transID=transID )
      finally:
        if not transID:
          transID = 'None'
        self.__logInfo( "Processed transformation in %.1f seconds" % ( time.time() - startTime ), transID=transID )
        self.__logVerbose( "%d transformations still in queue" % ( len( self.transInQueue ) - 1 ) )
        self.transInThread.pop( transID, None )
        if transID in self.transInQueue:
          self.transInQueue.remove( transID )
    return S_OK()

  def __getDataReplicas( self, transID, lfns, active=True ):
    """ Redefine base class one
    """
    method = '__getDataReplicas'
    startTime = time.time()
    dataReplicas = {}
    self.__logVerbose( "Getting replicas for %d files" % len( lfns ), method=method, transID=transID )
    lfns.sort()
    newLFNs = []
    nLfns = len( lfns )
    self.__acquireLock( transID=transID, method=method )
    try:
      cachedReplicaSets = self.replicaCache.get( transID, {} )
      cachedReplicas = {}
      # Merge all sets of replicas
      for crs in cachedReplicaSets:
        cachedReplicas.update( cachedReplicaSets[crs] )
      self.__logVerbose( "Number of cached replicas: %d" % len( cachedReplicas ), method=method, transID=transID )
      # Sorted browsing
      for cacheLfn in sorted( cachedReplicas ):
        while lfns and lfns[0] < cacheLfn:
          # All files until cacheLfn are new
          newLFNs.append( lfns.pop( 0 ) )
        if lfns:
          if lfns[0] == cacheLfn:
            # We found a match, copy and go to next cache
            lfn = lfns.pop( 0 )
            dataReplicas[lfn] = sorted( cachedReplicas[lfn] )
            continue
        if not lfns or lfns[0] > cacheLfn:
          # Remove files from the cache that are not in the required list
          for crs in cachedReplicaSets:
            cachedReplicaSets[crs].pop( cacheLfn, None )
      # Add what is left as new files
      newLFNs += lfns
    except Exception:
      self.__logException( "Exception when browsing cache", method=method, transID=transID )
    finally:
      self.__releaseLock( transID=transID, method=method )
      self.__logVerbose( "Lock released after %.1f seconds" % ( time.time() - startTime ), method=method, transID=transID )
    self.__logVerbose( "ReplicaCache hit for %d out of %d LFNs" % ( len( dataReplicas ), nLfns ),
                        method=method, transID=transID )
    if newLFNs:
      startTime = time.time()
      self.__logVerbose( "Getting replicas for %d files from catalog" % len( newLFNs ),
                         method=method, transID=transID )
      newReplicas = {}
      noReplicas = []
      for chunk in breakListIntoChunks( newLFNs, 1000 ):
        res = DIRACTransformationAgent.__getDataReplicas( self, transID, chunk, active=active )
        if res['OK']:
          for lfn in res['Value']:
            if res['Value'][lfn]:
              # Keep only the list of SEs as SURLs are useless
              newReplicas[lfn] = sorted( res['Value'][lfn] )
            else:
              noReplicas.append( lfn )
        else:
          self.__logWarn( "Failed to get replicas for %d files" % len( chunk ), res['Message'],
                          method=method, transID=transID )
      if noReplicas:
        self.__logWarn( "Found %d files without replicas" % len( noReplicas ),
                         method=method, transID=transID )
      self.__acquireLock( transID=transID, method=method )
      self.replicaCache.setdefault( transID, {} )[datetime.datetime.utcnow()] = newReplicas
      self.__releaseLock( transID=transID, method=method )
      dataReplicas.update( newReplicas )
      self.__logInfo( "Obtained %d replicas from catalog in %.1f seconds" \
                      % ( len( newReplicas ), time.time() - startTime ),
                      method=method, transID=transID )
    self.__cleanCache()
    return S_OK( dataReplicas )

  def __acquireLock( self, transID='None', method='None' ):
    self.lock.acquire()
    self.__logVerbose( "Lock acquired", method=method, transID=transID )
    return True
  def __releaseLock( self, transID='None', method='None' ):
    self.__logVerbose( "Lock released", method=method, transID=transID )
    self.lock.release()
    return False

  def __cleanCache( self ):
    """ Cleans the cache
    """
    method = 'cleanCache'
    self.__acquireLock( method=method )
    try:
      timeLimit = datetime.datetime.utcnow() - datetime.timedelta( days=self.replicaCacheValidity )
      for transID in self.replicaCache.keys():
        for updateTime in self.replicaCache[transID].keys():
          if updateTime < timeLimit or not self.replicaCache[transID][updateTime]:
            self.__logVerbose( "Clear %d cached replicas for transformation %s" % ( len( self.replicaCache[transID][updateTime] ),
                                                                                    str( transID ) ), method='__cleanCache' )
            self.replicaCache[transID].pop( updateTime )
        # Remove empty transformations
        if not self.replicaCache[transID]:
          self.replicaCache.pop( transID )
    except Exception:
      self.__logException( "Exception when cleaning replica cache:" )
    finally:
      self.__releaseLock( method=method )

    # Write the cache file
    try:
      self.__writeCache()
    except Exception:
      self.__logException( "While writing replica cache" )

  def __readCache( self ):
    """ Reads from the cache
    """
    try:
      cacheFile = open( self.cacheFile, 'r' )
      self.replicaCache = pickle.load( cacheFile )
      cacheFile.close()
      self.__logInfo( "Successfully loaded replica cache from file %s" % self.cacheFile )
    except Exception:
      self.__logException( "Failed to load replica cache from file %s" % self.cacheFile, method='__readCache' )
      self.replicaCache = {}

  def __writeCache( self, force=False ):
    """ Writes the cache
    """
    now = datetime.datetime.now()
    if ( now - self.dateWriteCache ) < datetime.timedelta( minutes=60 ) and not force:
      return
    while force and self.writingCache:
      #If writing is forced, wait until the previous write is over
      time.sleep( 10 )
    try:
      startTime = time.time()
      self.dateWriteCache = now
      # Protect the copy of the cache
      method = '__writeCache'
      self.__acquireLock( method=method )
      lock = True
      if self.writingCache:
        return
      self.writingCache = True
      tmpCache = self.replicaCache.copy()
      self.__releaseLock( method=method )
      lock = False
      # write to a temporary file in order to avoid corrupted files
      tmpFile = self.cacheFile + '.tmp'
      f = open( tmpFile, 'w' )
      pickle.dump( tmpCache, f )
      f.close()
      # Now rename the file as it shold
      os.rename( tmpFile, self.cacheFile )
      self.__logVerbose( "Successfully wrote replica cache file %s in %.1f seconds" \
                        % ( self.cacheFile, time.time() - startTime ), method=method )
    except Exception:
      self.__logException( "Could not write replica cache file %s" % self.cacheFile, method=method )
    finally:
      if lock:
        self.__releaseLock( method=method )

    self.writingCache = False

  def __generatePluginObject( self, plugin ):
    """ Generates the plugin object
    """
    try:
      plugModule = __import__( self.pluginLocation, globals(), locals(), ['TransformationPlugin'] )
    except Exception:
      gLogger.exception( "%s.__generatePluginObject: Failed to import 'TransformationPlugin'" % AGENT_NAME )
      return S_ERROR()
    try:
      oPlugin = getattr( plugModule, 'TransformationPlugin' )( '%s' % plugin,
                                                               replicaManager=self.rm,
                                                               transClient=self.transClient,
                                                               bkkClient=self.bkk,
                                                               rmClient=self.rmClient,
                                                               rss=self.resourceStatus,
                                                               transInThread=self.transInThread )
    except Exception:
      gLogger.exception( "%s.__generatePluginObject: Failed to create %s()." % ( AGENT_NAME, plugin ) )
      return S_ERROR()
    oPlugin.setDirectory( self.workDirectory )
    oPlugin.setCallback( self.pluginCallback )
    if self.debug:
      oPlugin.setDebug()
    return S_OK( oPlugin )

  def pluginCallback( self, transID, invalidateCache=False ):
    """ Standard plugin callback
    """
    method = 'pluginCallback'
    save = False
    if invalidateCache:
      try:
        self.__acquireLock( transID=transID, method=method )
        if transID in self.replicaCache:
          self.__logInfo( "Removed cached replicas for transformation" , method='pluginCallBack', transID=transID )
          self.replicaCache.pop( transID )
          save = True
      except:
        pass
      finally:
        self.__releaseLock( transID=transID, method=method )

      if save:
        self.__writeCache()
