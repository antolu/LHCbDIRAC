"""  TransformationAgent is and LHCb class just for overwriting some of the DIRAC methods
"""

__RCSID__ = "$Id: TransformationAgent.py 43068 2011-09-28 16:21:29Z phicharp $"


from DIRAC                                                             import gLogger, S_OK
import os, datetime, pickle, Queue, time, threading
from DIRAC.Core.Utilities.ThreadPool                                      import ThreadPool
from DIRAC.TransformationSystem.Agent.TransformationAgent              import TransformationAgent as DIRACTransformationAgent
from DIRAC.TransformationSystem.Agent.TransformationAgent              import AGENT_NAME

class TransformationAgent( DIRACTransformationAgent ):
  def initialize( self ):
    DIRACTransformationAgent.initialize( self )
    self.workDirectory = self.am_getWorkDirectory()
    self.cacheFile = os.path.join( self.workDirectory, 'ReplicaCache.pkl' )
    # Get it threaded
    self.maxNumberOfThreads = self.am_getOption( 'maxThreadsInPool', 1 )
    self.threadPool = ThreadPool( self.maxNumberOfThreads,
                                            self.maxNumberOfThreads )
    self.transQueue = Queue.Queue()
    self.lock = threading.Lock()
    self.__readCache()
    # Validity of the cache in days
    self.replicaCacheValidity = 2
    self.log.debug( "*************************************************" )
    self.log.debug( "Hello! This is the LHCbDirac TransformationAgent!" )
    self.log.debug( "*************************************************" )
    self.log.info( "Multithreaded with %d threads" % self.maxNumberOfThreads )
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

  def execute( self ):
    # Get the transformations to process
    res = self.getTransformations()
    if not res['OK']:
      self.__logInfo( "Failed to obtain transformations: %s" % ( res['Message'] ) )
      return S_OK()
    # Process the transformations
    for transDict in res['Value']:
      self.transQueue.put( transDict )
    for i in xrange( self.maxNumberOfThreads ):
      self.threadPool.generateJobAndQueueIt( self._execute )

    while not self.transQueue.empty():
      time.sleep( 1 )

    return S_OK()

  def _execute( self ):

    while not self.transQueue.empty():
      transDict = self.transQueue.get()
      transID = long( transDict['TransformationID'] )
      self.__logInfo( "[%d] Processing transformation %s." % ( transID, transID ) )
      startTime = time.time()
      res = self.processTransformation( transDict )
      if not res['OK']:
        self.__logInfo( "[%d] Failed to process transformation: %s" % ( transID, res['Message'] ) )
      else:
        self.__logInfo( "[%d] Processed transformation in %.1f seconds" % ( transID, time.time() - startTime ) )
    return S_OK()

  def __getDataReplicas( self, transID, lfns, active = True ):
    self.log.verbose( "Getting replicas for %d files" % len( lfns ) )
    cachedReplicaSets = self.replicaCache.get( transID, {} )
    dataReplicas = {}
    newLFNs = []
    foundReplicas = []
    for set in cachedReplicaSets:
      cachedReplicas = cachedReplicaSets[set]
      for lfn in [lfn for lfn in lfns if lfn in cachedReplicas]:
        dataReplicas[lfn] = cachedReplicas[lfn]
        foundReplicas.append( lfn )
      # Remove files from the cache that are not in the required list
      for lfn in [lfn for lfn in cachedReplicas if lfn not in lfns]:
        self.replicaCache[transID][set].pop( lfn )
    if dataReplicas:
      self.log.verbose( "ReplicaCache hit for %d out of %d LFNs" % ( len( dataReplicas ), len( lfns ) ) )
    newLFNs += [lfn for lfn in lfns if lfn not in foundReplicas]
    if newLFNs:
      self.log.verbose( "Getting replicas for %d files from catalog" % len( newLFNs ) )
      res = DIRACTransformationAgent.__getDataReplicas( self, transID, newLFNs, active = active )
      if res['OK']:
        newReplicas = res['Value']
        self.replicaCache.setdefault( transID, {} )[datetime.datetime.utcnow()] = newReplicas
        dataReplicas.update( newReplicas )
    self.__cleanCache()
    return S_OK( dataReplicas )

  def __cleanCache( self ):
    self.lock.acquire()
    timeLimit = datetime.datetime.utcnow() - datetime.timedelta( days = self.replicaCacheValidity )
    for transID in [transID for transID in self.replicaCache]:
      for updateTime in self.replicaCache[transID].copy():
        if updateTime < timeLimit or not self.replicaCache[transID][updateTime]:
          self.log.verbose( "Clear %d cached replicas for transformation %s" % ( len( self.replicaCache[transID][updateTime] ), str( transID ) ) )
          self.replicaCache[transID].pop( updateTime )
      # Remove empty transformations
      if not self.replicaCache[transID]:
        self.replicaCache.pop( transID )
    self.__writeCache( lock = False )
    self.lock.release()

  def __readCache( self, lock = True ):
    if lock: self.lock.acquire()
    try:
      f = open( self.cacheFile, 'r' )
      self.replicaCache = pickle.load( f )
      f.close()
      self.log.verbose( "Successfully loaded replica cache from file %s" % self.cacheFile )
    except:
      self.log.verbose( "Failed to load replica cache from file %s" % self.cacheFile )
      self.replicaCache = {}
    if lock: self.lock.release()

  def __writeCache( self, lock = True ):
    if lock: self.lock.acquire()
    try:
      f = open( self.cacheFile, 'w' )
      pickle.dump( self.replicaCache, f )
      f.close()
      self.log.verbose( "Successfully wrote replica cache file %s" % self.cacheFile )
    except:
      self.log.error( "Could not write replica cache file %s" % self.cacheFile )
    if lock: self.lock.release()

  def __generatePluginObject( self, plugin ):
    res = DIRACTransformationAgent.__generatePluginObject( self, plugin )
    if not res['OK']:
      return res
    oplugin = res['Value']
    oplugin.setDirectory( self.workDirectory )
    oplugin.setCallback( self.pluginCallback )
    oplugin.setDebug()
    return S_OK( oplugin )

  def pluginCallback( self, transID, invalidateCache = False ):
    if invalidateCache:
      self.lock.acquire()
      self.__readCache( lock = False )
      if transID in self.replicaCache:
        self.log.info( "Removed cached replicas for transformation %s" % str( transID ) )
        self.replicaCache.pop( transID )
        self.__writeCache( lock = False )
      self.lock.release()

