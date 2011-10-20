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
    self.debug = self.am_getOption( 'verbosePlugin', False )
    self.threadPool = ThreadPool( self.maxNumberOfThreads,
                                            self.maxNumberOfThreads )
    self.transQueue = Queue.Queue()
    self.transInQueue = []
    self.lock = threading.Lock()
    self.__readCache()
    # Validity of the cache in days
    self.replicaCacheValidity = 2
    self.log.debug( "*************************************************" )
    self.log.debug( "Hello! This is the LHCbDirac TransformationAgent!" )
    self.log.debug( "*************************************************" )
    self.log.info( "Multithreaded with %d threads" % self.maxNumberOfThreads )
    for i in xrange( self.maxNumberOfThreads ):
      self.threadPool.generateJobAndQueueIt( self._execute )
    return S_OK()

  def __logVerbose( self, message, param = '', method = "execute", transID = 'None' ):
    gLogger.verbose( AGENT_NAME + "." + method + ": [%s] " % str( transID ) + message, param )

  def __logDebug( self, message, param = '', method = "execute", transID = 'None' ):
    gLogger.debug( AGENT_NAME + "." + method + ": [%s] " % str( transID ) + message, param )

  def __logInfo( self, message, param = '', method = "execute", transID = 'None' ):
    gLogger.info( AGENT_NAME + "." + method + ": [%s] " % str( transID ) + message, param )

  def __logWarn( self, message, param = '', method = "execute", transID = 'None' ):
    gLogger.warn( AGENT_NAME + "." + method + ": [%s] " % str( transID ) + message, param )

  def __logError( self, message, param = '', method = "execute", transID = 'None' ):
    gLogger.error( AGENT_NAME + "." + method + ": [%s] " % str( transID ) + message, param )

  def execute( self ):
    # Get the transformations to process
    res = self.getTransformations()
    if not res['OK']:
      self.__logError( "Failed to obtain transformations: %s" % ( res['Message'] ) )
      return S_OK()
    # Process the transformations
    for transDict in res['Value']:
      transID = long( transDict['TransformationID'] )
      if transID not in self.transInQueue:
        self.transInQueue.append( transID )
        self.transQueue.put( transDict )

    return S_OK()

  def _execute( self ):

    while True:
      transDict = self.transQueue.get()
      transID = long( transDict['TransformationID'] )
      self.__logInfo( "Processing transformation %s." % transID, transID = transID )
      startTime = time.time()
      res = self.processTransformation( transDict )
      if not res['OK']:
        self.__logInfo( "Failed to process transformation: %s" % res['Message'], transID = transID )
      else:
        self.__logInfo( "Processed transformation in %.1f seconds" % ( time.time() - startTime ), transID = transID )
      self.transInQueue.remove( transID )
    return S_OK()

  def __getDataReplicas( self, transID, lfns, active = True ):
    self.__logVerbose( "Getting replicas for %d files" % len( lfns ), method = '__getDataReplicas', transID = transID )
    self.lock.acquire()
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
    self.lock.release()
    if dataReplicas:
      self.__logVerbose( "ReplicaCache hit for %d out of %d LFNs" % ( len( dataReplicas ), len( lfns ) ), method = '__getDataReplicas', transID = transID )
    newLFNs += [lfn for lfn in lfns if lfn not in foundReplicas]
    if newLFNs:
      self.__logVerbose( "Getting replicas for %d files from catalog" % len( newLFNs ), method = '__getDataReplicas', transID = transID )
      res = DIRACTransformationAgent.__getDataReplicas( self, transID, newLFNs, active = active )
      if res['OK']:
        newReplicas = res['Value']
        self.lock.acquire()
        self.replicaCache.setdefault( transID, {} )[datetime.datetime.utcnow()] = newReplicas
        self.lock.release()
        dataReplicas.update( newReplicas )
    self.__cleanCache()
    return S_OK( dataReplicas )

  def __cleanCache( self ):
    self.lock.acquire()
    timeLimit = datetime.datetime.utcnow() - datetime.timedelta( days = self.replicaCacheValidity )
    for transID in [transID for transID in self.replicaCache]:
      for updateTime in self.replicaCache[transID].copy():
        if updateTime < timeLimit or not self.replicaCache[transID][updateTime]:
          self.__logVerbose( "Clear %d cached replicas for transformation %s" % ( len( self.replicaCache[transID][updateTime] ), str( transID ) ), method = '__cleanCache' )
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
    if self.debug:
      oplugin.setDebug()
    return S_OK( oplugin )

  def pluginCallback( self, transID, invalidateCache = False ):
    if invalidateCache:
      self.lock.acquire()
      self.__readCache( lock = False )
      if transID in self.replicaCache:
        self.__logInfo( "Removed cached replicas for transformation" , method = 'pluginCallBack', transID = transID )
        self.replicaCache.pop( transID )
        self.__writeCache( lock = False )
      self.lock.release()

