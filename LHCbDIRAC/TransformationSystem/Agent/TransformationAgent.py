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
      self.log.info( "Wait for queue to get empty before terminating the agent (%d tasks)" % len( self.transInQueue ) )
      while self.transInQueue:
        time.sleep( 2 )
      self.log.info( "Queue is empty, terminating the agent..." )
    return S_OK()

  @classmethod
  def __logVerbose( self, message, param='', method="execute", transID='None' ):
    """ verbose """
    gLogger.verbose( AGENT_NAME + "." + method + ": [%s] " % str( transID ) + message, param )

  @classmethod
  def __logDebug( self, message, param='', method="execute", transID='None' ):
    """ debug """
    gLogger.debug( AGENT_NAME + "." + method + ": [%s] " % str( transID ) + message, param )

  @classmethod
  def __logInfo( self, message, param='', method="execute", transID='None' ):
    """ info """
    gLogger.info( AGENT_NAME + "." + method + ": [%s] " % str( transID ) + message, param )

  @classmethod
  def __logWarn( self, message, param='', method="execute", transID='None' ):
    """ warn """
    gLogger.warn( AGENT_NAME + "." + method + ": [%s] " % str( transID ) + message, param )

  @classmethod
  def __logError( self, message, param='', method="execute", transID='None' ):
    """ error """
    gLogger.error( AGENT_NAME + "." + method + ": [%s] " % str( transID ) + message, param )

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
      if transID not in self.transInQueue:
        count += 1
        self.transInQueue.append( transID )
        self.transQueue.put( transDict )
    self.log.info( "Out of %d transformations, %d put in thread queue" % ( len( res['Value'] ), count ) )
    return S_OK()

  def _execute( self ):
    """ thread - does the real job
    """
    while True:
      transDict = self.transQueue.get()
      try:
        transID = long( transDict['TransformationID'] )
        self.__logInfo( "Processing transformation %s." % transID, transID=transID )
        startTime = time.time()
        res = self.processTransformation( transDict )
        if not res['OK']:
          self.__logInfo( "Failed to process transformation: %s" % res['Message'], transID=transID )
      except Exception, x:
        gLogger.exception( '[%s] %s.execute %s' % ( str( transID ), AGENT_NAME, x ) )
      finally:
        self.__logInfo( "Processed transformation in %.1f seconds" % ( time.time() - startTime ), transID=transID )
        if transID in self.transInQueue:
          self.transInQueue.remove( transID )
    return S_OK()

  def __getDataReplicas( self, transID, lfns, active=True ):
    """ Redefine base class one
    """
    startTime = time.time()
    self.__logVerbose( "Getting replicas for %d files" % len( lfns ), method='__getDataReplicas', transID=transID )
    self.lock.acquire()
    dataReplicas = {}
    try:
      cachedReplicaSets = self.replicaCache.get( transID, {} )
      for crs, cachedReplicas in cachedReplicaSets.items():
        for lfn in [lfn for lfn in lfns if lfn in cachedReplicas]:
          dataReplicas[lfn] = cachedReplicas[lfn]
        # Remove files from the cache that are not in the required list
        for lfn in [lfn for lfn in cachedReplicas if lfn not in lfns]:
          self.replicaCache[transID][crs].pop( lfn )
    except:
      pass
    finally:
      self.lock.release()
    if dataReplicas:
      self.__logVerbose( "ReplicaCache hit for %d out of %d LFNs" % ( len( dataReplicas ), len( lfns ) ),
                         method='__getDataReplicas', transID=transID )
    newLFNs = [lfn for lfn in lfns if lfn not in dataReplicas]
    if newLFNs:
      self.__logVerbose( "Getting replicas for %d files from catalog" % len( newLFNs ),
                         method='__getDataReplicas', transID=transID )
      res = DIRACTransformationAgent.__getDataReplicas( self, transID, newLFNs, active=active )
      if res['OK']:
        newReplicas = {}
        noReplicas = []
        for lfn in res['Value']:
          if res['Value'][lfn]:
            newReplicas[lfn] = res['Value'][lfn]
          else:
            noReplicas.append( lfn )
        if noReplicas:
          self.__logWarn( "Found %d files without replicas" % len( noReplicas ) )
        self.lock.acquire()
        self.replicaCache.setdefault( transID, {} )[datetime.datetime.utcnow()] = newReplicas
        self.lock.release()
        dataReplicas.update( newReplicas )
      else:
        self.__logWarn( "Failed to get replicas for %d files" % len( newLFNs ), res['Message'] )
    self.__cleanCache()
    self.__logInfo( "Obtained %d replicas in %.1f seconds" % ( len( dataReplicas ), time.time() - startTime ), method='__getDataReplicas' )
    return S_OK( dataReplicas )

  def __cleanCache( self ):
    """ Cleans the cache
    """
    self.lock.acquire()
    try:
      timeLimit = datetime.datetime.utcnow() - datetime.timedelta( days=self.replicaCacheValidity )
      for transID in [transID for transID in self.replicaCache]:
        for updateTime in self.replicaCache[transID].copy():
          if updateTime < timeLimit or not self.replicaCache[transID][updateTime]:
            self.__logVerbose( "Clear %d cached replicas for transformation %s" % ( len( self.replicaCache[transID][updateTime] ),
                                                                                    str( transID ) ), method='__cleanCache' )
            self.replicaCache[transID].pop( updateTime )
        # Remove empty transformations
        if not self.replicaCache[transID]:
          self.replicaCache.pop( transID )
      self.__writeCache( lock=False )
    except:
      pass
    finally:
      self.lock.release()

  def __readCache( self, lock=True ):
    """ Reads from the cache
    """
    if lock:
      self.lock.acquire()
    try:
      cacheFile = open( self.cacheFile, 'r' )
      self.replicaCache = pickle.load( cacheFile )
      cacheFile.close()
      self.log.verbose( "Successfully loaded replica cache from file %s" % self.cacheFile )
    except:
      self.log.verbose( "Failed to load replica cache from file %s" % self.cacheFile )
      self.replicaCache = {}
    finally:
      if lock:
        self.lock.release()

  def __writeCache( self, lock=True ):
    """ Writes the cache
    """
    if lock: self.lock.acquire()
    try:
      f = open( self.cacheFile, 'w' )
      pickle.dump( self.replicaCache, f )
      f.close()
      self.log.verbose( "Successfully wrote replica cache file %s" % self.cacheFile )
    except:
      self.log.error( "Could not write replica cache file %s" % self.cacheFile )
    finally:
      if lock: self.lock.release()

  def __generatePluginObject( self, plugin ):
    """ Generates the plugin object
    """
    try:
      plugModule = __import__( self.pluginLocation, globals(), locals(), ['TransformationPlugin'] )
    except Exception, x:
      gLogger.exception( "%s.__generatePluginObject: Failed to import 'TransformationPlugin'" % AGENT_NAME, '', x )
      return S_ERROR()
    try:
      oPlugin = getattr( plugModule, 'TransformationPlugin' )( '%s' % plugin,
                                                               replicaManager=self.rm,
                                                               transClient=self.transClient,
                                                               bkkClient=self.bkk,
                                                               rmClient=self.rmClient,
                                                               rss=self.resourceStatus )
    except Exception, x:
      gLogger.exception( "%s.__generatePluginObject: Failed to create %s()." % ( AGENT_NAME, plugin ), '', x )
      return S_ERROR()
    oPlugin.setDirectory( self.workDirectory )
    oPlugin.setCallback( self.pluginCallback )
    if self.debug:
      oPlugin.setDebug()
    return S_OK( oPlugin )

  def pluginCallback( self, transID, invalidateCache=False ):
    """ Standard plugin callback
    """
    if invalidateCache:
      self.lock.acquire()
      try:
        self.__readCache( lock=False )
        if transID in self.replicaCache:
          self.__logInfo( "Removed cached replicas for transformation" , method='pluginCallBack', transID=transID )
          self.replicaCache.pop( transID )
          self.__writeCache( lock=False )
      except:
        pass
      finally:
        self.lock.release()

