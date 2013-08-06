#####################################################################
# File: StorageUsageAgent.py
########################################################################
''' :mod: StorageUsageAgent
    =======================

    .. module: StorageUsageAgent
    :synopsis: StorageUsageAgent takes the LFC as the primary source of information to
    determine storage usage.
'''
# # imports
import time
import os
import re
import threading
# # from DIRAC
from DIRAC import gMonitor, S_OK, S_ERROR
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.ConfigurationSystem.Client.Helpers import Registry
from DIRAC.Core.Utilities.DirectoryExplorer import DirectoryExplorer
from DIRAC.FrameworkSystem.Client.ProxyManagerClient import gProxyManager
from DIRAC.DataManagementSystem.Client.ReplicaManager import CatalogDirectory
from DIRAC.Core.Utilities import List
from DIRAC.Core.Utilities.Time import timeInterval, dateTime, week
from DIRAC.Core.Utilities.DictCache import DictCache
from DIRAC.Core.DISET.RPCClient import RPCClient
# # from LHCbDIRAC
from LHCbDIRAC.DataManagementSystem.DB.StorageUsageDB import StorageUsageDB


__RCSID__ = "$Id$"

AGENT_NAME = "DataManagement/StorageUsageAgent"

class StorageUsageAgent( AgentModule ):
  ''' .. class:: StorageUsageAgent


  :param CatalogDirectory catalog: CatalogDIrectory instance
  :parma mixed storageUsage: StorageUsageDB instance or its rpc client
  :param int pollingTime: polling time
  :param int activePeriod: active period on weeks
  :param threading.Lock dataLock: data lock
  :param threading.Lock replicaListLock: replica list lock
  :param DictCache proxyCache: creds cache
  '''
  catalog = None
  storageUsage = None
  pollingTime = 43200
  activePeriod = 0
  dataLock = None  # threading.Lock()
  replicaListLock = None  # threading.Lock()
  proxyCache = None  # DictCache()

  def __init__( self, *args, **kwargs ):
    ''' c'tor
    '''
    AgentModule.__init__( self, *args, **kwargs )

    self.catalog = CatalogDirectory()
    if self.am_getOption( 'DirectDB', False ):
      self.storageUsage = StorageUsageDB()
    else:
      self.storageUsage = RPCClient( 'DataManagement/StorageUsage' )

    self.activePeriod = self.am_getOption( 'ActivePeriod', self.activePeriod )
    self.dataLock = threading.Lock()
    self.replicaListLock = threading.Lock()
    self.proxyCache = DictCache()
    self.__noProxy = set()

  def initialize( self ):
    ''' agent initialisation '''

    self.am_setOption( "PollingTime", self.pollingTime )

    # This sets the Default Proxy to used as that defined under
    # /Operations/Shifter/DataManager
    # the shifterProxy option in the Configsorteduration can be used to change this default.
    self.am_setOption( 'shifterProxy', 'DataManager' )

    return S_OK()

  def __writeReplicasListFiles( self, dirPathList ):
    ''' dump replicas list to files '''
    self.replicaListLock.acquire()
    try:
      self.log.info( "Dumping replicas for %s dirs" % len( dirPathList ) )
      result = self.catalog.getCatalogDirectoryReplicas( dirPathList )
      if not result[ 'OK' ]:
        self.log.error( "Could not get directory replicas", "%s -> %s" % ( dirPathList, result[ 'Message' ] ) )
        return result
      resData = result[ 'Value' ]
      filesOpened = {}
      for dirPath in dirPathList:
        if dirPath in result[ 'Value' ][ 'Failed' ]:
          self.log.error( "Could not get directory replicas", "%s -> %s" % ( dirPath, resData[ 'Failed' ][ dirPath ] ) )
          continue
        dirData = resData[ 'Successful' ][ dirPath ]
        for lfn in dirData:
          for seName in dirData[ lfn ]:
            if seName not in filesOpened:
              filePath = os.path.join( self.__replicaListFilesDir, "replicas.%s.%s.filling" % ( seName,
                                                                                                 self.__baseDirLabel ) )
              # Check if file is opened and if not open it
              if seName not in filesOpened:
                if seName not in self.__replicaFilesUsed:
                  self.__replicaFilesUsed.add( seName )
                  filesOpened[ seName ] = file( filePath, "w" )
                else:
                  filesOpened[ seName ] = file( filePath, "a" )
            # seName file is opened. Write
            filesOpened[ seName ].write( "%s -> %s\n" % ( lfn, dirData[ lfn ][ seName ] ) )
      # Close the files
      for seName in filesOpened:
        filesOpened[ seName ].close()
      return S_OK()
    finally:
      self.replicaListLock.release()

  def __resetReplicaListFiles( self ):
    ''' prepare directories for replica list files '''
    self.__replicaFilesUsed = set()
    self.__replicaListFilesDir = os.path.join( self.am_getOption( "WorkDirectory" ), "replicaLists" )
    if not os.path.isdir( self.__replicaListFilesDir ):
      os.makedirs( self.__replicaListFilesDir )
    self.log.info( "Replica Lists directory is %s" % self.__replicaListFilesDir )

  def __replicaListFilesDone( self ):
    ''' rotate replicas list files '''
    self.replicaListLock.acquire()
    try:
      old = re.compile( "^replicas\.([a-zA-Z0-9\-_]*)\.%s\.old$" % self.__baseDirLabel )
      current = re.compile( "^replicas\.([a-zA-Z0-9\-_]*)\.%s$" % self.__baseDirLabel )
      filling = re.compile( "^replicas\.([a-zA-Z0-9\-_]*)\.%s\.filling$" % self.__baseDirLabel )
      # Delete old
      for fileName in os.listdir( self.__replicaListFilesDir ):
        match = old.match( fileName )
        if match:
          os.unlink( os.path.join( self.__replicaListFilesDir, fileName ) )
      # Current -> old
      for fileName in os.listdir( self.__replicaListFilesDir ):
        match = current.match( fileName )
        if match:
          newFileName = "replicas.%s.%s.old" % ( match.group( 1 ), self.__baseDirLabel )
          self.log.info( "Moving \n %s\n to \n %s" % ( os.path.join( self.__replicaListFilesDir, fileName ),
                                                       os.path.join( self.__replicaListFilesDir, newFileName ) ) )
          os.rename( os.path.join( self.__replicaListFilesDir, fileName ),
                     os.path.join( self.__replicaListFilesDir, newFileName ) )
      # filling to current
      for fileName in os.listdir( self.__replicaListFilesDir ):
        match = filling.match( fileName )
        if match:
          newFileName = "replicas.%s.%s" % ( match.group( 1 ), self.__baseDirLabel )
          self.log.info( "Moving \n %s\n to \n %s" % ( os.path.join( self.__replicaListFilesDir, fileName ),
                                                       os.path.join( self.__replicaListFilesDir, newFileName ) ) )
          os.rename( os.path.join( self.__replicaListFilesDir, fileName ),
                     os.path.join( self.__replicaListFilesDir, newFileName ) )

      return S_OK()
    finally:
      self.replicaListLock.release()

  def __printSummary( self ):
    ''' pretty print summary '''
    res = self.storageUsage.getStorageSummary()
    if res['OK']:
      self.log.notice( "Storage Usage Summary" )
      self.log.notice( "============================================================" )
      self.log.notice( "%-40s %20s %20s" % ( 'Storage Element', 'Number of files', 'Total size' ) )
      for se in sorted( res['Value'] ):
        usage = res['Value'][se]['Size']
        files = res['Value'][se]['Files']
        site = se.split( '_' )[0].split( '-' )[0]
        self.log.notice( "%-40s %20s %20s" % ( se, str( files ), str( usage ) ) )
        gMonitor.registerActivity( "%s-used" % se, "%s usage" % se, "StorageUsage/%s usage" % site,
                                   "", gMonitor.OP_MEAN, bucketLength = 600 )
        gMonitor.addMark( "%s-used" % se, usage )
        gMonitor.registerActivity( "%s-files" % se, "%s files" % se, "StorageUsage/%s files" % site,
                                   "Files", gMonitor.OP_MEAN, bucketLength = 600 )
        gMonitor.addMark( "%s-files" % se, files )

  def execute( self ):
    ''' execution in one cycle '''
    self.__publishDirQueue = {}
    self.__dirsToPublish = {}
    self.__baseDir = self.am_getOption( 'BaseDirectory', '/lhcb' )
    self.__baseDirLabel = "_".join( List.fromChar( self.__baseDir, "/" ) )
    self.__ignoreDirsList = self.am_getOption( 'Ignore', [] )
    self.__keepDirLevels = self.am_getOption( "KeepDirLevels", 4 )

    self.__startExecutionTime = long( time.time() )
    self.__dirExplorer = DirectoryExplorer( reverse = True )
    self.__resetReplicaListFiles()
    self.__noProxy = set()

    self.__printSummary()

    self.__dirExplorer.addDir( self.__baseDir )
    self.log.notice( "Initiating with %s as base directory." % self.__baseDir )
    # Loop over all the directories and sub-directories
    totalIterTime = 0.0
    numIterations = 0.0
    iterMaxDirs = 100
    while self.__dirExplorer.isActive():
      startT = time.time()
      d2E = []
      for i in range( iterMaxDirs ):
        if not self.__dirExplorer.isActive():
          break
        d2E.append( self.__dirExplorer.getNextDir() )
      self.__exploreDirList( d2E )
      iterTime = time.time() - startT
      totalIterTime += iterTime
      numIterations += len( d2E )
      self.log.verbose( "Query took %.2f seconds for %s dirs" % ( iterTime, len( d2E ) ) )
    self.log.verbose( "Average query time: %2.f secs/dir" % ( totalIterTime / numIterations ) )

    # Publish remaining directories
    self.__publishData( background = False )

    # Move replica list files
    self.__replicaListFilesDone()

    # Clean records older than 1 day
    self.log.info( "Finished recursive directory search." )

    if self.am_getOption( "PurgeOutdatedRecords", True ):
      elapsedTime = time.time() - self.__startExecutionTime
      outdatedSeconds = max( max( self.am_getOption( "PollingTime" ), elapsedTime ) * 2, 86400 )
      result = self.storageUsage.purgeOutdatedEntries( self.__baseDir,
                                                         long( outdatedSeconds ),
                                                         self.__ignoreDirsList )
      if not result[ 'OK' ]:
        return result
      self.log.notice( "Purged %s outdated records" % result[ 'Value' ] )
    return S_OK()

  def __exploreDirList( self, dirList ):
    ''' collect directory size for directory in :dirList: '''
    self.log.notice( "Retrieving info for %s dirs" % len( dirList ) )
    res = self.catalog.getCatalogDirectorySize( dirList )
    if not res['OK']:
      self.log.error( "Completely failed to get usage.", "%s %s" % ( dirList, res['Message'] ) )
      return
    for dirPath in dirList:
      if dirPath in res['Value']['Failed']:
        self.log.error( "Failed to get usage.", "%s %s" % ( dirPath, res['Value']['Failed'][ dirPath ] ) )
        continue
      self.__processDir( dirPath, res['Value']['Successful'][dirPath] )

  def __processDir( self, dirPath, directoryMetadata ):
    ''' calculate nb of files and size of :dirPath:, remove it if it's empty '''
    self.log.notice( "Processing %s" % dirPath )
    subDirs = directoryMetadata['SubDirs']
    closedDirs = directoryMetadata['ClosedDirs']
    self.log.info( "Found %s sub-directories" % len( subDirs ) )
    if closedDirs:
      self.log.info( "%s sub-directories are closed (ignored)" % len( closedDirs ) )
      for directory in closedDirs:
        self.log.info( "Closed dir %s" % directory )
        subDirs.pop( directory )
    numberOfFiles = long( directoryMetadata['Files'] )
    totalSize = long( directoryMetadata['TotalSize'] )
    self.log.info( "Found %s files in the directory ( %s bytes )" % ( numberOfFiles, totalSize ) )
    siteUsage = directoryMetadata['SiteUsage']
    if numberOfFiles > 0:
      dirData = { 'Files' : numberOfFiles, 'TotalSize' : totalSize, 'SEUsage' : siteUsage }
      self.__addDirToPublishQueue( dirPath, dirData )
      # Print statistics
      self.log.verbose( "%-40s %20s %20s" % ( 'Storage Element', 'Number of files', 'Total size' ) )
      for storageElement in sorted( siteUsage ):
        usageDict = siteUsage[storageElement]
        self.log.verbose( "%-40s %20s %20s" % ( storageElement, str( usageDict['Files'] ), str( usageDict['Size'] ) ) )
    # If it's empty delete it
    elif len( subDirs ) == 0 and len( closedDirs ) == 0:
      if not dirPath == self.__baseDir:
        self.removeEmptyDir( dirPath )
        return
    chosenDirs = []
    rightNow = dateTime()
    for subDir in subDirs:
      if subDir in self.__ignoreDirsList:
        continue
      if self.activePeriod:
        timeDiff = timeInterval( subDirs[subDir], self.activePeriod * week )
        if timeDiff.includes( rightNow ):
          chosenDirs.append( subDir )
      else:
        chosenDirs.append( subDir )

    self.__dirExplorer.addDirList( chosenDirs )
    notCommited = len( self.__publishDirQueue ) + len( self.__dirsToPublish )
    self.log.notice( "%d dirs to be explored. %d not yet commited." % ( self.__dirExplorer.getNumRemainingDirs(),
                                                                        notCommited ) )

  def __getOwnerProxy( self, dirPath ):
    ''' get owner creds for :dirPath: '''
    self.log.verbose( "Retrieving dir metadata..." )
    result = self.catalog.getCatalogDirectoryMetadata( dirPath, singleFile = True )
    if not result[ 'OK' ]:
      self.log.error( "Could not get metadata info", result[ 'Message' ] )
      return result
    ownerRole = result[ 'Value' ][ 'OwnerRole' ]
    ownerDN = result[ 'Value' ][ 'OwnerDN' ]
    if ownerRole[0] != "/":
      ownerRole = "/%s" % ownerRole

    # Getting the proxy...
    cacheKey = ( ownerDN, ownerRole )
    if cacheKey in self.__noProxy:
      return S_ERROR( "Proxy not available" )
    userProxy = self.proxyCache.get( cacheKey, 3600 )
    if userProxy:
      return S_OK( userProxy )
    downErrors = []
    for ownerGroup in Registry.getGroupsWithVOMSAttribute( ownerRole ):
      result = gProxyManager.downloadVOMSProxy( ownerDN, ownerGroup, limited = True,
                                                requiredVOMSAttribute = ownerRole )
      if not result[ 'OK' ]:
        downErrors.append( "%s : %s" % ( cacheKey, result[ 'Message' ] ) )
        continue
      userProxy = result[ 'Value' ]
      secsLeft = max( 0, userProxy.getRemainingSecs()[ 'Value' ] )
      self.proxyCache.add( cacheKey, secsLeft, userProxy )
      self.log.verbose( "Got proxy for %s@%s [%s]" % ( ownerDN, ownerGroup, ownerRole ) )
      return S_OK( userProxy )
    self.__noProxy.add( cacheKey )
    return S_ERROR( "Could not download user proxy:\n%s " % "\n ".join( downErrors ) )

  def removeEmptyDir( self, dirPath ):
    ''' unlink empty folder :dirPath: '''
    if len( List.fromChar( dirPath, "/" ) ) <= self.__keepDirLevels:
      return S_OK()

    self.log.notice( "Deleting empty directory %s" % dirPath )
    res = self.storageUsage.removeDirectory( dirPath )
    if not res['OK']:
      self.log.error( "Failed to remove empty directory from Storage Usage database.", res[ 'Message' ] )
      return S_OK()

    result = self.__getOwnerProxy( dirPath )
    if not result[ 'OK' ]:
      self.log.error( result[ 'Message' ] )
      return result

    userProxy = result[ 'Value' ]
    result = userProxy.dumpAllToFile()
    if not result[ 'OK' ]:
      return result
    upFile = result[ 'Value' ]
    prevProxyEnv = os.environ[ 'X509_USER_PROXY' ]
    os.environ[ 'X509_USER_PROXY' ] = upFile
    try:
      res = self.catalog.removeCatalogDirectory( dirPath )
      if not res['OK']:
        self.log.error( "Failed to remove empty directory from File Catalog.", res[ 'Message' ] )
      elif dirPath in res['Value']['Failed']:
        self.log.error( "Failed to remove empty directory from File Catalog.", res[ 'Value' ][ 'Failed' ][ dirPath ] )
      else:
        self.log.info( "Successfully removed empty directory from File Catalog." )
      return S_OK()
    finally:
      os.environ[ 'X509_USER_PROXY' ] = prevProxyEnv
      os.unlink( upFile )

  def __addDirToPublishQueue( self, dirName, dirData ):
    ''' enqueue :dirName: and :dirData: for publishing '''
    self.__publishDirQueue[ dirName ] = dirData
    numDirsToPublish = len( self.__publishDirQueue )
    if numDirsToPublish and numDirsToPublish % self.am_getOption( "PublishClusterSize", 100 ) == 0:
      self.__publishData( background = True )

  def __publishData( self, background = True ):
    ''' publish data in a separate deamon thread '''
    self.dataLock.acquire()
    try:
      # Dump to file
      if self.am_getOption( "DumpReplicasToFile", False ):
        pass
        # repThread = threading.Thread( target = self.__writeReplicasListFiles,
        #                              args = ( list( self.__publishDirQueue ), ) )
      self.__dirsToPublish.update( self.__publishDirQueue )
      self.__publishDirQueue = {}
    finally:
      self.dataLock.release()
    if background:
      pubThread = threading.Thread( target = self.__executePublishData )
      pubThread.setDaemon( 1 )
      pubThread.start()
    else:
      self.__executePublishData()

  def __executePublishData( self ):
    ''' publication thread target '''
    self.dataLock.acquire()
    try:
      if not self.__dirsToPublish:
        self.log.info( "No data to be published" )
        return
      self.log.info( "Publishing usage for %d directories" % len( self.__dirsToPublish ) )
      res = self.storageUsage.publishDirectories( self.__dirsToPublish )
      if res['OK']:
        self.__dirsToPublish = {}
      else:
        self.log.error( "Failed to publish directories", res['Message'] )
      return res
    finally:
      self.dataLock.release()
