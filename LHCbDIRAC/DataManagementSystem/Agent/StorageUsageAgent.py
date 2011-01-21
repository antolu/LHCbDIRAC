"""  StorageUsageAgent takes the LFC as the primary source of information to determine storage usage.
"""
# $HeadURL: svn+ssh://svn.cern.ch/reps/dirac/DIRAC/trunk/DIRAC/DataManagementSystem/Agent/StorageUsageAgent.py $
__RCSID__ = "$Id: StorageUsageAgent.py 18161 2009-11-11 12:07:09Z acasajus $"

from DIRAC  import gLogger, gMonitor, S_OK, S_ERROR, rootPath, gConfig
from DIRAC.Core.Base.AgentModule import AgentModule

from DIRAC.ConfigurationSystem.Client.Helpers import Registry
from DIRAC.Core.Utilities.DirectoryExplorer import DirectoryExplorer
from DIRAC.FrameworkSystem.Client.ProxyManagerClient import gProxyManager
from DIRAC.DataManagementSystem.Client.ReplicaManager import CatalogDirectory
from DIRAC.Core.Utilities import List
from DIRAC.Core.Utilities.Time import timeInterval, dateTime, week
from DIRAC.Core.Utilities.DictCache import DictCache

import time, os, re, threading
import tempfile
from types import *

class StorageUsageAgent( AgentModule ):

  def initialize( self ):
    self.catalog = CatalogDirectory()
    if self.am_getOption( 'DirectDB', False ):
      from LHCbDIRAC.DataManagementSystem.DB.StorageUsageDB import StorageUsageDB
      self.__storageUsage = StorageUsageDB()
    else:
      from DIRAC.Core.DISET.RPCClient import RPCClient
      self.__storageUsage = RPCClient( 'DataManagement/StorageUsage' )

    self.am_setOption( "PollingTime", 43200 )

    # This sets the Default Proxy to used as that defined under 
    # /Operations/Shifter/DataManager
    # the shifterProxy option in the Configsorteduration can be used to change this default.
    self.am_setOption( 'shifterProxy', 'DataManager' )

    self.__activePeriod = self.am_getOption( 'ActivePeriod', 0 )
    self.__dataLock = threading.Lock()
    self.__replicaListLock = threading.Lock()
    self.__proxyCache = DictCache()
    return S_OK()

  def __writeReplicasListFiles( self, dirPathList ):
    self.__replicaListLock.acquire()
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
          for SEName in dirData[ lfn ]:
            if SEName not in filesOpened:
              filePath = os.path.join( self.__replicaListFilesDir, "replicas.%s.%s.filling" % ( SEName,
                                                                                                 self.__baseDirLabel ) )
              #Check if file is opened and if not open it
              if SEName not in filesOpened:
                if SEName not in self.__replicaFilesUsed:
                  self.__replicaFilesUsed.add( SEName )
                  filesOpened[ SEName ] = file( filePath, "w" )
                else:
                  filesOpened[ SEName ] = file( filePath, "a" )
            #SEName file is opened. Write
            filesOpened[ SEName ].write( "%s -> %s\n" % ( lfn, dirData[ lfn ][ SEName ] ) )
      #Close the files
      for SEName in filesOpened:
        filesOpened[ SEName ].close()
      return S_OK()
    finally:
      self.__replicaListLock.release()

  def __resetReplicaListFiles( self ):
    self.__replicaFilesUsed = set()
    self.__replicaListFilesDir = os.path.join( self.am_getOption( "WorkDirectory" ), "replicaLists" )
    if not os.path.isdir( self.__replicaListFilesDir ):
      os.makedirs( self.__replicaListFilesDir )
    self.log.info( "Replica Lists directory is %s" % self.__replicaListFilesDir )

  def __replicaListFilesDone( self ):
    self.__replicaListLock.acquire()
    try:
      old = re.compile( "^replicas\.([a-zA-Z0-9\-_]*)\.%s\.old$" % self.__baseDirLabel )
      current = re.compile( "^replicas\.([a-zA-Z0-9\-_]*)\.%s$" % self.__baseDirLabel )
      filling = re.compile( "^replicas\.([a-zA-Z0-9\-_]*)\.%s\.filling$" % self.__baseDirLabel )
      #Delete old
      for fileName in os.listdir( self.__replicaListFilesDir ):
        match = old.match( fileName )
        if match:
          os.unlink( os.path.join( self.__replicaListFilesDir, fileName ) )
      #Current -> old
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
      self.__replicaListLock.release()

  def __printSummary( self ):
    res = self.__storageUsage.getStorageSummary()
    if res['OK']:
      gLogger.notice( "Storage Usage Summary" )
      gLogger.notice( "============================================================" )
      gLogger.notice( "%s %s %s" % ( 'Storage Element'.ljust( 40 ), 'Number of files'.rjust( 20 ), 'Total size'.rjust( 20 ) ) )
      for se in sorted( res['Value'] ):
        usage = res['Value'][se]['Size']
        files = res['Value'][se]['Files']
        site = se.split( '_' )[0].split( '-' )[0]
        gLogger.notice( "%s %s %s" % ( se.ljust( 40 ), str( files ).rjust( 20 ), str( usage ).rjust( 20 ) ) )
        gMonitor.registerActivity( "%s-used" % se, "%s usage" % se, "StorageUsage/%s usage" % site, "", gMonitor.OP_MEAN, bucketLength = 600 )
        gMonitor.addMark( "%s-used" % se, usage )
        gMonitor.registerActivity( "%s-files" % se, "%s files" % se, "StorageUsage/%s files" % site, "Files", gMonitor.OP_MEAN, bucketLength = 600 )
        gMonitor.addMark( "%s-files" % se, files )

  def execute( self ):
    self.__publishDirQueue = {}
    self.__dirsToPublish = {}
    self.__baseDir = self.am_getOption( 'BaseDirectory', '/lhcb' )
    self.__baseDirLabel = "_".join( List.fromChar( self.__baseDir, "/" ) )
    self.__ignoreDirsList = self.am_getOption( 'Ignore', [] )

    self.__startExecutionTime = long( time.time() )
    self.__dirExplorer = DirectoryExplorer( reverse = True )
    self.__resetReplicaListFiles()

    self.__printSummary()

    self.__dirExplorer.addDir( self.__baseDir )
    gLogger.notice( "Initiating with %s as base directory." % self.__baseDir )
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
      gLogger.verbose( "Query took %.2f seconds for %s dirs" % ( iterTime, len( d2E ) ) )
    gLogger.verbose( "Average query time: %2.f secs/dir" % ( totalIterTime / numIterations ) )

    #Publish remaining directories
    self.__publishData( background = False )

    #Move replica list files
    self.__replicaListFilesDone()

    #Clean records older than 1 day
    gLogger.info( "Finished recursive directory search." )

    if self.am_getOption( "PurgeOutdatedRecords", True ):
      elapsedTime = time.time() - self.__startExecutionTime
      outdatedSeconds = max( max( self.am_getOption( "PollingTime" ), elapsedTime ) * 2, 86400 )
      result = self.__storageUsage.purgeOutdatedEntries( self.__baseDir,
                                                         long( outdatedSeconds ),
                                                         self.__ignoreDirsList )
      if not result[ 'OK' ]:
        return result
      self.log.notice( "Purged %s outdated records" % result[ 'Value' ] )

    return S_OK()

  def __exploreDirList( self, dirList ):
    gLogger.notice( "Retrieving info for %s dirs" % len( dirList ) )

    res = self.catalog.getCatalogDirectorySize( dirList )
    if not res['OK']:
      gLogger.error( "Completely failed to get usage.", "%s %s" % ( dirList, res['Message'] ) )
      return

    for dirPath in dirList:
      if dirPath in res['Value']['Failed']:
        gLogger.error( "Failed to get usage.", "%s %s" % ( dirPath, res['Value']['Failed'][ dirPath ] ) )
        continue
      self.__processDir( dirPath, res['Value']['Successful'][dirPath] )


  def __processDir( self, dirPath, directoryMetadata ):
    gLogger.notice( "Processing %s" % dirPath )

    subDirs = directoryMetadata['SubDirs']
    closedDirs = directoryMetadata['ClosedDirs']

    gLogger.info( "Found %s sub-directories" % len( subDirs ) )

    if closedDirs:
      gLogger.info( "%s sub-directories are closed (ignored)" % len( closedDirs ) )
      for dir in closedDirs:
        gLogger.info( "Closed dir %s" % dir )
        subDirs.pop( dir )

    numberOfFiles = long( directoryMetadata['Files'] )
    totalSize = long( directoryMetadata['TotalSize'] )
    gLogger.info( "Found %s files in the directory ( %s bytes )" % ( numberOfFiles, totalSize ) )

    siteUsage = directoryMetadata['SiteUsage']
    if numberOfFiles > 0:
      dirData = { 'Files' : numberOfFiles, 'TotalSize' : totalSize, 'SEUsage' : siteUsage }
      self.__addDirToPublishQueue( dirPath, dirData )
      #Print statistics
      gLogger.verbose( "%s %s %s" % ( 'Storage Element'.ljust( 40 ),
                                      'Number of files'.rjust( 20 ),
                                      'Total size'.rjust( 20 ) ) )
      for storageElement in sorted( siteUsage ):
        usageDict = siteUsage[storageElement]
        gLogger.verbose( "%s %s %s" % ( storageElement.ljust( 40 ),
                                        str( usageDict['Files'] ).rjust( 20 ),
                                        str( usageDict['Size'] ).rjust( 20 ) ) )
    #If it's empty delete it
    elif len( subDirs ) == 0 and len( closedDirs ) == 0:
      if not dirPath == self.__baseDir:
        self.removeEmptyDir( dirPath )
        return

    chosenDirs = []
    rightNow = dateTime()
    for subDir in subDirs:
      if subDir in self.__ignoreDirsList:
        continue
      if self.__activePeriod:
        timeDiff = timeInterval( subDirs[subDir], self.__activePeriod * week )
        if timeDiff.includes( rightNow ):
          chosenDirs.append( subDir )
      else:
        chosenDirs.append( subDir )

    self.__dirExplorer.addDirList( chosenDirs )
    self.log.notice( "%d dirs to be explored. %d not yet commited." % ( self.__dirExplorer.getNumRemainingDirs(),
                                                                        len( self.__publishDirQueue ) + len ( self.__dirsToPublish ) ) )

  def __getOwnerProxy( self, dirPath ):
    gLogger.verbose( "Retrieving dir metadata..." )

    result = self.catalog.getCatalogDirectoryMetadata( dirPath, singleFile = True )
    if not result[ 'OK' ]:
      gLogger.error( "Could not get metadata info", res[ 'Message' ] )
    ownerRole = result[ 'Value' ][ 'OwnerRole' ]
    ownerDN = result[ 'Value' ][ 'OwnerDN' ]
    if ownerRole[0] != "/":
      ownerRole = "/%s" % ownerRole

    #Getting the proxy...
    cacheKey = ( ownerDN, ownerRole )
    userProxy = self.__proxyCache.get( cacheKey, 3600 )
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
      self.__proxyCache.add( cacheKey, secsLeft, userProxy )
      gLogger.verbose( "Got proxy for %s@%s [%s]" % ( ownerDN, ownerGroup, ownerRole ) )
      return S_OK( userProxy )
    return S_ERROR( "Could not download user proxy:\n%s " % "\n ".join( downErrors ) )



  def removeEmptyDir( self, dirPath ):
    self.log.notice( "Deleting empty directory %s" % dirPath )
    res = self.__storageUsage.removeDirectory( dirPath )
    if not res['OK']:
      gLogger.error( "Failed to remove empty directory from Storage Usage database.", res[ 'Message' ] )
      return S_OK()

    result = self.__getOwnerProxy( dirPath )
    if not result[ 'OK' ]:
      gLogger.error( result[ 'Message' ] )
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
        gLogger.error( "Failed to remove empty directory from File Catalog.", res[ 'Message' ] )
      elif dirPath in res['Value']['Failed']:
        gLogger.error( "Failed to remove empty directory from File Catalog.", res[ 'Value' ][ 'Failed' ][ dirPath ] )
      else:
        gLogger.info( "Successfully removed empty directory from File Catalog." )
      return S_OK()
    finally:
      os.environ[ 'X509_USER_PROXY' ] = prevProxyEnv
      os.unlink( upFile )

  def __addDirToPublishQueue( self, dirName, dirData ):
    self.__publishDirQueue[ dirName ] = dirData
    numDirsToPublish = len( self.__publishDirQueue )
    if numDirsToPublish and numDirsToPublish % self.am_getOption( "PublishClusterSize", 100 ) == 0:
      self.__publishData( background = True )

  def __publishData( self, background = True ):
    self.__dataLock.acquire()
    try:
      #Dump to file
      if self.am_getOption( "DumpReplicasToFile", False ):
        repThread = threading.Thread( target = self.__writeReplicasListFiles,
                                      args = ( list( self.__publishDirQueue ), ) )

      self.__dirsToPublish.update( self.__publishDirQueue )
      self.__publishDirQueue = {}
    finally:
      self.__dataLock.release()
    if background:
      pubThread = threading.Thread( target = self.__executePublishData )
      pubThread.setDaemon( 1 )
      pubThread.start()
    else:
      self.__executePublishData()

  def __executePublishData( self ):
    self.__dataLock.acquire()
    try:
      if not self.__dirsToPublish:
        gLogger.info( "No data to be published" )
        return
      gLogger.info( "Publishing usage for %d directories" % len( self.__dirsToPublish ) )

      res = self.__storageUsage.publishDirectories( self.__dirsToPublish )
      if res['OK']:
        self.__dirsToPublish = {}
      else:
        gLogger.error( "Failed to publish directories", res['Message'] )
      return res

    finally:
      self.__dataLock.release()



