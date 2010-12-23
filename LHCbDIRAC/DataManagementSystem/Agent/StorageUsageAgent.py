"""  StorageUsageAgent takes the LFC as the primary source of information to determine storage usage.
"""
# $HeadURL: svn+ssh://svn.cern.ch/reps/dirac/DIRAC/trunk/DIRAC/DataManagementSystem/Agent/StorageUsageAgent.py $
__RCSID__ = "$Id: StorageUsageAgent.py 18161 2009-11-11 12:07:09Z acasajus $"

from DIRAC  import gLogger, gMonitor, S_OK, S_ERROR, rootPath, gConfig
from DIRAC.Core.Base.AgentModule import AgentModule

from DIRAC.Core.Utilities.DirectoryExplorer import DirectoryExplorer
from DIRAC.DataManagementSystem.Client.ReplicaManager import CatalogDirectory
from DIRAC.Core.Utilities.List import sortList
from DIRAC.Core.Utilities.Time import timeInterval, dateTime, week

import time, os
from types import *

AGENT_NAME = 'DataManagement/StorageUsageAgent'

class StorageUsageAgent( AgentModule ):

  def initialize( self ):
    self.catalog = CatalogDirectory()
    if self.am_getOption( 'DirectDB', False ):
      from LHCbDIRAC.DataManagementSystem.DB.StorageUsageDB import StorageUsageDB
      self.StorageUsageDB = StorageUsageDB()
    else:
      from DIRAC.Core.DISET.RPCClient import RPCClient
      self.StorageUsageDB = RPCClient( 'DataManagement/StorageUsage' )

    # This sets the Default Proxy to used as that defined under 
    # /Operations/Shifter/DataManager
    # the shifterProxy option in the Configuration can be used to change this default.
    self.am_setOption( 'shifterProxy', 'DataManager' )

    self.activePeriod = self.am_getOption( 'ActivePeriod', 0 )
    return S_OK()

  def execute( self ):
    startExecuteTime = time.time()
    res = self.StorageUsageDB.getStorageSummary()
    if res['OK']:
      gLogger.notice( "Storage Usage Summary" )
      gLogger.notice( "============================================================" )
      gLogger.notice( "%s %s %s" % ( 'Storage Element'.ljust( 40 ), 'Number of files'.rjust( 20 ), 'Total size'.rjust( 20 ) ) )
      for se in sortList( res['Value'].keys() ):
        usage = res['Value'][se]['Size']
        files = res['Value'][se]['Files']
        site = se.split( '_' )[0].split( '-' )[0]
        gLogger.notice( "%s %s %s" % ( se.ljust( 40 ), str( files ).rjust( 20 ), str( usage ).rjust( 20 ) ) )
        gMonitor.registerActivity( "%s-used" % se, "%s usage" % se, "StorageUsage/%s usage" % site, "", gMonitor.OP_MEAN, bucketLength = 600 )
        gMonitor.addMark( "%s-used" % se, usage )
        gMonitor.registerActivity( "%s-files" % se, "%s files" % se, "StorageUsage/%s files" % site, "Files", gMonitor.OP_MEAN, bucketLength = 600 )
        gMonitor.addMark( "%s-files" % se, files )


    baseDir = self.am_getOption( 'BaseDirectory', '/lhcb' )
    ignoreDirectories = self.am_getOption( 'Ignore', [] )
    dirExplorer = DirectoryExplorer( reverse = True )
    dirExplorer.addDir( baseDir )
    gLogger.notice( "Initiating with %s as base directory." % baseDir )

    # Loop over all the directories and sub-directories
    directoriesToPublish = {}
    while dirExplorer.isActive():
      currentDir = dirExplorer.getNextDir()
      gLogger.notice( "Getting usage for %s." % currentDir )
      numberOfFiles = 0
      subDirs = []

      res = self.catalog.getCatalogDirectorySize( currentDir )
      if not res['OK']:
        gLogger.error( "Completely failed to get usage.", "%s %s" % ( currentDir, res['Message'] ) )
        continue

      if currentDir in res['Value']['Failed']:
        gLogger.error( "Failed to get usage.", "%s %s" % ( currentDir, res['Value']['Failed'][currentDir] ) )
        continue

      directoryMetadata = res['Value']['Successful'][currentDir]
      subDirs = directoryMetadata['SubDirs']
      closedDirs = directoryMetadata['ClosedDirs']
      gLogger.info( "Found %s sub-directories." % len( subDirs ) )
      if closedDirs:
        gLogger.info( "%s sub-directories are closed (ignored)." % len( closedDirs ) )
        for dir in closedDirs:
          gLogger.info( "closed dir %s" % dir )
          subDirs.pop( dir )
      numberOfFiles = long( directoryMetadata['Files'] )
      gLogger.info( "Found %s files in the directory." % numberOfFiles )
      totalSize = long( directoryMetadata['TotalSize'] )

      siteUsage = directoryMetadata['SiteUsage']
      if numberOfFiles > 0:
        directoriesToPublish[currentDir] = {'Files':numberOfFiles, 'TotalSize':totalSize, 'SEUsage':siteUsage}
        gLogger.verbose( "%s %s %s" % ( 'Storage Element'.ljust( 40 ), 'Number of files'.rjust( 20 ), 'Total size'.rjust( 20 ) ) )
        for storageElement in sortList( siteUsage.keys() ):
          usageDict = siteUsage[storageElement]
          gLogger.verbose( "%s %s %s" % ( storageElement.ljust( 40 ), str( usageDict['Files'] ).rjust( 20 ), str( usageDict['Size'] ).rjust( 20 ) ) )
      elif len( subDirs ) == 0 and len( closedDirs ) == 0:
        if not currentDir == baseDir:
          self.removeEmptyDir( currentDir )
          continue

      #Try to publish every 100 dirs
      if len( directoriesToPublish ) % 100 == 0 and directoriesToPublish:
        if self.publishDirectories( directoriesToPublish )[ 'OK' ]:
          directoriesToPublish = {}

      chosenDirs = []
      rightNow = dateTime()
      for subDir in subDirs:
        if subDir in ignoreDirectories:
          continue
        if self.activePeriod:
          timeDiff = timeInterval( subDirs[subDir], self.activePeriod * week )
          if timeDiff.includes( rightNow ):
            chosenDirs.append( subDir )
        else:
          chosenDirs.append( subDir )

      dirExplorer.addDirList( chosenDirs )
      self.log.notice( "%d dirs to be explored. %d not yet commited." % ( dirExplorer.getNumRemainingDirs(), len( directoriesToPublish ) ) )

    #Publish remaining directories
    self.publishDirectories( directoriesToPublish )

    #Clean records older than 1 day
    gLogger.info( "Finished recursive directory search." )

    elapsedTime = time.time() - startExecuteTime
    result = self.StorageUsageDB.purgeOutdatedEntries( baseDir, self.am_getOption( "OutdatedSeconds", elapsedTime * 2 ) )
    if not result[ 'OK' ]:
      return result
    self.log.notice( "Purged %s outdated records" % result[ 'Value' ] )
    return S_OK()

  def removeEmptyDir( self, dirPath ):
    self.log.notice( "Deleting empty directory %s" % dirPath )
    res = self.StorageUsageDB.removeDirectory( dirPath )
    if not res['OK']:
      gLogger.error( "Failed to remove empty directory from Storage Usage database.", res[ 'Message' ] )
      return S_OK()
    res = self.catalog.removeCatalogDirectory( dirPath )
    if not res['OK']:
      gLogger.error( "Failed to remove empty directory from File Catalog.", res[ 'Message' ] )
    elif dirPath in res['Value']['Failed']:
      gLogger.error( "Failed to remove empty directory from File Catalog.", res[ 'Value' ][ 'Failed' ][ dirPath ] )
    else:
      gLogger.info( "Successfully removed empty directory from File Catalog." )
    return S_OK()

  def publishDirectories( self, directoriesToPublish ):
    gLogger.info( "Publishing usage for %d directories" % len( directoriesToPublish ) )
    res = self.StorageUsageDB.publishDirectories( directoriesToPublish )
    if not res['OK']:
      gLogger.error( "Failed to publish directories", res['Message'] )
    return res
