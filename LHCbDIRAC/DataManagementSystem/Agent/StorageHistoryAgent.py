'''   The Storage History Agent will create a summary of the
      storage usage DB grouped by processing pass or other
      interesting parameters.

      Initially this will dump the information to a file but eventually
      can be inserted in a new DB table and made visible via the web portal.
'''

__RCSID__ = "$Id$"

import os, time, copy

from DIRAC                                                  import S_OK, S_ERROR, gLogger
from DIRAC.Core.Utilities                                   import Time

from DIRAC.Core.Base.AgentModule                            import AgentModule
from DIRAC.AccountingSystem.Client.DataStoreClient          import gDataStoreClient

from LHCbDIRAC.AccountingSystem.Client.Types.UserStorage    import UserStorage
from LHCbDIRAC.AccountingSystem.Client.Types.Storage        import Storage
from LHCbDIRAC.AccountingSystem.Client.Types.DataStorage    import DataStorage
from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient   import BookkeepingClient
from LHCbDIRAC.DataManagementSystem.Client.DataUsageClient  import DataUsageClient
from DIRAC.Core.Utilities.List                              import breakListIntoChunks

byteToGB = 1.0e9

def _standardDirectory( dirPath ):
  return dirPath if dirPath[-1] == '/' else dirPath + '/'

def _standardDirList( dirList ):
  return [_standardDirectory( dirPath ) for dirPath in dirList]

def _fillMetadata( dictToFill, metadataValue ):
  ''' Fill the dictionary to send to the accounting.
      If metadataValue is a string then set all the values of dictToFill, to this value
      if metadataValue is a dictionary then set each value of dictToFill to the corresponding value of metadataValue
  '''
  # ds = DataStorage()
  # keyList = ds.keyFieldsList
  # this is the list of attributes returned by the Bookkeeping for a given directory
  keyList = ( 'ConfigName', 'ConfigVersion', 'FileType', 'Production',
             'ProcessingPass', 'ConditionDescription', 'EventType', 'Visibility' )
  if isinstance( metadataValue, basestring ):
    for k in keyList:
      dictToFill[ k ] = metadataValue
  elif isinstance( metadataValue, dict ):
    for k in keyList:
      dictToFill[k] = metadataValue.get( k, 'na' )

class StorageHistoryAgent( AgentModule ):
  def initialize( self ):
    '''Sets defaults
    '''
    self.am_setOption( 'PollingTime', 43200 )
    if self.am_getOption( 'DirectDB', False ):
      from LHCbDIRAC.DataManagementSystem.DB.StorageUsageDB import StorageUsageDB
      self.__stDB = StorageUsageDB()
    else:
      from DIRAC.Core.DISET.RPCClient import RPCClient
      self.__stDB = RPCClient( 'DataManagement/StorageUsage' )
    self.__workDirectory = self.am_getOption( "WorkDirectory" )
    if not os.path.isdir( self.__workDirectory ):
      os.makedirs( self.__workDirectory )
    self.log.info( "Working directory is %s" % self.__workDirectory )

    self.__ignoreDirsList = self.am_getOption( 'Ignore', [] )
    self.log.info( "List of directories to ignore for the DataStorage accounting: %s " % self.__ignoreDirsList )

    self.__bkClient = BookkeepingClient()
    self.__dataUsageClient = DataUsageClient()
    self.cachedMetadata = {}
    # build a dictionary with Event Type descriptions (to be send to accounting, instead of number Event Type ID)
    self.eventTypeDescription = { 'na':'na', 'notInBkk':'notInBkk', 'FailedBkkQuery':'FailedBkkQuery', 'None':'None'}
    self.limitForCommit = self.am_getOption( "LimitForCommit", 1000 )
    self.callsToGetSummary = 0
    self.callsToDirectorySummary = 0
    self.callsToDudbForMetadata = 0
    # keep count of calls to Bkk
    self.callsToBkkgetDirectoryMetadata = 0
    self.callsToBkkGetEvtType = 0

    return S_OK()

  def userStorageAccounting( self ):
    self.log.notice( "-------------------------------------------------------------------------------------\n" )
    self.log.notice( "Generate accounting records for user directories " )
    self.log.notice( "-------------------------------------------------------------------------------------\n" )

    result = self.__stDB.getUserSummary()
    if not result[ 'OK' ]:
      return result
    userCatalogData = result[ 'Value' ]
    print userCatalogData
    self.log.notice( "Got summary for %s users" % ( len( userCatalogData ) ) )
    result = self.__stDB.getUserSummaryPerSE()
    if not result[ 'OK' ]:
      return result
    userSEData = result[ 'Value' ]
    self.log.notice( "Got SE summary for %s users" % ( len( userSEData ) ) )

    now = Time.dateTime()
    numRows = 0
    for user in sorted( userSEData ):
      if user not in userCatalogData:
        self.log.error( "User has SE data but not Catalog data!", user )
        continue
      for se in sorted( userSEData[ user ] ):
        seData = userSEData[ user ][ se ]
        usRecord = UserStorage()
        usRecord.setStartTime( now )
        usRecord.setEndTime( now )
        usRecord.setValueByKey( "User", user )
        usRecord.setValueByKey( "StorageElement", se )
        usRecord.setValueByKey( "LogicalSize", userCatalogData[ user ][ 'Size' ] )
        usRecord.setValueByKey( "LogicalFiles", userCatalogData[ user ][ 'Files' ] )
        usRecord.setValueByKey( "PhysicalSize", seData[ 'Size' ] )
        usRecord.setValueByKey( "PhysicalFiles", seData[ 'Files' ] )
        usRecord.setValueByKey( "StorageSize", 0 )
        usRecord.setValueByKey( "StorageFiles", 0 )
        gDataStoreClient.addRegister( usRecord )
        numRows += 1

      self.log.notice( " User %s is using %.2f GiB (%s files)" % ( user,
                                                                   userCatalogData[ user ][ 'Size' ] / ( 1024.0 ** 3 ),
                                                                   userCatalogData[ user ][ 'Files' ] ) )
    self.log.notice( "Sending %s records to accounting for user storage" % numRows )
    res = gDataStoreClient.commit()
    if not res[ 'OK' ]:
      self.log.notice( "ERROR: committing UserStorage records: %s " % res )
      return S_ERROR( res )
    else:
      self.log.notice( "%s records for UserStorage type successfully committed" % numRows )

  def topDirectoryAccounting( self ):
    self.log.notice( "-------------------------------------------------------------------------------------\n" )
    self.log.notice( "Generate accounting records for top directories " )
    self.log.notice( "-------------------------------------------------------------------------------------\n" )

    ftb = 1.0e12

    # get info from the DB about the LOGICAL STORAGE USAGE (from the su_Directory table):
    result = self.__stDB.getSummary( '/lhcb/' )
    if not result[ 'OK' ]:
      return result
    logicalUsage = result['Value']
    topDirLogicalUsage = {}  # build the list of first level directories
    for row in logicalUsage:
      # d, size, files = row
      splitDir = row.split( "/" )
      if len( splitDir ) > 3:  # skip the root directory "/lhcb/"
        firstLevelDir = '/' + splitDir[1] + '/' + splitDir[2] + '/'
        topDirLogicalUsage.setdefault( firstLevelDir, {'Files':0, 'Size':0} )
        topDirLogicalUsage[ firstLevelDir ][ 'Files' ] += logicalUsage[ row ][ 'Files' ]
        topDirLogicalUsage[ firstLevelDir ][ 'Size' ] += logicalUsage[ row ][ 'Size' ]
    self.log.notice( "Summary on logical usage of top directories: " )
    for row in topDirLogicalUsage:
      self.log.notice( "dir: %s size: %.4f TB  files: %d" % ( row, topDirLogicalUsage[row]['Size'] / ftb,
                                                              topDirLogicalUsage[row]['Files'] ) )

    # loop on top level directories (/lhcb/data, /lhcb/user/, /lhcb/MC/, etc..)
    # to get the summary in terms of PHYSICAL usage grouped by SE:
    seData = {}
    for directory in topDirLogicalUsage:
      result = self.__stDB.getDirectorySummaryPerSE( directory )  # retrieve the PHYSICAL usage
      if not result[ 'OK' ]:
        return result
      seData[ directory ] = result[ 'Value' ]
      self.log.notice( "Got SE summary for %s directories " % ( len( seData ) ) )
      self.log.debug( "SEData: %s" % seData )
    # loop on top level directories to send the accounting records
    numRows = 0
    now = Time.dateTime()
    for directory in seData:
      self.log.debug( "dir: %s SEData: %s " % ( directory, seData[ directory ] ) )
      if directory not in topDirLogicalUsage:
        self.log.error( "Dir %s is in the summary per SE, but it is not in the logical files summary!" % directory )
        continue
      for se in sorted( seData[ directory ] ):
        storageRecord = Storage()
        storageRecord.setStartTime( now )
        storageRecord.setEndTime( now )
        storageRecord.setValueByKey( "Directory", directory )
        storageRecord.setValueByKey( "StorageElement", se )
        storageRecord.setValueByKey( "LogicalFiles", topDirLogicalUsage[ directory ][ 'Files' ] )
        storageRecord.setValueByKey( "LogicalSize", topDirLogicalUsage[ directory ][ 'Size' ] )
        try:
          physicalFiles = seData[ directory ][ se ][ 'Files' ]
        except:
          self.log.error( "WARNING! no files replicas for directory %s on SE %s" % ( directory, se ) )
          physicalFiles = 0
        try:
          physicalSize = seData[ directory ][ se ][ 'Size' ]
        except:
          self.log.error( "WARNING! no size for replicas for directory %s on SE %s" % ( directory, se ) )
          physicalSize = 0
        storageRecord.setValueByKey( "PhysicalFiles", physicalFiles )
        storageRecord.setValueByKey( "PhysicalSize", physicalSize )
        gDataStoreClient.addRegister( storageRecord )
        numRows += 1
        self.log.debug( "Directory: %s SE: %s  physical size: %.4f TB (%d files)" % ( directory,
                                                                                      se,
                                                                                      physicalSize / ftb,
                                                                                      physicalFiles ) )

    self.log.notice( "Sending %s records to accounting for top level directories storage" % numRows )
    res = gDataStoreClient.commit()
    if not res[ 'OK' ]:
      self.log.notice( "ERROR: committing Storage records: %s " % res )
      return S_ERROR( res )
    else:
      self.log.notice( "%s records for Storage type successfully committed" % numRows )

  def bkPathAccounting( self ):
    self.log.notice( "-------------------------------------------------------------------------------------\n" )
    self.log.notice( "Generate accounting records for DataStorage type " )
    self.log.notice( "-------------------------------------------------------------------------------------\n" )


    # counter for DataStorage records, commit to the accounting in bunches of self.limitForCommit records
    self.totalRecords = 0
    self.recordsToCommit = 0
    self.log.notice( " Call the function to create the StorageUsageDB dump.." )
    res = self.generateStorageUsagePerDir()
    if not res[ 'OK' ]:
      self.log.error( "ERROR generating the StorageUsageDB dump per directory" )
      return S_ERROR()

    # Keep a list of all directories in LFC that are not found in the Bkk
    self.directoriesNotInBkk = []
    # for debugging purposes build dictionaries with storage usage to compare with the accounting plots
    self.debug_seUsage = {}
    self.debug_seUsage_acc = {}

    # set the time for the accounting records (same time for all records)
    now = Time.dateTime()
    # Get the directory metadata in a bulk query
    metaForList = self.__getMetadataForAcc( self.dirDict.values() )

    # loop on all directories  to get the bkk metadata
    for dirLfn, fullDirectory in self.dirDict.items():
      if dirLfn not in fullDirectory:
        self.log.error( "ERROR: fullDirectory should include the dirname: %s %s " % ( fullDirectory, dirLfn ) )
        continue
      self.log.info( "Processing directory %s " % dirLfn )
      if dirLfn not in self.pfnUsage:
        self.log.error( "ERROR: directory does not have PFN usage %s " % dirLfn )
        continue
      self.log.verbose( "PFN usage: %s " % self.pfnUsage[ dirLfn ] )
      if dirLfn not in self.lfnUsage:
        self.log.error( "ERROR: directory does not have LFN usage %s " % dirLfn )
        continue
      self.log.verbose( "LFN usage: %s " % self.lfnUsage[ dirLfn ] )

      # for DEBUGGING:
      for se in self.pfnUsage[ dirLfn ]:
        self.debug_seUsage.setdefault( se, {'Files': 0 , 'Size' : 0 } )
        self.debug_seUsage[ se ][ 'Files' ] += self.pfnUsage[ dirLfn ][ se ][ 'Files' ]
        self.debug_seUsage[ se ][ 'Size' ] += self.pfnUsage[ dirLfn ][ se ][ 'Size' ]
      # end of DEBUGGING

      # get metadata for this directory
      metaForDir = metaForList.get( fullDirectory, {} )
      if not metaForDir:
        self.log.warn( "Metadata not available for directory %s" % fullDirectory )
        continue

      # Fill in the accounting record
      self.log.info( "Fill the record for %s and metadata: %s " % ( dirLfn, metaForDir ) )
      res = self.fillAndSendAccountingRecord( dirLfn, metaForDir, now )
      if not res['OK']:
        return res
      for se in self.pfnUsage[ dirLfn ]:
        self.debug_seUsage_acc.setdefault( se, { 'Files': 0 , 'Size': 0 } )
        self.debug_seUsage_acc[ se ][ 'Files' ] += self.pfnUsage[ dirLfn ][ se ][ 'Files' ]
        self.debug_seUsage_acc[ se ][ 'Size' ] += self.pfnUsage[ dirLfn ][ se ][ 'Size' ]

    # Don't forget to commit the remaining records!
    self.__commitRecords()


  def execute( self ):
    if self.am_getOption( "CleanBefore", False ):
      self.log.notice( "Cleaning the DB" )
      result = self.__stDB.purgeOutdatedEntries( "/lhcb/user", self.am_getOption( "OutdatedSeconds", 86400 * 10 ) )
      if not result[ 'OK' ]:
        return result
      self.log.notice( "Purged %s outdated records" % result[ 'Value' ] )

    # User accounting (per user and SE)
    self.userStorageAccounting()
    # Accounting per top directory
    self.topDirectoryAccounting()
    # full production data accounting
    self.bkPathAccounting()


    self.log.notice( "-------------------------------------------------------------------------------------\n" )
    self.log.notice( "------ End of cycle report for DataStorage accounting--------------------------------\n" )
    self.log.notice( "Total directories found in LFC:  %d " % len( self.dirDict ) )
    totalCallsToStorageUsage = self.callsToGetSummary + self.callsToDirectorySummary
    self.log.notice( "Total calls to StorageUsage: %d , took: %d s " % ( totalCallsToStorageUsage, self.genTotalTime ) )
    totalCallsToBkk = self.callsToBkkgetDirectoryMetadata + self.callsToBkkGetEvtType
    self.log.notice( "Total calls to DataUsage for cache: %d" % self.callsToDudbForMetadata )
    self.log.notice( "Total calls to Bookkeeping: %d (getDirectoryMetadata: %d, getEventType: %d)" % ( totalCallsToBkk,
                                                                                         self.callsToBkkgetDirectoryMetadata,
                                                                                         self.callsToBkkGetEvtType ) )
    self.log.notice( "Total records sent to accounting for DataStorage:  %d " % self.totalRecords )
    self.log.notice( "Directories not found in Bookkeeping: %d " % ( len( self.directoriesNotInBkk ) ) )
    fileName = os.path.join( self.__workDirectory, "directoriesNotInBkk.txt" )
    self.log.notice( "written to file: %s " % fileName )
    f = open( fileName, "w" )
    for d in self.directoriesNotInBkk:
      f.write( "%s\n" % d )
    f.close()
    # for DEBUG only
    self.log.info( "Summary of StorageUsage: files size " )
    for se in sorted( self.debug_seUsage ):
      self.log.info( "all: %s  %d %d Bytes ( %.2f TB ) " % ( se, self.debug_seUsage[ se ]['Files'],
                                                            self.debug_seUsage[ se ]['Size'],
                                                            self.debug_seUsage[ se ]['Size'] / 1.0e12 ) )
      if se in self.debug_seUsage_acc:
        self.log.info( "acc: %s  %d %d Bytes ( %.2f TB ) " % ( se, self.debug_seUsage_acc[ se ]['Files'],
                                                              self.debug_seUsage_acc[ se ]['Size'],
                                                              self.debug_seUsage_acc[ se ]['Size'] / 1.0e12 ) )
      else:
        self.log.info( "SE not in self.debug_seUsage_acc keys" )
    return S_OK()

  def __getMetadataForAcc( self, dirList ):
    """ Get metadata for a directory either from memory, from the storageDB or from BK """
    # Try and get the metadata from memory cache
    notFound = []
    metaForList = {}
    for dirName in dirList:
      metaForList[dirName] = self.cachedMetadata.get( dirName, {} )
      if not metaForList[dirName]:
        notFound.append( dirName )
    notInCache = []
    if notFound:
      self.log.info( "Memory metadata cache missed for %d directories" % len( notFound ) )
      self.log.verbose( "call getDirMetadata for (first 10): %s " % str( notFound[0:10] ) )
      for dirChunk in breakListIntoChunks( notFound, 10000 ):
        self.callsToDudbForMetadata += 1
        res = self.__dataUsageClient.getDirMetadata( dirChunk )  # this could be a bulk query for a list of directories
        if not res[ 'OK' ]:
          self.log.error( "Error retrieving %d directories meta-data %s " % ( len( dirChunk ), res['Message'] ) )
          # this usually happens when directories are removed from LFC between the StorageUsageDB dump and this call,
          # if the Db is refreshed exactly in this time interval. Not really a problem.
          #######################3 just a try ##############################################3
          notInCache += dirChunk
          continue
        self.log.verbose( "getDirMetadata returned: %s " % str( res['Value'] ) )
        for dirName in dirChunk:
          # Compatibility with old (list for single file) and new (dictionary) service
          if type( res['Value'] ) == type( {} ):
            metaTuple = res['Value'].get( dirName, () )
          elif len( dirList ) == 1 and res['Value']:
            metaTuple = res['Value'][0]
          else:
            metaTuple = ()
          if metaTuple:
            metaForDir = metaForList[dirName]
            _dirID, metaForDir[ 'DataType' ], metaForDir[ 'Activity' ], metaForDir[ 'Conditions' ], metaForDir[ 'ProcessingPass' ], \
              metaForDir[ 'EventType' ], metaForDir[ 'FileType' ], metaForDir[ 'Production' ], metaForDir['Visibility'] = metaTuple
          else:
            notInCache.append( dirName )

      failedBK = []
      if notInCache:
        cachedFromBK = []
        self.log.info( "Directory metadata cache missed for %d directories => query BK and cache" % len( notInCache ) )
        for dirChunk in breakListIntoChunks( notInCache, 200 ):
          self.callsToBkkgetDirectoryMetadata += 1
          res = self.__bkClient.getDirectoryMetadata( dirChunk )
          if not res[ 'OK' ]:
            self.log.error( "Totally failed to query Bookkeeping", res[ 'Message' ] )
            failedBK += dirChunk
            for dirName in dirChunk:
              metaForDir = metaForList[dirName]
              _fillMetadata( metaForDir, 'FailedBkkQuery' )
          else:
            bkMetadata = res['Value']
            self.log.debug( "Successfully queried Bookkeeping, result: %s " % bkMetadata )
            for dirName in dirChunk:
              metaForDir = metaForList[dirName]
              # BK returns a list of metadata, chose the first one...
              metadata = bkMetadata['Successful'].get( dirName, [{}] )[0]
              if metadata:
                metadata['Visibility'] = metadata.pop( 'VisibilityFlag', metadata.get( 'Visibility', 'na' ) )
                # All is OK, directory found
                _fillMetadata( metaForDir, metadata )
                self.log.verbose( "Cache entry %s in DirMetadata table.." % dirName )
                resInsert = self.__dataUsageClient.insertToDirMetadata( { dirName: metadata} )
                if not resInsert[ 'OK' ]:
                  self.log.error( "Failed to cache metadata in DirMetadata table! %s " % resInsert[ 'Message' ] )
                else:
                  cachedFromBK.append( dirName )
                  self.log.verbose( "Successfully cached metadata for %s : %s" % ( dirName, str( metadata ) ) )
                  self.log.debug( "result: %s " % str( resInsert ) )
              else:
                # Directory not found
                self.log.verbose( "Directory %s not registered in Bookkeeping!" % dirName )
                _fillMetadata( metaForDir, 'notInBkk' )
                failedBK.append( dirName )
                self.directoriesNotInBkk.append( dirName )
              # Translate a few keys for accounting
              for bkName, accName in ( ( 'ConfigName', 'DataType' ), ( 'ConfigVersion', 'Activity' ), ( 'ConditionDescription', 'Conditions' ) ):
                metaForDir[accName] = metaForDir.pop( bkName, 'na' )
        self.log.info( 'Successfully cached %d directories from BK' % len( cachedFromBK ) )
        if self.directoriesNotInBkk:
          self.log.warn( '%d directories not found in BK' % len( self.directoriesNotInBkk ) )

      # cache locally the metadata
      for dirName in [dn for dn in notFound if dn not in failedBK]:
        metaForDir = metaForList[dirName]
        # Translate the numerical event type to a description string
        metaForDir['EventType'] = self.__getEventTypeDescription( metaForDir.pop( 'EventType', 'na' ) )
        self.cachedMetadata[ dirName] = metaForDir.copy()
    else:
      self.log.info( "Memory metadata cache hit for %d directories" % len( dirList ) )
    return metaForList

  def __commitRecords( self ):
    if self.recordsToCommit:
      res = gDataStoreClient.commit()
      if not res[ 'OK' ]:
        self.log.error( "Accounting ERROR: commit returned %s" % res )
      else:
        self.log.notice( "%d records committed " % self.recordsToCommit )
        self.recordsToCommit = 0
        self.log.notice( "commit for DataStorage returned: %s" % res )

  def generateStorageUsagePerDir( self ):
    '''Generate a dump of the StorageUsageDB and keep it in memory in a dictionary
       (new version of Apr 2012)
    '''

    start = time.time()
    self.log.notice( 'Starting from path: /lhcb/' )
    res = self.__stDB.getStorageDirectories( '/lhcb/' )
    if not res['OK']:
      return S_ERROR()
    totalDirList = res['Value']
    self.log.info( "Total directories retrieved from StorageUsageDB: %d " % len( totalDirList ) )
    # select only terminal directories (directories without sub-directories)
    # mc directory structure: /lhcb/MC/[year]/[file type]/[prod]/0000/ => len = 7
    # raw data:               /lhcb/data/2011/RAW/FULL/LHCb/COLLISION11/99983
    # => len 9 (under /lhcb/data/ from 2011 only RAW, before 2011 also other file types)
    # processed data: under both /lhcb/data and /lhcb/LHCb/
    #                         /lhcb/data/2010/DST/00009300/0000
    # data:                   /lhcb/LHCb/Collision12/ICHEP.DST/00017399/0000/
    self.dirDict = {}
    ignoredDirectories = dict.fromkeys( self.__ignoreDirsList, 0 )
    self.log.info( "Directories to be ignored: %s " % str( sorted( ignoredDirectories ) ) )
    for dirItem in totalDirList:
      # make sure that last character is a '/'
      dirItem = _standardDirectory( dirItem )
      splitDir = dirItem.split( '/' )
      if len( splitDir ) < 4:  # avoid picking up intermediate directories which don't contain files, like /lhcb/
        self.log.warn( "Directory %s skipped, as top directory" % dirItem )
        continue
      secDir = splitDir[ 2 ]
      if secDir in ignoredDirectories:
        self.log.verbose( "Directory to be ignored, skipped: %s " % dirItem )
        ignoredDirectories[ secDir ] += 1
        continue
      # for each type of directory (MC, reconstructed data and runs) check the format, in order not to count more than
      # once the productions with more than one sub-directory
      # for MC directories:
      # example: '/lhcb/MC/MC10/ALLSTREAMS.DST/00010908/0000/',
      # or        /lhcb/MC/2011/DST/00010870/0000
      # one directory for each file type
      # for data
      # /lhcb/LHCb/Collision11/SWIMSTRIPPINGD02KSPIPI.MDST/00019088/0000/
      # for raw data: /lhcb/data/2012/RAW/FULL/LHCb/COLLISION12/133784/
      try:
        dataType = splitDir[ -6 ]
        if dataType == "RAW":
          self.log.verbose( "RAW DATA directory: %s" % splitDir )
          directory = '/'.join( splitDir[:-1] )
          fullDirectory = directory
        else:
          suffix = splitDir[ -2 ]  # is the sub-directory suffix 0000, 0001, etc...
          self.log.verbose( "MC or reconstructed data directory: %s" % splitDir )
          if splitDir[-3] == 'HIST':
            directory = '/'.join( splitDir[:-1] )
            fullDirectory = directory
            self.log.verbose( "histo dir: %s " % directory )
          else:
            directory = '/'.join( splitDir[:-2] )
            fullDirectory = os.path.join( directory, suffix )
        directory = _standardDirectory( directory )
        fullDirectory = _standardDirectory( fullDirectory )
        if directory not in self.dirDict:
          self.dirDict[ directory ] = fullDirectory
        self.log.verbose( "Directory contains production files: %s " % directory )
      except:
        self.log.warn( "The directory has unexpected format: %s " % splitDir )

    self.lfnUsage = {}
    self.pfnUsage = {}
    totalDiscardedDirs = 0
    self.log.info( "Directories that have been discarded:" )
    for dd in ignoredDirectories:
      self.log.info( "/lhcb/%s - %d " % ( dd, ignoredDirectories[ dd ] ) )
      totalDiscardedDirs += ignoredDirectories[ dd ]
    self.log.info( "Total discarded directories: %d " % totalDiscardedDirs )
    self.log.info( "Retrieved %d dirs from StorageUsageDB containing prod files" % len( self.dirDict ) )
    self.log.info( "Getting the number of files and size from StorageUsage service" )
    for d in self.dirDict:
      self.log.verbose( "Get storage usage for directory %s " % d )
      res = self.__stDB.getDirectorySummaryPerSE( d )
      self.callsToDirectorySummary += 1
      if not res[ 'OK' ]:
        self.log.error( "Cannot retrieve PFN usage %s" % res['Message'] )
        continue
      if d not in self.pfnUsage:
        self.pfnUsage[ d ] = res['Value']
      self.log.verbose( "Get logical usage for directory %s " % d )
      res = self.__stDB.getSummary( d )
      self.callsToGetSummary += 1
      if not res[ 'OK' ]:
        self.log.error( "Cannot retrieve LFN usage %s" % res['Message'] )
        continue
      if not res['Value']:
        self.log.error( "For dir %s getSummary returned an empty value: %s " % ( d, str( res ) ) )
        continue
      self.lfnUsage.setdefault( d, {} )
      for retDir, dirInfo in res['Value'].items():
        if d in retDir:
          self.lfnUsage[ d ][ 'LfnSize' ] = dirInfo['Size']
          self.lfnUsage[ d ][ 'LfnFiles'] = dirInfo['Files']
      self.log.verbose( "PFN usage: %s" % self.pfnUsage[ d ] )
      self.log.verbose( "LFN usage: %s" % self.lfnUsage[ d ] )


    end = time.time()
    self.genTotalTime = end - start
    self.log.info( "StorageUsageDB dump completed in %d s" % self.genTotalTime )

    return S_OK()

  def __getEventTypeDescription( self, eventType ):
    # convert eventType to string:
    try:
      eventType = int( eventType )
    except:
      pass
    # check that the event type description is in the cached dictionary, and otherwise query the Bkk
    if eventType not in self.eventTypeDescription:
      self.log.notice( "Event type description not available for eventTypeID %s, getting from Bkk" % eventType )
      res = self.__bkClient.getAvailableEventTypes()
      self.callsToBkkGetEvtType += 1
      if not res['OK']:
        self.log.error( "Error querying the Bkk: %s" % res['Message'] )
      else:
        self.eventTypeDescription.update( dict( res['Value'] ) )
      self.log.verbose( "Updated  self.eventTypeDescription dict: %s " % str( self.eventTypeDescription ) )
      # If still not found, log it!
      if eventType not in self.eventTypeDescription:
        self.log.error( "EventType %s is not in cached dictionary" % str( eventType ) )

    return self.eventTypeDescription.get( eventType, 'na' )

  def fillAndSendAccountingRecord( self, lfnDir, metadataDict, now ):
    ''' Create, fill and send to accounting a record for the DataStorage type.
    '''
    dataRecord = DataStorage()
    dataRecord.setStartTime( now )
    dataRecord.setEndTime( now )
    logicalSize = self.lfnUsage[ lfnDir ][ 'LfnSize' ]
    logicalFiles = self.lfnUsage[ lfnDir ][ 'LfnFiles' ]
    dataRecord.setValueByKey( "LogicalSize", logicalSize )
    dataRecord.setValueByKey( "LogicalFiles", logicalFiles )
    for key in ( 'DataType', 'Activity', 'FileType', 'Production', 'ProcessingPass', 'Conditions', 'EventType' ):
      dataRecord.setValueByKey( key, metadataDict.get( key, 'na' ) )
    self.log.verbose( ">>> Send DataStorage record to accounting:" )
    self.log.verbose( "\tlfnFiles: %d lfnSize: %d " % ( logicalFiles, logicalSize ) )

    for se in self.pfnUsage[ lfnDir ]:
      self.log.verbose( "Filling accounting record for se %s" % se )
      physicalSize = self.pfnUsage[ lfnDir ][ se ][ 'Size' ]
      physicalFiles = self.pfnUsage[ lfnDir ][ se ][ 'Files' ]

      dataRecord.setValueByKey( "StorageElement", se )
      dataRecord.setValueByKey( "PhysicalSize", physicalSize )
      dataRecord.setValueByKey( "PhysicalFiles", physicalFiles )
      self.log.verbose( "\t\tStorageElement: %s --> physFiles: %d  physSize: %d " % ( se, physicalFiles, physicalSize ) )

      # addRegister is NOT making a copy, therefore all records are otherwise overwritten
      res = gDataStoreClient.addRegister( copy.deepcopy( dataRecord ) )
      if not res[ 'OK']:
        self.log.error( "addRegister returned: %s" % res )
        return S_ERROR( "addRegister returned: %s" % res )
      # Reset logical information to zero in order to send it only once!
      dataRecord.setValueByKey( "LogicalSize", 0 )
      dataRecord.setValueByKey( "LogicalFiles", 0 )
      self.totalRecords += 1
      self.recordsToCommit += 1

    # Commit if necessary
    if self.recordsToCommit > self.limitForCommit:
      self.__commitRecords()

    return S_OK()
