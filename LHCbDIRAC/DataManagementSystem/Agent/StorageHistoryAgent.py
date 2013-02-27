'''   The Storage History Agent will create a summary of the
      storage usage DB grouped by processing pass or other
      interesting parameters.

      Initially this will dump the information to a file but eventually
      can be inserted in a new DB table and made visible via the web portal.
'''

__RCSID__ = "$Id$"

import os, time
from types import IntType, StringType

from DIRAC                                                  import S_OK, S_ERROR, gLogger
from DIRAC.Core.Utilities                                   import Time

from DIRAC.Core.Base.AgentModule                            import AgentModule
from DIRAC.AccountingSystem.Client.DataStoreClient          import gDataStoreClient

from LHCbDIRAC.AccountingSystem.Client.Types.UserStorage    import UserStorage
from LHCbDIRAC.AccountingSystem.Client.Types.Storage        import Storage
from LHCbDIRAC.AccountingSystem.Client.Types.DataStorage    import DataStorage
from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient   import BookkeepingClient
from LHCbDIRAC.DataManagementSystem.Client.DataUsageClient  import DataUsageClient

byteToGB = 1.0e9

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
    self.bkClient = BookkeepingClient()
    self.__dataUsageClient = DataUsageClient()
    self.__workDirectory = self.am_getOption( "WorkDirectory" )
    if not os.path.isdir( self.__workDirectory ):
      os.makedirs( self.__workDirectory )
    self.log.info( "Working directory is %s" % self.__workDirectory )
    self.bkkCacheTimeout = self.am_getOption( 'BookkeepingCacheTimeout', 259200 )  # by default 3 days
    self.__ignoreDirsList = self.am_getOption( 'Ignore', [] )
    self.log.info( "List of directories to ignore for the DataStorage accounting: %s " % self.__ignoreDirsList )

    return S_OK()

  def execute( self ):
    if self.am_getOption( "CleanBefore", False ):
      self.log.notice( "Cleaning the DB" )
      result = self.__stDB.purgeOutdatedEntries( "/lhcb/user", self.am_getOption( "OutdatedSeconds", 86400 * 10 ) )
      if not result[ 'OK' ]:
        return result
      self.log.notice( "Purged %s outdated records" % result[ 'Value' ] )
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

    # ##
    # Accounting records relative to the general Storage type:
    # ##
    ftb = 1.0e12

    # get info from the DB about the LOGICAL STORAGE USAGE (from the su_Directory table):
    result = self.__stDB.getSummary( '/lhcb/' )
    if not result[ 'OK' ]:
      return result
    logicalUsage = result['Value']
    topDirLogicalUsage = {}  # build the list of first level directories
    for row in logicalUsage.keys():
      # d, size, files = row
      d = row
      files = logicalUsage[ d ][ 'Files' ]
      size = logicalUsage[ d ][ 'Size' ]
      splitDir = d.split( "/" )
      if len( splitDir ) > 3:  # skip the root directory "/lhcb/"
        firstLevelDir = '/' + splitDir[1] + '/' + splitDir[2] + '/'
        if firstLevelDir not in topDirLogicalUsage.keys():
          topDirLogicalUsage[ firstLevelDir ] = {}
          topDirLogicalUsage[ firstLevelDir ][ 'Files' ] = 0
          topDirLogicalUsage[ firstLevelDir ][ 'Size' ] = 0
        topDirLogicalUsage[ firstLevelDir ][ 'Files' ] += files
        topDirLogicalUsage[ firstLevelDir ][ 'Size' ] += size
    self.log.notice( "Summary on logical usage of top directories: " )
    for d in topDirLogicalUsage.keys():
      self.log.notice( "dir: %s size: %.4f TB  files: %d" % ( d, topDirLogicalUsage[d]['Size'] / ftb,
                                                              topDirLogicalUsage[d]['Files'] ) )

    # loop on top level directories (/lhcb/data, /lhcb/user/, /lhcb/MC/, etc..)
    # to get the summary in terms of PHYSICAL usage grouped by SE:
    SEData = {}
    for directory in topDirLogicalUsage.keys():
      result = self.__stDB.getDirectorySummaryPerSE( directory )  # retrieve the PHYSICAL usage
      if not result[ 'OK' ]:
        return result
      SEData[ directory ] = result[ 'Value' ]
      self.log.notice( "Got SE summary for %s directories " % ( len( SEData ) ) )
      self.log.debug( "SEData: %s" % SEData )
    # loop on top level directories to send the accounting records
    numRows = 0
    for directory in SEData.keys():
      self.log.debug( "dir: %s SEData: %s " % ( directory, SEData[ directory ] ) )
      if directory not in topDirLogicalUsage.keys():
        self.log.error( "Dir %s is in the summary per SE, but it is not in the logical files summary!" % directory )
        continue
      for se in sorted( SEData[ directory ].keys() ):
        storageRecord = Storage()
        storageRecord.setStartTime( now )
        storageRecord.setEndTime( now )
        storageRecord.setValueByKey( "Directory", directory )
        storageRecord.setValueByKey( "StorageElement", se )
        logicalFiles = topDirLogicalUsage[ directory ][ 'Files' ]
        logicalSize = topDirLogicalUsage[ directory ][ 'Size' ]
        storageRecord.setValueByKey( "LogicalFiles", logicalFiles )
        storageRecord.setValueByKey( "LogicalSize", logicalSize )
        try:
          physicalFiles = SEData[ directory ][ se ][ 'Files' ]
        except:
          self.log.notice( "WARNING! no files replicas for directory %s on SE %s" % ( directory, se ) )
          physicalFiles = 0
        try:
          physicalSize = SEData[ directory ][ se ][ 'Size' ]
        except:
          self.log.notice( "WARNING! no size for replicas for directory %s on SE %s" % ( directory, se ) )
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

    self.log.notice( "-------------------------------------------------------------------------------------\n" )
    self.log.notice( "Generate accounting records for DataStorage type " )
    self.log.notice( "-------------------------------------------------------------------------------------\n" )


    # counter for DataStorage records, commit to the accounting in bunches of self.limitForCommit records
    self.totalRecords = 0
    self.recordsToCommit = 0
    self.limitForCommit = 200
    self.log.notice( " Call the function to create the StorageUsageDB dump.." )
    res = self.generateStorageUsageDictionaryPerDirectory()
    if not res[ 'OK' ]:
      self.log.error( "ERROR generating the StorageUsageDB dump per directory" )
      return S_ERROR()

    # keep count of calls to Bkk
    callsToBkkgetDirectoryMetadata = 0
    self.callsToBkkGetEvtType = 0
    # Keep a list of all directories in LFC that are not found in the Bkk
    directoriesNotInBkk = []
    # build a dictionary with Event Type descriptions (to be send to accounting, instead of number Event Type ID)
    self.eventTypeDescription = {}
    self.eventTypeDescription[ 'na' ] = 'na'
    self.eventTypeDescription[ 'notInBkk' ] = 'notInBkk'
    self.eventTypeDescription[ 'FailedBkkQuery' ] = 'FailedBkkQuery'
    # for debugging purposes build dictionaries with storage usage to compare with the accounting plots
    self.debug_seUsage = {}
    self.debug_seUsage_acc = {}

    # set the time for the accounting records (same time for all records)
    self.now = Time.dateTime()
    # loop on all directories  to get the bkk metadata
    # for dirLfn in self.dirList:
    for dirLfn in self.dirDict.keys():
      gLogger.info( "Processing directory %s " % dirLfn )
      # for DEBUGGING:
      for se in self.PFNUsage[ dirLfn ]:
        if se not in self.debug_seUsage.keys():
          self.debug_seUsage[ se ] = {'Files': 0 , 'Size' : 0 }
        physicalSize = self.PFNUsage[ dirLfn ][ se ][ 'Size' ]
        physicalFiles = self.PFNUsage[ dirLfn ][ se ][ 'Files' ]
        self.debug_seUsage[ se ][ 'Files' ] += physicalFiles
        self.debug_seUsage[ se ][ 'Size' ] += physicalSize
      # end of DEBUGGING

      self.metadataForAcc = {}
      # initialize the values to fill the accounting record
      self.FillMetadata( self.metadataForAcc, 'na' )
      gLogger.info( "self.metadataForAcc initialized as: %s " % self.metadataForAcc )
      if dirLfn not in self.PFNUsage.keys():
        gLogger.error( "ERROR: directory does not have PFN usage %s " % dirLfn )
        continue
      gLogger.verbose( "PFN usage: %s " % self.PFNUsage[ dirLfn ] )
      if dirLfn not in self.LFNUsage.keys():
        gLogger.error( "ERROR: directory does not have LFN usage %s " % dirLfn )
        continue
      gLogger.verbose( "LFN usage: %s " % self.LFNUsage[ dirLfn ] )

      # try to query the cached table:
      fullDirectory = self.dirDict[ dirLfn ]
      if dirLfn not in fullDirectory:
        gLogger.error( "ERROR: fullDirectory should include the dirname: %s %s " % ( fullDirectory, dirLfn ) )
        continue

      dirList = [ fullDirectory ]  # convert to list format
      gLogger.info( "call getDirMetadata for: %s " % dirList )
      res = self.__dataUsageClient.getDirMetadata( dirList )  # this could be a bulk query for a list of directories
      gLogger.info( "getDirMetadata returned: %s " % res )
      if not res[ 'OK' ]:
        gLogger.error( "Error retrieving directory meta-data %s " % res['Message'] )
        # self.FillMetadata( self.metadataForAcc, 'na' )
        # this usually happens when directories are removed from LFC between the StorageUsageDB dump and this call,
        # if the Db is refreshed exactly in this time interval. Not really a problem.
        #######################3 just a try ##############################################3
        continue
      if res['Value']:
        for row in res['Value']:
          _dirID, configName, configVersion, conditions, processingPass, eventType, fileType, production = row
          self.metadataForAcc[ 'ConfigName' ] = configName
          self.metadataForAcc[ 'ConfigVersion' ] = configVersion
          self.metadataForAcc[ 'ConditionDescription' ] = conditions
          self.metadataForAcc[ 'ProcessingPass' ] = processingPass
          self.metadataForAcc[ 'EventType' ] = eventType
          self.metadataForAcc[ 'FileType' ] = fileType
          self.metadataForAcc[ 'Production' ] = production

      else:
        gLogger.info( "No result returned from getDirMetadata => \
        Query Bookkeeping to retrieve meta-data for %s directory and then cache it" % dirLfn )
        res = self.bkClient.getDirectoryMetadata( dirLfn )
        callsToBkkgetDirectoryMetadata += 1
        if not res[ 'OK' ]:
          gLogger.error( "Totally failed to query Bookkeeping %s" % res[ 'Message' ] )
          self.FillMetadata( self.metadataForAcc, 'FailedBkkQuery' )
        else:
          gLogger.info( "Successfully queried Bookkeeping, result: %s " % res )
          if not res['Value']:
            gLogger.warn( "Directory is not registered in Bookkeeping! %s " % dirLfn )
            self.FillMetadata( self.metadataForAcc, 'notInBkk' )
            directoriesNotInBkk.append( dirLfn )
          else:
            metadata = res['Value'][ 0 ]
            self.FillMetadata( self.metadataForAcc, metadata )
            gLogger.info( "Cache this entry in DirMetadata table.." )
            dirMetadataDict = {}
            # dirMetadataDict[ dirLfn ] = metadata
            dirMetadataDict[ fullDirectory ] = metadata
            resInsert = self.__dataUsageClient.insertToDirMetadata( dirMetadataDict )
            if not resInsert[ 'OK' ]:
              gLogger.error( "Failed to insert metadata in DirMetadata table! %s " % resInsert[ 'Message' ] )
            else:
              gLogger.info( "Successfully inserted metadata for directory %s in DirMetadata table " % dirMetadataDict )
              gLogger.verbose( "result: %s " % resInsert )

      gLogger.info( "Fill the record for Dir: %s and metadata: %s " % ( dirLfn, self.metadataForAcc ) )
      for se in self.PFNUsage[ dirLfn ]:
        gLogger.verbose( "Filling accounting record for se %s" % se )
        res = self.fillAndSendAccountingRecord( dirLfn, se, self.metadataForAcc )
        if not res['OK']:
          return res
        if se not in self.debug_seUsage_acc.keys():
          self.debug_seUsage_acc[ se ] = { 'Files': 0 , 'Size': 0 }
        physicalSize = self.PFNUsage[ dirLfn ][ se ][ 'Size' ]
        physicalFiles = self.PFNUsage[ dirLfn ][ se ][ 'Files' ]
        self.debug_seUsage_acc[ se ][ 'Files' ] += physicalFiles
        self.debug_seUsage_acc[ se ][ 'Size' ] += physicalSize

      if self.recordsToCommit > self.limitForCommit:
        res = gDataStoreClient.commit()
        if not res[ 'OK' ]:
          self.log.error( "Accounting ERROR: commit returned %s" % res )
        else:
          self.log.notice( "%d records committed " % self.recordsToCommit )
          self.recordsToCommit = 0
          self.log.notice( "commit for DataStorage returned: %s" % res )



    gLogger.notice( "-------------------------------------------------------------------------------------\n" )
    gLogger.notice( "------ End of cycle report for DataStorage accounting--------------------------------\n" )
    gLogger.notice( "Total directories found in LFC:  %d " % len( self.dirDict.keys() ) )
    totalCallsToStorageUsage = self.getSummaryCalls + self.getDirectorySummaryPerSECalls
    gLogger.notice( "Total calls to StorageUsage: %d , took: %d s " % ( totalCallsToStorageUsage, self.SUDBtotalTime ) )
    totalCallsToBkk = callsToBkkgetDirectoryMetadata + self.callsToBkkGetEvtType
    gLogger.notice( "Total calls to Bookkeeping: %d (getDirectoryMetadata: %d, getEventType: %d)" % ( totalCallsToBkk,
                                                                                         callsToBkkgetDirectoryMetadata,
                                                                                         self.callsToBkkGetEvtType ) )
    gLogger.notice( "Total records sent to accounting for DataStorage:  %d " % self.totalRecords )
    gLogger.notice( "Directories not found in Bookkeeping: %d " % ( len( directoriesNotInBkk ) ) )
    fileName = os.path.join( self.__workDirectory, "directoriesNotInBkk.txt" )
    gLogger.notice( "written to file: %s " % fileName )
    f = open( fileName, "w" )
    for d in directoriesNotInBkk:
      f.write( "%s\n" % d )
    f.close()
    f = open( fileName, "w" )
    for d in directoriesNotInBkk:
      f.write( "%s\n" % d )
    f.close()
    # for DEBUG only
    gLogger.info( "Summary of StorageUsage: files size " )
    seList = self.debug_seUsage.keys()
    seList.sort()
    for se in seList:
      gLogger.info( "all: %s  %d %d Bytes ( %.2f TB ) " % ( se, self.debug_seUsage[ se ]['Files'],
                                                            self.debug_seUsage[ se ]['Size'],
                                                            self.debug_seUsage[ se ]['Size'] / 1.0e12 ) )
      if se in self.debug_seUsage_acc.keys():
        gLogger.info( "acc: %s  %d %d Bytes ( %.2f TB ) " % ( se, self.debug_seUsage_acc[ se ]['Files'],
                                                              self.debug_seUsage_acc[ se ]['Size'],
                                                              self.debug_seUsage_acc[ se ]['Size'] / 1.0e12 ) )
      else:
        gLogger.info( "SE not in self.debug_seUsage_acc keys" )
    return S_OK()


  def FillMetadata( self, dictToFill, metadataValue ):
    ''' Fill the dictionary to send to the accounting.
        If metadataValue is a string then set all the values of dictToFill, to this value
        if metadataValue is a dictionary then set each value of dictToFill to the corresponding value of metadataValue
    '''
    # ds = DataStorage()
    # keyList = ds.keyFieldsList
    # this is the list of attributes returned by the Bookkeeping for a given directory
    keyList = ['ConfigName', 'ConfigVersion', 'FileType', 'Production',
               'ProcessingPass', 'ConditionDescription', 'EventType']
    gLogger.verbose( "In FillMetadata dictToFill(start): %s  - metadataValue: %s " % ( dictToFill, metadataValue ) )
    if type( metadataValue ) == type( '' ):
      for k in keyList:
        dictToFill[ k ] = metadataValue
    elif type( metadataValue ) == type( {} ):
      for k in keyList:
        try:
          dictToFill = metadataValue[ k ]
        except KeyError:
          gLogger.error( "In FillMetadata Key not available: %s " % k )
    else:
      gLogger.error( "In FillMetadata: the argument metadataValue must be either string or dictionary" )

    gLogger.verbose( "In FillMetadata dictToFill(end): %s  - metadataValue: %s " % ( dictToFill, metadataValue ) )



  def generateStorageUsageDictionaryPerDirectory( self ):
    '''Generate a dump of the StorageUsageDB and keep it in memory in a dictionary
       (new version of Apr 2012)
    '''

    self.getSummaryCalls = 0
    self.getDirectorySummaryPerSECalls = 0
    start = time.time()
    gLogger.notice( 'Starting from path: /lhcb/' )
    res = self.__stDB.getStorageDirectories( '/lhcb/' )
    if not res['OK']:
      return S_ERROR()
    self.totalDirList = res['Value']
    gLogger.info( "Total directories retrieved from StorageUsageDB: %d " % len( self.totalDirList ) )
    # select only terminal directories (directories without sub-directories)
    # mc directory structure: /lhcb/MC/[year]/[file type]/[prod]/0000/ => len = 7
    # raw data:               /lhcb/data/2011/RAW/FULL/LHCb/COLLISION11/99983
    # => len 9 (under /lhcb/data/ from 2011 only RAW, before 2011 also other file types)
    # processed data: under both /lhcb/data and /lhcb/LHCb/
    #                         /lhcb/data/2010/DST/00009300/0000
    # data:                   /lhcb/LHCb/Collision12/ICHEP.DST/00017399/0000/
    self.dirList = []  # list of directories containing files
    self.dirDict = {}
    directoriesNotInBookkeeping = {}
    for idir in self.__ignoreDirsList:
      directoriesNotInBookkeeping[ idir ] = 0
    gLogger.info( "Directories will be ignored: content not in the BKK: %s " % directoriesNotInBookkeeping )
    for dirItem in self.totalDirList:
      # make sure that last character is a '/'
      if dirItem[-1] != "/":
        dirItem = "%s/" % dirItem
      splitDir = dirItem.split( '/' )
      if len( splitDir ) < 4:  # avoid picking up intermediate directories which don't contain files, like /lhcb/
        gLogger.warn( "Directory %s does not contain files" % dirItem )
        continue
      secDir = splitDir[ 2 ]
      if secDir in directoriesNotInBookkeeping.keys():
        gLogger.verbose( "Not a production directory. Skip it. %s " % dirItem )
        directoriesNotInBookkeeping[ secDir ] += 1
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
          gLogger.verbose( "RAW DATA directory: %s" % splitDir )
          directory = '/'.join( splitDir[:-1] )
          fullDirectory = directory
        else:
          suffix = splitDir[ -2 ]  # is the sub-directory suffix 0000, 0001, etc...
          gLogger.verbose( "MC or reconstructed data directory: %s" % splitDir )
          if splitDir[-3] == 'HIST':
            directory = '/'.join( splitDir[:-1] )
            fullDirectory = directory
            gLogger.verbose( "histo dir: %s " % directory )
          else:
            directory = '/'.join( splitDir[:-2] )
            if directory[-1] != '/':
              directory = "%s/" % directory
            fullDirectory = directory + suffix
        if directory[-1] != '/':
          directory = "%s/" % directory
        if fullDirectory[-1] != '/':
          fullDirectory = "%s/" % fullDirectory
        if directory not in self.dirDict.keys():
          self.dirDict[ directory ] = fullDirectory
        gLogger.verbose( "Directory contains production files: %s " % directory )
      except:
        gLogger.info( "The directory has unexpected format: %s " % splitDir )

    self.LFNUsage = {}
    self.PFNUsage = {}
    totalDiscardedDirs = 0
    gLogger.info( "Directories that have been discarded:" )
    for dd in directoriesNotInBookkeeping.keys():
      gLogger.info( "/lhcb/%s - %d " % ( dd, directoriesNotInBookkeeping[ dd ] ) )
      totalDiscardedDirs += directoriesNotInBookkeeping[ dd ]
    gLogger.info( "total discarded directories: %d " % totalDiscardedDirs )
    gLogger.info( "Retrieved %d dirs from StorageUsageDB containing prod files: %d " % len( self.dirDict.keys() ) )
    gLogger.info( "Getting the number of files and size from StorageUsage service" )
    for d in self.dirDict.keys():
      gLogger.verbose( "Get storage usage for directory %s " % d )
      res = self.__stDB.getDirectorySummaryPerSE( d )
      self.getDirectorySummaryPerSECalls += 1
      if not res[ 'OK' ]:
        gLogger.error( "Cannot retrieve PFN usage %s" % res['Message'] )
        continue
      if d not in self.PFNUsage:
        self.PFNUsage[ d ] = res['Value']
      gLogger.verbose( "Get logical usage for directory %s " % d )
      res = self.__stDB.getSummary( d )
      self.getSummaryCalls += 1
      if not res[ 'OK' ]:
        gLogger.error( "Cannot retrieve LFN usage %s" % res['Message'] )
        continue
      if not res['Value']:
        gLogger.error( "For dir % getSummary returned an empty value: %s " % ( d, res ) )
        continue
      for retDir in res['Value'].keys():
        if retDir[-1] != '/':
          retDir = retDir + '/'
        if d not in self.LFNUsage.keys():
          self.LFNUsage[ d ] = {}
        if d in retDir:
          self.LFNUsage[ d ][ 'LfnSize' ] = res['Value'][ retDir ]['Size']
          self.LFNUsage[ d ][ 'LfnFiles'] = res['Value'][ retDir ]['Files']
      gLogger.verbose( "PFN usage: %s" % self.PFNUsage[ d ] )
      gLogger.verbose( "LFN usage: %s" % self.LFNUsage[ d ] )


    end = time.time()
    self.SUDBtotalTime = end - start
    gLogger.info( "StorageUsageDB dump completed in %d s" % self.SUDBtotalTime )

    return S_OK()

  def fillAndSendAccountingRecord( self, lfnDir, se, metadataDict ):
    ''' Create, fill and send to accounting a record for the DataStorage type.
    '''

    physicalSize = self.PFNUsage[ lfnDir ][ se ][ 'Size' ]
    physicalFiles = self.PFNUsage[ lfnDir ][ se ][ 'Files' ]
    logicalSize = self.LFNUsage[ lfnDir ][ 'LfnSize' ]
    logicalFiles = self.LFNUsage[ lfnDir ][ 'LfnFiles' ]

    try:
      configName = metadataDict['ConfigName']
      configVersion = metadataDict['ConfigVersion']
      fileType = metadataDict['FileType']
      production = metadataDict['Production']
      processingPass = metadataDict['ProcessingPass']
      conditions = metadataDict['ConditionDescription']
      eventType = metadataDict['EventType']
    except KeyError, e:
      gLogger.error( "In fillAndSendAccountingRecord: Parameter not available in metadataDict %s " % e )
      return S_ERROR( "Parameter not available in metadataDict" )
    # convert eventType to string:
    if type( eventType ) != StringType:
      if type( eventType ) == IntType:
        eventType = str( eventType )
      else:
        gLogger.error( "Wrong format for eventType %s %s" % ( eventType, type( eventType ) ) )

    # check that the event type description is in the cached dictionary, and otherwise query the Bkk
    if eventType not in self.eventTypeDescription.keys():
      gLogger.notice( "Event type description not available for eventTypeID %s, getting from Bkk" % eventType )
      bkDict = {'ConfigName': configName, 'ConfigVersion': configVersion }
      res = self.bkClient.getEventTypes( bkDict )
      self.callsToBkkGetEvtType += 1
      if not res['OK']:
        gLogger.error( "Error querying the Bkk: %s" % res['Message'] )
      else:
        val = res['Value']
        for record in val['Records']:
          indexId = val['ParameterNames'].index( 'EventTypeId' )
          indexDesc = val['ParameterNames'].index( 'Description' )
          eventTypeID = record[ indexId ]
          eventTypeDescription = record[ indexDesc ]
          self.eventTypeDescription[ str( eventTypeID ) ] = eventTypeDescription
      gLogger.info( "Updated  self.eventTypeDescription dict: %s " % self.eventTypeDescription )


    try:
      eventTypeDescription = self.eventTypeDescription[ eventType]
    except KeyError, e:
      gLogger.error( "The EventType is not in cached dictionary: %s " % e )
      eventTypeDescription = 'na'

    dataRecord = DataStorage()
    dataRecord.setStartTime( self.now )
    dataRecord.setEndTime( self.now )
    dataRecord.setValueByKey( "DataType", configName )
    dataRecord.setValueByKey( "Activity", configVersion )
    dataRecord.setValueByKey( "FileType", fileType )
    dataRecord.setValueByKey( "Production", production )
    dataRecord.setValueByKey( "ProcessingPass", processingPass )
    dataRecord.setValueByKey( "Conditions", conditions )
    dataRecord.setValueByKey( "EventType", eventTypeDescription )
    dataRecord.setValueByKey( "StorageElement", se )
    dataRecord.setValueByKey( "PhysicalSize", physicalSize )
    dataRecord.setValueByKey( "PhysicalFiles", physicalFiles )
    dataRecord.setValueByKey( "LogicalSize", logicalSize )
    dataRecord.setValueByKey( "LogicalFiles", logicalFiles )
    gLogger.notice( ">>>>>>>>Send DataStorage record to accounting for fields: \
    DataType: %s \
    Activity: %s \
    FileType: %s \
    Production: %s \
    ProcessingPass: %s \
    Conditions: %s \
    EventType: %s \
    StorageElement: %s \
    --> physFiles: %d  \
    physSize: %d \
    lfnFiles: %d \
    lfnSize: %d " % ( configName, configVersion, fileType, production, processingPass, conditions,
                      eventTypeDescription, se, physicalFiles, physicalSize, logicalFiles, logicalSize ) )

    res = gDataStoreClient.addRegister( dataRecord )
    if not res[ 'OK']:
      self.log.error( "addRegister returned: %s" % res )
      return S_ERROR( "addRegister returned: %s" % res )
    self.totalRecords += 1
    self.recordsToCommit += 1

    return S_OK()

