########################################################################
# File: StorageHistoryAgent.py
########################################################################
""" :mod: StorageHistoryAgent 
    =========================
 
    .. module: StorageHistoryAgent
    :synopsis: The Storage History Agent will create a summary of the
    storage usage DB grouped by processing pass or other interesting parameters.

    Initially this will dump the information to a file but eventually
    can be inserted in a new DB table and made visible via the web portal.
"""
## imports
import os
import re
import time
## from DIRAC
from DIRAC  import S_OK, S_ERROR
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.Core.Utilities import Time
from DIRAC.Core.Utilities.List import sortList
from DIRAC.AccountingSystem.Client.DataStoreClient import gDataStoreClient
from DIRAC.Core.DISET.RPCClient import RPCClient
## from LHCbDIRAC
from LHCbDIRAC.AccountingSystem.Client.Types.UserStorage import UserStorage
from LHCbDIRAC.AccountingSystem.Client.Types.Storage import Storage
from LHCbDIRAC.AccountingSystem.Client.Types.DataStorage import DataStorage
from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
from LHCbDIRAC.DataManagementSystem.DB.StorageUsageDB import StorageUsageDB

__RCSID__ = "$Id$"

AGENT_NAME = "DataManagement/StorageHistoryAgent"

# gB2GB = 1.0e9

class StorageHistoryAgent( AgentModule ):
  """
  .. class:: StorageHistoryAgent

  :param mixed storageUsage: StorageUsageDB instance or its rpc client
  :param BookkeepingClient bkClient: BookkeepingClient instance 
  :param str workDirectory: abs path to work directory
  :param int bkkCacheTimeout: cache validity period
  """
  storageUsage = None
  bkClient = None
  workDirectory = None
  bkkCacheTimeout = 259200

  dataDirs = None
  dataSum = None
  prodsNotInLFC = None
  persistentDictList = None
  rawDirs = None
  limitForCommit = None
  totalRecords = None
  dict1 = None
  recordsToCommit = None
  mcSum = None
  eventTypeDescription = None
  proPassTuples = None
  totalBkkProductions = None
  totalBkkRuns = None
  runsNotInLFC = None
  rawSum = None
  prodsInLFC = None
  numDataRows = None
  mcDirs = None
  bkkDictFile = None 
  runsInLFC = None

  def initialize( self ):
    """ agent initialisation """
    self.am_setOption( 'PollingTime', 43200 )
    if self.am_getOption( 'DirectDB', False ):
      self.storageUsage = StorageUsageDB()
    else:
      self.storageUsage = RPCClient( 'DataManagement/StorageUsage' )
    self.bkClient = BookkeepingClient()

    self.workDirectory =  self.am_getOption( "WorkDirectory" )
    if not os.path.isdir( self.workDirectory ):
      os.makedirs( self.workDirectory )
    self.log.info( "Working directory is %s" % self.workDirectory )
    self.bkkCacheTimeout = self.am_getOption( 'BookkeepingCacheTimeout', self.bkkCacheTimeout ) # by default 3 days
    return S_OK()

  def execute( self ):
    """ execution in one cycle """
    if self.am_getOption( "CleanBefore", False ):
      self.log.notice( "Cleaning the DB" )
      result = self.storageUsage.purgeOutdatedEntries( "/lhcb/user", 
                                                       self.am_getOption( "OutdatedSeconds", 86400 * 10 ) )
      if not result[ 'OK' ]:
        return result
      self.log.notice( "Purged %s outdated records" % result[ 'Value' ] )
    result = self.storageUsage.getUserSummary()
    if not result[ 'OK' ]:
      return result
    userCatalogData = result[ 'Value' ]
    print userCatalogData
    self.log.notice( "Got summary for %s users" % ( len( userCatalogData ) ) )
    result = self.storageUsage.getUserSummaryPerSE()
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

    ###
    # Accounting records relative to the general Storage type:
    ###
    ftb = 1.0e12

    # get info from the DB about the LOGICAL STORAGE USAGE (from the su_Directory table):
    result = self.storageUsage.getSummary( '/lhcb/' )
    if not result[ 'OK' ]:
      return result
    logicalUsage = result['Value']
    topDirLogicalUsage = {} # build the list of first level directories
    for row in logicalUsage.keys():
      #self.log.debug("row is %s " %row)
      #d, size, files = row
      #d = row
      files = logicalUsage[ row ][ 'Files' ]
      size = logicalUsage[ row ][ 'Size' ]
      splitDir = row.split( "/" )
      if len( splitDir ) > 3: # skip the root directory "/lhcb/"
        firstLevelDir = '/' + splitDir[1] + '/' + splitDir[2] + '/'
        if firstLevelDir not in topDirLogicalUsage.keys():
          topDirLogicalUsage[ firstLevelDir ] = {}
          topDirLogicalUsage[ firstLevelDir ][ 'Files' ] = 0
          topDirLogicalUsage[ firstLevelDir ][ 'Size' ] = 0
        topDirLogicalUsage[ firstLevelDir ][ 'Files' ] += files
        topDirLogicalUsage[ firstLevelDir ][ 'Size' ] += size
    self.log.notice( "Summary on logical usage of top directories: " )
    for folder in topDirLogicalUsage:
      self.log.notice( "dir: %s size: %.4f TB  files: %d" % ( folder, 
                                                              topDirLogicalUsage[folder]['Size'] / ftb, 
                                                              topDirLogicalUsage[folder]['Files'] ) )

    # loop on top level directories (/lhcb/data, /lhcb/user/, /lhcb/MC/, etc..) 
    #  to get the summary in terms of PHYSICAL usage grouped by SE:
    seData = {}
    for directory in topDirLogicalUsage.keys():
      result = self.storageUsage.getDirectorySummaryPerSE( directory ) # retrieve the PHYSICAL usage
      if not result[ 'OK' ]:
        return result
      seData[ directory ] = result[ 'Value' ]
      self.log.notice( "Got SE summary for %s directories " % ( len( seData ) ) )
      self.log.debug( "seData: %s" % seData )
    # loop on top level directories to send the accounting records
    numRows = 0
    for directory in seData.keys():
      self.log.debug( "dir: %s seData: %s " % ( directory, seData[ directory ] ) )
      if directory not in topDirLogicalUsage:
        self.log.error( "directory %s is in the summary per SE, but missing in the logical files summary!" % directory )
        continue
      for se in sorted( seData[ directory ].keys() ):
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
          physicalFiles = seData[ directory ][ se ][ 'Files' ]
        except KeyError:
          self.log.notice( "WARNING! no files replicas for directory %s on SE %s" % ( directory, se ) )
          physicalFiles = 0
        try:
          physicalSize = seData[ directory ][ se ][ 'Size' ]
        except KeyError:
          self.log.notice( "WARNING! no size for replicas for directory %s on SE %s" % ( directory, se ) )
          physicalSize = 0
        storageRecord.setValueByKey( "PhysicalFiles", physicalFiles )
        storageRecord.setValueByKey( "PhysicalSize", physicalSize )
        gDataStoreClient.addRegister( storageRecord )
        numRows += 1
        self.log.debug( "Directory: %s SE: %s physical size: %.4f TB (%d files)" % ( directory, se, 
                                                                                     physicalSize/ftb, physicalFiles ) )

    self.log.notice( "Sending %s records to accounting for top level directories storage" % numRows )
    res = gDataStoreClient.commit()
    if not res[ 'OK' ]:
      self.log.notice( "ERROR: committing Storage records: %s " % res )
      return S_ERROR( res )
    else:
      self.log.notice( "%s records for Storage type successfully committed" % numRows )

    self.log.notice("-------------------------------------------------------------------------------------\n")
    self.log.notice("Generate accounting records for DataStorage type ")
    self.log.notice("-------------------------------------------------------------------------------------\n")

    # work flow:
    # 1. queries the BKK client to get the set of data taking conditions relative to: 
    #    eventType=93000000,ConfigVersion': 'Collision10', 'ConfigName': 'LHCb'.
    # 2- loops on the data taking conditions and retrieve for each of them the set of all existing processing passes.
    #    Loops on all processing passes:
    #    2.1 - for every ( data taking condition, proc. pass) queries the BKK client to get the list of productions/runs
    #    2.2 - for production, gets the list of file types and adds the HIST, DST and SETC (not clear..) and fill 
    #          and accounting record per SE.

    # counter for DataStorage records, commit to the accounting in bunches of self.limitForCommit records
    self.totalRecords = 0
    self.recordsToCommit = 0
    self.limitForCommit = 200
    # keep a counter for all runs and productions returned by the Bkk queries, 
    # to compare with those stored in the LFC and check consistency
    self.totalBkkProductions = []
    self.totalBkkRuns = []
 
    self.log.notice(" Call the function to create the StorageUsageDB dump.." )
    res = self.generateStorageUsageDictionary()
    if not res[ 'OK' ]:
      self.log.error("ERROR generating the StorageUsageDB dump")
      return S_ERROR()
           
    self.eventTypeDescription = {}
    self.persistentDictList = [] # persistency Bookkeeping dictionary    


    self.log.notice(" Try to read cached Bookkeeping dictionary from disk..")
    bkkCachedInfo = False
    self.bkkDictFile = os.path.join( self.workDirectory, "bkkPersistentDict.txt" )
    if not os.path.exists(self.bkkDictFile):
      self.log.notice("Could not read cached Bookkeeping dictionary from file => regenerate the dictionary")
    else:
      self.log.notice("File with cached Bookkeeping dictionary found. Checking the creation time...")
      #(mode, ino, dev, nlink, uid, gid, size, atime, mtime, ctime) = os.stat( self.bkkDictFile )
      mtime = os.path.getmtime( self.bkkDictFile ) 
      elapsedTime = time.time() - mtime
      self.log.notice("Creation time: %s elapsed time: %d h (max delay allowed: %d h ) " %( time.ctime( mtime ), 
                                                                                            elapsedTime/3600, 
                                                                                            self.bkkCacheTimeout/3600) )
      if elapsedTime > self.bkkCacheTimeout:
        self.log.warn( "Bookkeeping cached dictionary is older than maximum limit! %s s ( %d h ) " \
                         " => It will be re-created and dump to file: %s " % ( self.bkkCacheTimeout, 
                                                                               self.bkkCacheTimeout/3600, 
                                                                               self.bkkDictFile ) ) 
      else:
        self.log.notice("Bookkeeping cached dictionary is fresh") 
        bkkCachedInfo = True

    if not bkkCachedInfo: 
      creationStart =  time.time()
      res = self.generateBookkeepingDictionary()
      if not res[ 'OK' ]:
        self.log.error("ERROR! %s " % res )
        return S_ERROR( res )
      creationEnd = time.time()
      self.log.notice("Bookkeeping dictionary creation took: %d s " %(creationEnd-creationStart) )
    self.log.notice("Reading Bookkeeping persistent dictionary from file %s " % self.bkkDictFile )
    for line in open( self.bkkDictFile , "r" ).readlines():
      thisList = eval( line )
      self.persistentDictList.append( thisList ) # list of Bkk dictionaries
    totalBookkeepingQueries = len( self.persistentDictList )
    processedBookkeepingQueries = 0
    self.log.notice("Total number of cached Bookkeeping queries to be processed: %d " % totalBookkeepingQueries ) 
    self.numDataRows = 0 # count the total number of records sent to the accounting
    self.dict1 = {}
    for bkkDict in self.persistentDictList:
      self.dict1[ 'ConfigName' ] = bkkDict[ 'ConfigName' ]
      self.dict1[ 'ConfigVersion' ] = bkkDict[ 'ConfigVersion' ]
      self.dict1[ 'EventTypeId' ] = bkkDict[ 'EventTypeId' ]
      self.dict1[ 'EventTypeDescription' ] = bkkDict[ 'EventTypeDescription' ]
      self.dict1[ 'dataTypeFlag' ] = bkkDict[ 'dataTypeFlag' ]
      self.dict1[ 'ConditionDescription' ] = bkkDict[ 'ConditionDescription' ]
      # loop on processing pass
      self.proPassTuples = []
      # this function stores in the self.proPassTuples variable a list of processing pass tuples.
      self.getProcessingPass( bkkDict, '/' )  
      for proPassTuple in sortList( self.proPassTuples ): # loop on processing passes
        rawDataFlag = False # True only for RAW data
        self.log.notice( "proPassTuple: %s" % proPassTuple )
        # RAW DATA. Warning: this is condition sufficient but NOT necessary to be raw data!
        if proPassTuple == '/Real Data':
          rawDataFlag = True
 
        self.dict1['ProcessingPass'] = proPassTuple
        self.dict1['rawDataFlag'] = rawDataFlag
        self.log.notice( "Call getStorageUsage with dict: %s " % self.dict1 )          
        res = self.getStorageUsage()
        if not res[ 'OK' ]:
          self.log.error( "ERROR!! self.getStorageUsage returned ERROR! %s" % res )
          continue
      processedBookkeepingQueries += 1
      progress = 1.0*processedBookkeepingQueries/totalBookkeepingQueries
      if not processedBookkeepingQueries % 100:
        self.log.notice("Bookkeeping queries processed: %d ( %f of total queries ) " % ( processedBookkeepingQueries, 
                                                                                         progress ) )


    self.log.notice( "Sending %d records to DataStore for the DataStorage type" % self.numDataRows )
    res = gDataStoreClient.commit()
    if not res[ 'OK' ]:
      self.log.notice( "ERROR: commit returned: %s " % res )
      return S_ERROR( res )
    self.log.notice("-------------------------------------------------------------------------------------\n")
    self.log.notice("------ End of cycle report ----------------------------------------------------------\n")
    self.log.notice("End of full BKK browsing: tot records sent to accounting for DataStorage: %d" % self.totalRecords )
    self.log.notice("Total Bookkeeping queries to process: %d Correctly processed: %d" %\
                      ( totalBookkeepingQueries, processedBookkeepingQueries ) )
    self.log.notice("Total productions returned by Bkk: %d Total productions in LFC: %d" %\
                      ( len(self.totalBkkProductions), len(self.prodsInLFC) ) )
    self.log.notice("Total runs returned by Bkk: %d Total runs in LFC: %d " % ( len(self.totalBkkRuns), 
                                                                                len(self.runsInLFC) ) )    
    dumpFile = open( os.path.join( self.workDirectory, "totalBkkProductions.txt"), "w")
    dumpFile.write("%s" % self.totalBkkProductions )
    dumpFile.close()
    dumpFile = open( os.path.join( self.workDirectory, "totalBkkRuns.txt"), "w")
    dumpFile.write("%s" % self.totalBkkRuns )
    dumpFile.close()
    self.log.notice( "Consistency LFC vs BKK: " )
    runsInLFCNotInBKK = []
    for run in self.runsInLFC.keys():
      if not self.runsInLFC[ run ][ 'InBkk' ]:
        runsInLFCNotInBKK.append( run )
    dumpFile = open( os.path.join( self.workDirectory, "runsInLFCNotInBkk.txt"), "w")
    dumpFile.write("%s" % runsInLFCNotInBKK )
    dumpFile.close()
    self.log.notice( "Runs in LFC not in BKK: %d " % len(runsInLFCNotInBKK) )
    # productions:
    prodsInLFCNotInBKK = []
    for prod in self.prodsInLFC.keys():
      if not self.prodsInLFC[ prod ]['InBkk']:
        prodsInLFCNotInBKK.append( prod )
    dumpFile = open( os.path.join( self.workDirectory, "prodsInLFCNotInBkk.txt"), "w")
    dumpFile.write("%s" % prodsInLFCNotInBKK )
    dumpFile.close()
    self.log.notice( "Productions in LFC not in BKK: %d " % len(prodsInLFCNotInBKK) )
    self.log.notice( "Consistency BKK vs LFC: " )
    self.log.notice( "Runs in BKK not in LFC: %d " % len(self.runsNotInLFC) )
    self.log.notice( "Productions in BKK not in LFC: %d " % len(self.prodsNotInLFC) )
    return S_OK()

  # ..................................................................................
  def generateBookkeepingDictionary( self ):
    """ Generate the list of Bkk dictionaries and store them on disk """

    bkDict = {}
    mainBkDict = {}
    count = 0
    res = self.bkClient.getAvailableConfigNames()
    if not res[ 'OK' ]:
      return S_ERROR( res )
    configNames = res['Value']['Records']
    # returns a list of list with one element
    for config in configNames: # loop on ConfigNames
      configName = config[ 0 ]
      bkDict[ 'ConfigName' ] = configName
      res = self.bkClient.getConfigVersions( bkDict )
      if not res[ 'OK' ]:
        return S_ERROR( res )
      # this is a list of lists , of length 1, lets' change it to a list of strings
      configVersions = res['Value']['Records']
      for version in configVersions:  # loop on configVersions
        configVersion = version[ 0 ]
        bkDict[ 'ConfigVersion' ] = configVersion
        self.log.notice( " Getting event types for %s " % bkDict )
        res = self.bkClient.getEventTypes ( bkDict )
        if not res[ 'OK' ]:
          return S_ERROR( res )
        eventTypes = res['Value']['Records']
        self.log.notice( "--> EventTypes: %s " % eventTypes )
        for ev in eventTypes:
          eventTypeId = ev[ 0 ]
          bkDict[ 'EventTypeId' ] = eventTypeId
          bkDict[ 'EventTypeDescription' ] = ev[ 1 ]
          self.log.notice( " Getting conditions for %s " % bkDict )
          res = self.bkClient.getConditions( bkDict )
          if not res[ 'OK' ]:
            return S_ERROR( res )
          # res['Value'][0] stores SIMULATION CONDITIONS: filled only for simulated data
          # res['Value'][1] stores DATA TAKING CONDITIONS: filled only for real data
          if ( res['Value'][1]['TotalRecords'] + res['Value'][0]['TotalRecords'] == 0 ):
            self.log.error( "ERROR: empty getConditions query for dict1= %s" % bkDict )
            return S_OK( res )
          dataTypeFlag = '' # either RealData or SimData
          if res['Value'][1]['TotalRecords'] > 0:
            self.log.notice( "Returned conditions are relative to REAL DATA for dict= %s" % bkDict )
            index = 1
            dataTypeFlag = 'RealData'
          elif res['Value'][0]['TotalRecords'] > 0:
            self.log.notice( "Returned conditions are relative to SIMULATED DATA for dict= %s" % bkDict )
            index = 0
            dataTypeFlag = 'SimData'
          records = res[ 'Value' ][ index ][ 'Records' ] # records is a list of Data taking conditions tuples
          for dataTakingTuple in records:
            dataTakingCond = dataTakingTuple[ 1 ]   # Description
            bkDict[ 'ConditionDescription' ] = dataTakingCond
            bkDict[ 'dataTypeFlag' ] = dataTypeFlag
 
            count += 1
            mainBkDict[ count ] = {}
            mainBkDict[ count ]['ConfigName'] = bkDict['ConfigName']
            mainBkDict[ count ]['ConfigVersion'] = bkDict['ConfigVersion']
            mainBkDict[ count ]['EventTypeId'] = bkDict['EventTypeId']
            mainBkDict[ count ]['EventTypeDescription'] = bkDict['EventTypeDescription']
            mainBkDict[ count ]['dataTypeFlag'] = bkDict['dataTypeFlag']
            mainBkDict[ count ]['ConditionDescription'] = bkDict['ConditionDescription']

    fp = open( self.bkkDictFile, "w")
    for value in mainBkDict.values():
      fp.write( "%s\n" % value )
    fp.close()
    self.log.notice( "Bookkeeping persistent dictionary written to file: %s " % self.bkkDictFile )

    return S_OK( mainBkDict )

  # ....................................................................................................

  def generateStorageUsageDictionary( self ):
    """ Generate a dump of the StorageUsageDB and keep it in memory in a dictionary """
    count = 0
    getSummaryCalls = 0
    start = time.time()
    self.log.notice( 'Starting MC', '/lhcb/MC' )
    res = self.storageUsage.getStorageDirectories( '/lhcb/MC' )
    if not res['OK']:
      return S_ERROR()
    mcDirs = {}
    mcSum = {}
    for directory in res['Value']:
      words = directory.split('/')
      prod = words[-3]
      typ = words[-4]
      directory = '/'.join( words[:-2] )
      if prod not in mcDirs:
        mcDirs[prod] = {}
      if typ not in mcDirs[prod]:
        mcDirs[prod][typ] = { 'Dirs': [] }
      if directory not in mcDirs[prod][typ]['Dirs']:
        mcDirs[prod][typ]['Dirs'].append( directory )

    self.log.notice( 'MC Productions:', len( mcDirs ) )
    for prod, pTypes in mcDirs.items():
      for typ, data in pTypes.items():
        for directory in data['Dirs']:
          res = self.storageUsage.getDirectorySummaryPerSE( directory )
          count += 1
          if res['OK']:
            for se in res['Value']:
              if not se in data:
                data[se] = { 'Files': 0, 'Size': 0 }
              data[se]['Files'] += res['Value'][se]['Files']
              data[se]['Size'] += res['Value'][se]['Size']
              if se not in mcSum:
                mcSum[se] = { 'Files': 0, 'Size': 0 }
              mcSum[se]['Files'] += res['Value'][se]['Files']
              mcSum[se]['Size'] += res['Value'][se]['Size']
          else:
            self.log.error("Cannot retrieve storage usage %s" % res['Message'] )
            continue
          res = self.storageUsage.getSummary( directory )
          getSummaryCalls += 1
          if not res[ 'OK' ]:
            self.log.error("Cannot retrieve LFN usage %s" % res['Message'])
            continue
          for retDir in res['Value'].keys():
            if retDir[-1] != '/':
              retDir = retDir+'/'
            if directory in retDir:
              data['LfnSize'] = res['Value'][ retDir ]['Size']
              data['LfnFiles'] = res['Value'][ retDir ]['Files']
 
    self.log.notice( 'MC Done' )
    dumpFile = open( os.path.join( self.workDirectory, "mcSum.txt" ) , "w")
    for se, value in mcSum.items():
      dumpFile.write("%s %s \n" %( se, value ) )
    dumpFile.close()
    #self.log.notice( pprint.pformat( mcSum ) )
    dumpFile = open( os.path.join( self.workDirectory, "mcDirs.txt" ), "w")
    for prod in mcDirs:
      dumpFile.write("%s\n" % prod)
      for ft in mcDirs[ prod ]:
        dumpFile.write("-- %s\n" % ft )
        for key, item  in mcDirs[ prod ][ ft ].items():
          dumpFile.write("---- %s %s \n" % ( key, item ) )
    dumpFile.close()      
    #self.log.info( pprint.pformat( mcDirs ) )
  
    self.log.notice( 'Starting Data', '/lhcb/LHCb,/lhcb/data,/lhcb/validation' )
  
    dataDirs = {}
    dataSum = {}
    rawDirs = {}
    rawSum = {}
    # get list of reconstructed data productions: 
    for path in ['/lhcb/LHCb', '/lhcb/data','/lhcb/validation']:
      res = self.storageUsage.getStorageDirectories( path )
      for directory in res['Value']:
        if "RAW" in directory:
          continue          
        words = directory.split('/')
        prod = words[-3]
        typ = words[-4]
        directory = '/'.join( words[:-2])
        myDirs = dataDirs
        if prod not in myDirs:
          myDirs[prod] = {}
        if typ  not in myDirs[prod]:
          myDirs[prod][typ] = { 'Dirs': [] }
        if directory not in myDirs[prod][typ]['Dirs']:
          myDirs[prod][typ]['Dirs'].append( directory )
    
    self.log.notice( 'Data Productions:', len( dataDirs ) )

    # get list of raw data runs: 
    for path in ['/lhcb/data']:
      res = self.storageUsage.getStorageDirectories( path )
      for directory in res['Value']:
        words = directory.split('/')
        if words[4] != 'RAW': # not raw data 
          continue      
        prod = words[-2]
        typ = 'RAW'
        #d = '/'.join( s[:-2])
        if len(words) == 10:
          stream = words[5]
            #print 'ok, usual format for raw data, stream is ', stream
          #if stream not in possibleStreams:
          #possibleStreams.append( stream )
        else:
          self.log.warn("unusual directory format (probably data previous to Jan 2009): %s " % directory )
          continue
        myDirs = rawDirs
        if prod not in myDirs:
          myDirs[prod] = {}
        if stream not in myDirs[prod]:
          myDirs[prod][stream] = { 'Dirs': [] }
        if directory not in myDirs[prod][stream]['Dirs']:
          myDirs[prod][stream]['Dirs'].append( directory )
    
    self.log.notice( 'Raw data runs:', len( rawDirs ) )

    # get storage usage for reconstructed data directories:
    for prod, pTypes in dataDirs.items():
      for typ, data in pTypes.items():
        for directory in data['Dirs']:
          res = self.storageUsage.getDirectorySummaryPerSE( directory )
          count += 1
          if res['OK']:
            for se in res['Value']:
              if se not in data:
                data[se] = { 'Files': 0, 'Size':0 }
              data[se]['Files'] += res['Value'][se]['Files']
              data[se]['Size'] += res['Value'][se]['Size']
              if se not in dataSum:
                dataSum[se] = { 'Files': 0, 'Size': 0 }
              dataSum[se]['Files'] += res['Value'][se]['Files']
              dataSum[se]['Size'] += res['Value'][se]['Size']
          else:
            self.log.error("Cannot retrieve storage usage: %s "  % res['Message'] )
            continue
          res = self.storageUsage.getSummary( directory )
          getSummaryCalls += 1
          if not res[ 'OK' ]:
            self.log.error("Cannot retrieve LFN usage %s" % res['Message'])
            continue
          for retDir in res['Value'].keys():
            if retDir[-1] != '/':
              retDir = retDir+'/'
            if directory in retDir:
              data['LfnSize'] = res['Value'][ retDir ]['Size']
              data['LfnFiles'] = res['Value'][ retDir ]['Files']
 
    
    self.log.notice( 'Data Productions Done' )
    dumpFile = open( os.path.join( self.workDirectory, "dataSum.txt" ), "w")
    for se, item in dataSum.items():
      dumpFile.write("%s %s \n" % ( se,  item ) )
    dumpFile.close()
    # self.log.notice( pprint.pformat( dataSum ) )
    dumpFile = open( os.path.join( self.workDirectory, "dataDirs.txt" ), "w")
    for prod in dataDirs:
      dumpFile.write( "%s\n" % prod )
      for ft in dataDirs[ prod ]:
        dumpFile.write("-- %s\n" % ft )
        for key, value in dataDirs[ prod ][ ft ].items():
          dumpFile.write("---- %s %s \n" % ( key, value ) ) 
    dumpFile.close()      

    # get storage usage for runs directories:
    self.log.notice( 'Data Runs:', len( rawDirs ) )
    
    for prod, pTypes in rawDirs.items():
      for stream, data in pTypes.items():
        for directory in data['Dirs']:
          res = self.storageUsage.getDirectorySummaryPerSE( directory )
          count += 1
          if res['OK']:
            for se in res['Value']:
              if not se in data:
                data[se] = { 'Files': 0, 'Size': 0 }
              data[se]['Files'] += res['Value'][se]['Files']
              data[se]['Size'] += res['Value'][se]['Size']
              if not se in rawSum:
                rawSum[se] = { 'Files': 0, 'Size': 0 }
              rawSum[se]['Files'] += res['Value'][se]['Files']
              rawSum[se]['Size'] += res['Value'][se]['Size']
          else:
            self.log.error("Cannot retrieve storage usage: %s " %res['Message'] )
            continue

          res = self.storageUsage.getSummary( directory )
          getSummaryCalls += 1
          if not res[ 'OK' ]:
            self.log.error("Cannot retrieve LFN usage %s" % res['Message'])
            continue
          for retDir in res['Value'].keys():
            if retDir[-1] != '/':
              retDir = retDir+'/'
            if directory in retDir:
              data['LfnSize'] = res['Value'][ retDir ]['Size']
              data['LfnFiles'] = res['Value'][ retDir ]['Files']
 

    self.log.notice( 'Data Runs Done' )
    dumpFile = open( os.path.join( self.workDirectory, "rawSum.txt" ), "w")
    for se, value in rawSum.items():
      dumpFile.write( "%s %s \n" %( se, value ) )
    dumpFile.close()
    dumpFile = open( os.path.join( self.workDirectory, "rawDirs.txt" ), "w")
    for prod in rawDirs:
      dumpFile.write( "%s\n" % prod )
      for stream in rawDirs[ prod ]:
        dumpFile.write( "-- %s\n" % stream )
        for key, value in rawDirs[ prod ][ stream ].items():
          dumpFile.write( "---- %s %s \n" %( key, value ) )
    dumpFile.close()      

    self.log.notice( 'Data Done' )
    
    self.log.notice( 'Queried directories for storage usage:', count )
    self.log.notice( 'Queried directories for LFN usage: ' , getSummaryCalls )
    
    end = time.time()
    duration = end - start
    self.log.info("Total time to create StorageUsageDB dump: %d s" % duration )
    # export these dictionaries:
    self.mcDirs = mcDirs
    self.mcSum = mcSum
    self.dataDirs = dataDirs
    self.dataSum = dataSum
    self.rawDirs = rawDirs
    self.rawSum = rawSum
    # store all prod/run in the LFC to check their existance in the Bkk
    self.prodsInLFC = {}
    for prod in self.mcDirs:
      self.prodsInLFC[ prod ] = {}
      self.prodsInLFC[ prod ]['InBkk'] = False
    for prod in self.dataDirs:
      self.prodsInLFC[ prod ] = {}
      self.prodsInLFC[ prod ]['InBkk'] = False
    self.runsInLFC = {}
    for run in self.rawDirs.keys():
      self.runsInLFC[ run ] = {}
      self.runsInLFC[ run ][ 'InBkk' ] = False
       
    self.prodsNotInLFC = []
    self.runsNotInLFC = []

    return S_OK()

#....................................................................................................
  def getStorageUsage( self ):
    """ For a given dictionary defining: ConfigName, ConfigVersion, EventType, Conditions, ProcessingPass 
        gets the available productions/runs and file types and fill the accounting records 
    """

    # mapping from Bokkeeping stream to LFC path:
    # possible LFC sub-directories: ['EXPRESS', 'FULL', 'LUMI', 'CALIB', 'ERRORS', 'BEAMGAS', 'NOBIAS']
    mapBkk2LFC = {
       91000000 : 'EXPRESS',
       91000001 : 'EXPRESS',
       90000000 : 'FULL',
       90000001 : 'FULL', # 'stream?'
       93000000 : 'LUMI',
       93000001 : 'LUMI',
       95000000 : 'CALIB',
       95000001 : 'CALIB',
       92000000 : 'ERRORS',
       92000001 : 'ERRORS',
       97000000 : 'BEAMGAS',
       97000001 : 'BEAMGAS',
       96000000 : 'NOBIAS',
       96000001 : 'NOBIAS',
       98000000 : 'HLTONE',
       98000001 : 'HLTONE'
    }

    now = Time.dateTime()
    eventTypeDesc = self.dict1['EventTypeDescription' ] # needed to send it to accounting (instead of numeric ID)
    eventTypeDesc = str( self.dict1['EventTypeId'] ) + '-' + eventTypeDesc
    self.log.debug( "eventTypeDescription %s" % eventTypeDesc )
    # dataTypeFlag = '' # either RealData or SimData
    # LFCBasePath = {'RealData': '/lhcb/', 'SimData': '/lhcb/MC/'}
    dict3 = {}
    dict3[ 'ConfigName' ] = self.dict1[ 'ConfigName' ]
    dict3[ 'ConfigVersion' ] = self.dict1[ 'ConfigVersion' ]
    dict3[ 'EventTypeId' ] = self.dict1[ 'EventTypeId']
    dict3[ 'EventTypeDescription' ] = self.dict1[ 'EventTypeDescription' ]
    dict3[ 'ProcessingPass' ] = self.dict1[ 'ProcessingPass' ]
    dict3[ 'ConditionDescription'] = self.dict1[ 'ConditionDescription' ]
    dict3[ 'dataTypeFlag' ] = self.dict1[ 'dataTypeFlag' ] 
    dict3[ 'rawDataFlag'] = self.dict1[ 'rawDataFlag' ] # True for RAW data only
    # get list of productions for every tuple (ConfigName, ConfigVersion, ProcessingPass, ConditionDescription)
    self.log.notice( " Get productions/runs for dict3 : %s " % dict3 )
    res = self.bkClient.getProductions( dict3 )
    if not res[ 'OK' ]:
      self.log.error( "could not get productions/runs for dict %s Error: %s" % ( dict3, 
                                                                                 res['Message'] ) )
      return S_ERROR( res )
    productions = sortList( [x[0] for x in res[ 'Value' ][ 'Records' ]] )
    if productions == []:
      self.log.notice( "WARNING: EMPTY QUERY ! no Production available for dict3= %s" % dict3 )
      return S_OK()
    self.log.notice( "Got the productions/runs list: %s" % productions )
    for prodID in productions:
      if int( prodID ) < 0:
        if not dict3[ 'rawDataFlag']:
          self.log.warn("rawDataFlag was set to False. Now set to True")
          dict3[ 'rawDataFlag'] = True
      else:
        self.totalBkkProductions.append( prodID )

    #  MC and reconstructed data
    if not dict3[ 'rawDataFlag']:
      for prodID in productions:
        dict3[ 'Production' ] = prodID
        res = self.bkClient.getFileTypes( dict3 )
        if not res['OK']:
          self.log.notice( "ERROR getting file types for prod %s, Error: %s" % ( prodID, res['Message'] ) )
          continue
        prodFileTypes = reduce( lambda x, y: x+y, res['Value']['Records'] )
        prodFileTypes = [ x for x in prodFileTypes if not re.search( "HIST", x ) ]
        # add the HIST, SETC, DST file type to the list (to be checked why)
        prodFileTypes.append( 'HIST' )
        if 'DST' not in prodFileTypes:
          prodFileTypes.append( 'DST' )
        if 'SETC' not in prodFileTypes:
          prodFileTypes.append( 'SETC' )
        self.log.notice( "For prod %d list of file types: %s" % ( prodID, prodFileTypes ) )
     
        # manipulates if necessary the prodId to match the format of the StorageUsage dictionary (string of 8 chars) 
        prodID = str(prodID)
        if len(prodID)< 8:
          diff = 8 - len(prodID)
          prodID = diff*'0' + prodID
        # mark the production as existing in Bkk
        if prodID in self.prodsInLFC.keys(): 
          self.prodsInLFC[ prodID ][ 'InBkk'] = True
        else:
          self.prodsNotInLFC.append( prodID )
          self.log.notice("Production %s is in Bkk but NOT in LFC! " % prodID )
        # select the dictionary: either reconstructed data or MC:
        if dict3[ 'dataTypeFlag' ] == 'RealData':
          myDirs =  self.dataDirs
        else:
          myDirs = self.mcDirs 

        for fType in prodFileTypes:
          if prodID not in myDirs.keys():
            self.log.warn("The storage usage for production %s was not stored in dictionary! " \
                            "(this should never happen! should have been checked before!)" % (prodID ) )
            continue
          if fType not in myDirs[ prodID ].keys():
            self.log.warn("The storage usage for production %s and fileType %s was not stored in dictionary!" %\
                            ( prodID, fType ) )
            continue
          
          for seName in sortList( myDirs[ prodID ][ fType ].keys() ):
            if seName in ['Dirs', 'LfnFiles','LfnSize']:
              continue
            try:
              physicalFiles = myDirs[ prodID ][ fType ][ seName ][ 'Files' ]
              physicalSize = myDirs[ prodID ][ fType ][ seName ][ 'Size' ]
            except KeyError:
              self.log.warn("The storage usage for production %s fileType %s SE %s was not stored in dictionary!" %\
                              (prodID, fType, seName ) )
              continue
            
            logicalSize = 0
            logicalFiles = 0
            try:
              logicalSize = myDirs[ prodID ][ fType ]['LfnSize']
              logicalFiles = myDirs[ prodID ][ fType ]['LfnFiles']
            except KeyError:
              self.log.warn("LFN size/files not stored for prod,fileType = %s, %s " %(prodID, fType ))
            # create a record to be sent to the accounting:
            self.log.notice( ">>>>>>>>Send DataStorage record to accounting for fields: " \
                               "DataType: %s Activity: %s FileType: %s Production: %s ProcessingPass: %s " \
                               "Conditions: %s EventType: %s StorageElement: %s --> physFiles: %d  " \
                               "physSize: %d lfnFiles: %d lfnSize: %d " %\
                               ( dict3['ConfigName'] , dict3['ConfigVersion'], fType, prodID, dict3['ProcessingPass'], 
                                 dict3['ConditionDescription'], dict3['EventTypeDescription'], seName, physicalFiles, 
                                 physicalSize, logicalFiles, logicalSize ) )
            # call function to send the record to the accounting
         
            dataRecord = DataStorage()
            dataRecord.setStartTime( now )
            dataRecord.setEndTime( now )
            dataRecord.setValueByKey( "DataType", dict3['ConfigName'] )
            dataRecord.setValueByKey( "Activity", dict3['ConfigVersion'] )
            dataRecord.setValueByKey( "FileType", fType )
            dataRecord.setValueByKey( "Production", prodID )
            dataRecord.setValueByKey( "ProcessingPass", dict3['ProcessingPass'] )
            dataRecord.setValueByKey( "Conditions", dict3['ConditionDescription'] )
            dataRecord.setValueByKey( "EventType", dict3['EventTypeDescription'] )
            dataRecord.setValueByKey( "StorageElement", seName )
            dataRecord.setValueByKey( "PhysicalSize", physicalSize )
            dataRecord.setValueByKey( "PhysicalFiles", physicalFiles )
            dataRecord.setValueByKey( "LogicalSize", logicalSize )
            dataRecord.setValueByKey( "LogicalFiles", logicalFiles )
            res = gDataStoreClient.addRegister( dataRecord )
            if not res[ 'OK']:
              self.log.notice( "ERROR: In getStorageUsage addRegister returned: %s" % res )
            self.numDataRows += 1
            self.totalRecords += 1
            self.recordsToCommit += 1


    else: # raw data
      myDirs = self.rawDirs
      for prodID in productions:
        # for the accounting, keep the negative number not to confuse with productions.
        runNoForAccounting = str( prodID )
        self.totalBkkRuns.append( runNoForAccounting )
        # Bkk returns a negative number for raw data runs, change it to positive 
        # (necessary to match the key in the dictionary where storage usage is stored)
        prodID = str(-1*prodID)

        if prodID not in myDirs.keys():
          self.log.warn("Run %s was not in the storage usage dictionary! " % prodID )
          self.runsNotInLFC.append( prodID )
          continue
        self.runsInLFC[ prodID ][ 'InBkk' ] = True
        fType = 'RAW'
        try:
          streamInLFC = mapBkk2LFC[ dict3[ 'EventTypeId' ] ]
        except KeyError:
          self.log.warn("This event type is not mapped to any LFC directory: %s " % dict3[ 'EventTypeId' ] )
          continue

        if streamInLFC not in myDirs[ prodID ].keys():
          self.log.warn("The stream %s is not in the storage dictionary for run %s " %(dict3[ 'EventTypeId' ], prodID))
          continue
        for seName in myDirs[ prodID ][ streamInLFC ].keys():
          if seName in ['Dirs', 'LfnFiles','LfnSize']:
            continue
          try:
            physicalFiles = myDirs[ prodID ][ streamInLFC ][ seName ][ 'Files' ]
            physicalSize = myDirs[ prodID ][ streamInLFC ][ seName ][ 'Size' ]
          except KeyError:
            self.log.warn("The storage usage for production %s stream %s SE %s was not stored in dictionary!" %\
                            (prodID, streamInLFC, seName ) )
            continue
          
          logicalSize = 0
          logicalFiles = 0
          try:
            logicalSize = myDirs[ prodID ][ streamInLFC ]['LfnSize']
            logicalFiles = myDirs[ prodID ][ streamInLFC ]['LfnFiles']
          except KeyError:
            self.log.warn("LFN size/files not stored for run,stream = %s, %s " %(prodID, streamInLFC ))
 
          #res = __fillAccountingRecord( )        
          self.log.notice( ">>>>>>>>Send DataStorage record to accounting for fields: " \
                             "DataType: %s Activity: %s FileType: %s Run: %s ProcessingPass: %s Conditions: %s " \
                             "EventType: %s StorageElement: %s --> physFiles: %d  physSize: %d " \
                             "lfnFiles: %d lfnSize: %d" %\
                             ( dict3['ConfigName'] , dict3['ConfigVersion'], fType, runNoForAccounting, 
                               dict3['ProcessingPass'], dict3['ConditionDescription'], dict3['EventTypeDescription'], 
                               seName, physicalFiles, physicalSize, logicalFiles, logicalSize ) )
          dataRecord = DataStorage()
          dataRecord.setStartTime( now )
          dataRecord.setEndTime( now )
          dataRecord.setValueByKey( "DataType", dict3['ConfigName'] )
          dataRecord.setValueByKey( "Activity", dict3['ConfigVersion'] )
          dataRecord.setValueByKey( "FileType", fType )
          #dataRecord.setValueByKey( "Production", prodID )
          dataRecord.setValueByKey( "Production", runNoForAccounting )
          dataRecord.setValueByKey( "ProcessingPass", dict3['ProcessingPass'] )
          dataRecord.setValueByKey( "Conditions", dict3['ConditionDescription'] )
          dataRecord.setValueByKey( "EventType", dict3['EventTypeDescription'] )
          dataRecord.setValueByKey( "StorageElement", seName )
          dataRecord.setValueByKey( "PhysicalSize", physicalSize )
          dataRecord.setValueByKey( "PhysicalFiles", physicalFiles )
          dataRecord.setValueByKey( "LogicalSize", logicalSize )
          dataRecord.setValueByKey( "LogicalFiles", logicalFiles )
          res = gDataStoreClient.addRegister( dataRecord )
          if not res[ 'OK']:
            self.log.notice( "ERROR: In getStorageUsage addRegister returned: %s" % res )
          self.numDataRows += 1
          self.totalRecords += 1
          self.recordsToCommit += 1

    if self.recordsToCommit > self.limitForCommit:
      res = gDataStoreClient.commit()
      if not res[ 'OK' ]:
        self.log.error( "Accounting ERROR: commit returned %s" % res )
      else:
        self.log.notice( "%d records committed " % self.recordsToCommit )
        self.recordsToCommit = 0
      self.log.notice( "In getStorageUsage commit returned: %s" % res )
 
    return S_OK()

  def getProcessingPass( self, thisDict, path ):
    """ retrieves all processing pass recursively from the initial path '/', for the given bkk dictionary """

    dict2 = {}
    #dict2[ 'ConfigName'] = self.dict1[ 'ConfigName' ]
    #dict2[ 'ConfigVersion'] = self.dict1[ 'ConfigVersion' ]
    #dict2[ 'EventTypeId' ] = self.dict1[ 'EventTypeId' ]
    #dict2[ 'ConditionDescription'] = dataTakingCond
    dict2[ 'ConfigName'] = thisDict[ 'ConfigName' ]
    dict2[ 'ConfigVersion'] = thisDict[ 'ConfigVersion' ]
    dict2[ 'EventTypeId' ] = thisDict[ 'EventTypeId' ]
    dict2[ 'ConditionDescription'] = thisDict['ConditionDescription']

    res = self.bkClient.getProcessingPass( dict2, path )
    if not res[ 'OK' ]:
      self.log.error( "getProcessingPass failed with error: %s" % res["Message"] )
      return S_ERROR( res )
    recs = res['Value'][0]
    if recs['TotalRecords'] != 0:
      for rec in recs['Records']:
        #srec = "/%s" % rec[0]
        if path == "/":
          newpath = "/%s" % ( rec[0] )
        else:
          newpath = "%s/%s" % ( path, rec[0] )
        self.proPassTuples += [newpath]
        self.getProcessingPass( dict2, newpath )

    return S_OK()



