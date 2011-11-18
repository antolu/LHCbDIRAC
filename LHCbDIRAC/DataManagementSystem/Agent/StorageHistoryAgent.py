########################################################################
# $HeadURL$
# File :   StorageSummaryAgent.py
########################################################################

"""   The Storage History Agent will create a summary of the
      storage usage DB grouped by processing pass or other
      interesting parameters.

      Initially this will dump the information to a file but eventually
      can be inserted in a new DB table and made visible via the web portal.
"""

__RCSID__ = "$Id$"

from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.Core.Utilities import Time
from DIRAC.Core.Utilities.List                           import sortList, intListToString
from LHCbDIRAC.AccountingSystem.Client.Types.UserStorage import UserStorage
from LHCbDIRAC.AccountingSystem.Client.Types.Storage import Storage
from LHCbDIRAC.AccountingSystem.Client.Types.DataStorage import DataStorage
from DIRAC.AccountingSystem.Client.DataStoreClient import gDataStoreClient
from LHCbDIRAC.NewBookkeepingSystem.Client.BookkeepingClient            import BookkeepingClient
import os, sys, re
import pprint, time

from DIRAC  import S_OK, S_ERROR, gLogger



byteToGB = 1.0e9
QUICKLOOP = False # this is a flag for debugging purposes: if it is True the browsing of the BKK is speeded up the loop

class StorageHistoryAgent( AgentModule ):

  def initialize( self ):
    """Sets defaults
    """
    self.am_setOption( 'PollingTime', 43200 )
    if self.am_getOption( 'DirectDB', False ):
      from LHCbDIRAC.DataManagementSystem.DB.StorageUsageDB import StorageUsageDB
      self.__stDB = StorageUsageDB()
    else:
      from DIRAC.Core.DISET.RPCClient import RPCClient
      self.__stDB = RPCClient( 'DataManagement/StorageUsage' )
    self.bkClient = BookkeepingClient()

    self.__workDirectory =  self.am_getOption( "WorkDirectory" )
    if not os.path.isdir( self.__workDirectory ):
      os.makedirs( self.__workDirectory )
    self.log.info( "Working directory is %s" % self.__workDirectory )

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

    ###
    # Accounting records relative to the general Storage type:
    ###
    ftb = 1.0e12

    # get info from the DB about the LOGICAL STORAGE USAGE (from the su_Directory table):
    result = self.__stDB.getSummary( '/lhcb/' )
    if not result[ 'OK' ]:
      return result
    logicalUsage = result['Value']
    topDirLogicalUsage = {} # build the list of first level directories
    for row in logicalUsage.keys():
      #self.log.debug("row is %s " %row)
      #d, size, files = row
      d = row
      files = logicalUsage[ d ][ 'Files' ]
      size = logicalUsage[ d ][ 'Size' ]
      splitDir = d.split( "/" )
      if len( splitDir ) > 3: # skip the root directory "/lhcb/"
        firstLevelDir = '/' + splitDir[1] + '/' + splitDir[2] + '/'
        if firstLevelDir not in topDirLogicalUsage.keys():
          topDirLogicalUsage[ firstLevelDir ] = {}
          topDirLogicalUsage[ firstLevelDir ][ 'Files' ] = 0
          topDirLogicalUsage[ firstLevelDir ][ 'Size' ] = 0
        topDirLogicalUsage[ firstLevelDir ][ 'Files' ] += files
        topDirLogicalUsage[ firstLevelDir ][ 'Size' ] += size
    self.log.notice( "Summary on logical usage of top directories: " )
    for d in topDirLogicalUsage.keys():
      self.log.notice( "dir: %s size: %.4f TB  files: %d" % ( d, topDirLogicalUsage[d]['Size'] / ftb, topDirLogicalUsage[d]['Files'] ) )

    # loop on top level directories (/lhcb/data, /lhcb/user/, /lhcb/MC/, etc..) to get the summary in terms of PHYSICAL usage grouped by SE:
    SEData = {}
    for directory in topDirLogicalUsage.keys():
      result = self.__stDB.getDirectorySummaryPerSE( directory ) # retrieve the PHYSICAL usage
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
        self.log.error( "ERROR: directory %s is in the summary per SE, but it is not in the logical files summary!" % directory )
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
        self.log.debug( "Directory: %s SE: %s  physical size: %.4f TB (%d files)" % ( directory, se, physicalSize / ftb, physicalFiles ) )

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
    # 1- queries the BKK client to get the set of data taking conditions relative to: eventType=93000000,ConfigVersion': 'Collision10', 'ConfigName': 'LHCb'.
    # 2- loops on the data taking conditions and retrieve for each of them the set of all existing processing passes.
    #    Loops on all processing passes:
    #    2.1 - for every ( data taking condition, proc. pass) queries the BKK client to get the list of productions/runs
    #    2.2 - for production, gets the list of file types and adds the HIST, DST and SETC (not clear..) and fill and accounting record per SE.

    reportForDataManager = '/tmp/reportForDataManager.txt'
    self.report = open( reportForDataManager, "w" )
    self.sesOfInterest = ''

    # counter for DataStorage records, commit to the accounting in bunches of self.limitForCommit records
    self.totalRecords = 0
    self.recordsToCommit = 0
    self.limitForCommit = 200
  
    self.log.notice(" Call the function to create the StorageUsageDB dump.." )
    res = self.generateStorageUsageDictionary()
    if not res[ 'OK' ]:
      self.log.error("ERROR generating the StorageUsageDB dump")
      return S_ERROR()
           
    self.eventTypeDescription = {}
    self.persistentDictList = [] # persistency Bookkeeping dictionary    


    self.log.notice(" Try to read persistentDict from disk..")
    self.bkkDictFile = os.path.join( self.__workDirectory, "bkkPersistentDict.txt" )
    if not os.path.exists(self.bkkDictFile):
      self.log.notice("Could not read persistent Bkk dictionary from file => regenerate the dictionary")
      res = self.generateBookkeepingDictionary()
      if not res[ 'OK' ]:
        self.log.error("ERROR! %s " % res )
        return S_ERROR( res )
    
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
      #self.log.notice("Bkk dictionary : %s " % bkkDict)
      self.dict1[ 'ConfigName' ] = bkkDict[ 'ConfigName' ]
      self.dict1[ 'ConfigVersion' ] = bkkDict[ 'ConfigVersion' ]
      self.dict1[ 'EventTypeId' ] = bkkDict[ 'EventTypeId' ]
      self.dict1[ 'EventTypeDescription' ] = bkkDict[ 'EventTypeDescription' ]
      self.dict1[ 'dataTypeFlag' ] = bkkDict[ 'dataTypeFlag' ]
      self.dict1[ 'ConditionDescription' ] = bkkDict[ 'ConditionDescription' ]
      # loop on processing pass
      self.proPassTuples = []
      self.getProcessingPass( bkkDict, '/' )  # this function stores in the self.proPassTuples variable a list of processing pass tuples.
      for proPassTuple in sortList( self.proPassTuples ): # loop on processing passes
        rawDataFlag = False # True only for RAW data
        self.log.notice( "proPassTuple: %s" % proPassTuple )
        if proPassTuple == '/Real Data': #RAW DATA. Warning: this is condition sufficient but NOT necessary to be raw data!
          rawDataFlag = True
 
        self.dict1['ProcessingPass'] = proPassTuple
        self.dict1['rawDataFlag'] = rawDataFlag
        self.log.notice( "Call getStorageUsage with dict: %s " % self.dict1 )          
        #res = self.getStorageUsage()
        res = self.getStorageUsage_fromDBDump()
        if not res[ 'OK' ]:
          self.log.error( "ERROR!! self.getStorageUsage returned ERROR! %s" % res )
          #return S_ERROR( res )
          continue
      processedBookkeepingQueries += 1
      progress = 1.0*processedBookkeepingQueries/totalBookkeepingQueries
      if not processedBookkeepingQueries%10:
        self.log.notice("Bookkeeping queries processed so far: %d ( %f of total queries ) " % ( processedBookkeepingQueries, progress ) )


    self.log.notice( "Sending %d records to DataStore for the DataStorage type" % self.numDataRows )
    res = gDataStoreClient.commit()
    if not res[ 'OK' ]:
      self.log.notice( "ERROR: commit returned: %s " % res )
      return S_ERROR( res )
    self.log.notice("-------------------------------------------------------------------------------------\n")
    self.log.notice("------ End of cycle report ----------------------------------------------------------\n")
    self.log.notice( "End of full BKK browsing: total records sent to accounting for DataStorage:  %d " % self.totalRecords )
    self.log.notice( "Total Bookkeeping queries to process: %d and correctly processed: %d " %(totalBookkeepingQueries, processedBookkeepingQueries) )
    return S_OK()

#..................................................................................
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
    for c in configNames: # loop on ConfigNames
      configName = c[ 0 ]
      bkDict[ 'ConfigName' ] = configName
      res = self.bkClient.getConfigVersions( bkDict )
      if not res[ 'OK' ]:
        return S_ERROR( res )
      configVersions = res['Value']['Records']# this is a list of lists , of length 1, lets' change it to a list of strings
      for v in configVersions:  # loop on configVersions
        configVersion = v[ 0 ]
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
    for k in mainBkDict.keys():
      fp.write( "%s\n" % mainBkDict[ k ] )
    fp.close()
    self.log.notice( "Bookkeeping persistent dictionary written to file: %s " % self.bkkDictFile )

    return S_OK( mainBkDict )

#....................................................................................................



  def generateStorageUsageDictionary( self ):
    """Generate a dump of the StorageUsageDB and keep it in memory in a dictionary
    """

    c = 0
    start = time.time()
    gLogger.notice( 'Starting MC', '/lhcb/MC' )
    res = self.__stDB.getStorageDirectories( '/lhcb/MC' )
    if not res['OK']:
      return S_ERROR()

    mcDirs = {}
    mcSum = {}
    for d in res['Value']:
      s = d.split('/')
      prod = s[-3]
      typ = s[-4]
      d = '/'.join( s[:-2])
      if prod not in mcDirs:
        mcDirs[prod] = {}
      if typ  not in mcDirs[prod]:
        mcDirs[prod][typ] = {'Dirs':[]}
      if d not in mcDirs[prod][typ]['Dirs']:
        mcDirs[prod][typ]['Dirs'].append( d )

    gLogger.notice( 'MC Productions:', len( mcDirs ) )

    for prod,t in mcDirs.items():
      for typ,data in t.items():
        for d in data['Dirs']:
          res = self.__stDB.getDirectorySummaryPerSE( d )
          c += 1
          if res['OK']:
            for se in res['Value'].keys():
              if not se in data:
                data[se] = {'Files':0,'Size':0}
              data[se]['Files'] += res['Value'][se]['Files']
              data[se]['Size'] += res['Value'][se]['Size']
              if se not in mcSum:
                mcSum[se] = {'Files':0,'Size':0}
              mcSum[se]['Files'] += res['Value'][se]['Files']
              mcSum[se]['Size'] += res['Value'][se]['Size']


    gLogger.notice( 'MC Done' )
    fileName = os.path.join( self.__workDirectory, "mcSum.txt" )
    f = open(fileName, "w")
    for se in mcSum.keys():
      f.write("%s %s \n" %(se, mcSum[ se ]))
    f.close()
    #gLogger.notice( pprint.pformat( mcSum ) )
    fileName = os.path.join( self.__workDirectory, "mcDirs.txt" )
    f = open(fileName, "w")
    for prod in mcDirs.keys():
      f.write("%s\n" % prod)
      for ft in mcDirs[ prod ].keys():
        f.write("-- %s\n" % ft )
        for k in mcDirs[ prod ][ ft ].keys():
          f.write("---- %s %s \n" %(k, mcDirs[ prod ][ ft ][ k ]) )
    f.close()      
    #gLogger.info( pprint.pformat( mcDirs ) )
  

    gLogger.notice( 'Starting Data', '/lhcb/LHCb,/lhcb/data,/lhcb/validation' )
  
    dataDirs = {}
    dataSum = {}
    rawDirs = {}
    rawSum = {}
    # get list of reconstructed data productions: 
    for path in ['/lhcb/LHCb', '/lhcb/data','/lhcb/validation']:
      res = self.__stDB.getStorageDirectories( path )
      for d in res['Value']:
        s = d.split('/')
        if d.find('RAW') > -1:
          continue
        prod = s[-3]
        typ = s[-4]
        d = '/'.join( s[:-2])
        myDirs = dataDirs
        if prod not in myDirs:
          myDirs[prod] = {}
        if typ  not in myDirs[prod]:
          myDirs[prod][typ] = {'Dirs':[]}
        if d not in myDirs[prod][typ]['Dirs']:
          myDirs[prod][typ]['Dirs'].append( d )
    
    gLogger.notice( 'Data Productions:', len( dataDirs ) )

    # get list of raw data runs: 
    for path in ['/lhcb/data']:
      res = self.__stDB.getStorageDirectories( path )
      for d in res['Value']:
        s = d.split('/')
        if s[4] != 'RAW': # not raw data 
          continue      
        prod = s[-2]
        typ = 'RAW'
        #d = '/'.join( s[:-2])
        if len(s) == 10:
          stream = s[5]
            #print 'ok, usual format for raw data, stream is ', stream
          #if stream not in possibleStreams:
          #possibleStreams.append( stream )
        else:
          gLogger.warn("unusual directory format (probably data previous to Jan 2009): %s " % d )
          continue
        myDirs = rawDirs
        if prod not in myDirs:
          myDirs[prod] = {}
        if stream not in myDirs[prod]:
          myDirs[prod][stream] = {'Dirs':[]}
        if d not in myDirs[prod][stream]['Dirs']:
          myDirs[prod][stream]['Dirs'].append( d )
    
    gLogger.notice( 'Raw data runs:', len( rawDirs ) )


    # get storage usage for reconstructed data directories:
    for prod,t in dataDirs.items():
      for typ,data in t.items():
        for d in data['Dirs']:
          res = self.__stDB.getDirectorySummaryPerSE( d )
          c += 1
          if res['OK']:
            for se in res['Value'].keys():
              if not se in data:
                data[se] = {'Files':0,'Size':0}
              data[se]['Files'] += res['Value'][se]['Files']
              data[se]['Size'] += res['Value'][se]['Size']
              if not se in dataSum:
                dataSum[se] = {'Files':0,'Size':0}
              dataSum[se]['Files'] += res['Value'][se]['Files']
              dataSum[se]['Size'] += res['Value'][se]['Size']
    
    gLogger.notice( 'Data Productions Done' )
    fileName = os.path.join( self.__workDirectory, "dataSum.txt" )
    f = open(fileName, "w")
    for se in dataSum.keys():
      f.write("%s %s \n" %(se, dataSum[ se ]))
    f.close()
    #gLogger.notice( pprint.pformat( dataSum ) )
    fileName = os.path.join( self.__workDirectory, "dataDirs.txt" )
    f = open(fileName, "w")
    for prod in dataDirs.keys():
      f.write("%s\n" % prod)
      for ft in dataDirs[ prod ].keys():
        f.write("-- %s\n" % ft )
        for k in dataDirs[ prod ][ ft ].keys():
          f.write("---- %s %s \n" %(k, dataDirs[ prod ][ ft ][ k ]) )
    f.close()      

    # get storage usage for runs directories:
    gLogger.notice( 'Data Runs:', len( rawDirs ) )
    
    for prod,t in rawDirs.items():
      for stream,data in t.items():
        for d in data['Dirs']:
          res = self.__stDB.getDirectorySummaryPerSE( d )
          c += 1
          if res['OK']:
            for se in res['Value'].keys():
              if not se in data:
                data[se] = {'Files':0,'Size':0}
              data[se]['Files'] += res['Value'][se]['Files']
              data[se]['Size'] += res['Value'][se]['Size']
              if not se in rawSum:
                rawSum[se] = {'Files':0,'Size':0}
              rawSum[se]['Files'] += res['Value'][se]['Files']
              rawSum[se]['Size'] += res['Value'][se]['Size']
    
    gLogger.notice( 'Data Runs Done' )
    fileName = os.path.join( self.__workDirectory, "rawSum.txt" )
    f = open(fileName, "w")
    for se in rawSum.keys():
      f.write("%s %s \n" %(se, rawSum[ se ]))
    f.close()
    fileName = os.path.join( self.__workDirectory, "rawDirs.txt" )
    f = open(fileName, "w")
    for prod in rawDirs.keys():
      f.write("%s\n" % prod)
      for stream in rawDirs[ prod ].keys():
        f.write("-- %s\n" % stream )
        for k in rawDirs[ prod ][ stream ].keys():
          f.write("---- %s %s \n" %(k, rawDirs[ prod ][ stream ][ k ]) )
    f.close()      


    gLogger.notice( 'Data Done' )
    
    gLogger.notice( 'Queried directories:', c )
    
    end = time.time()
    duration = end - start
    gLogger.info("Total time to create storage usage dump: %d " % duration )
    # export these dictionaries:
    self.mcDirs = mcDirs
    self.mcSum = mcSum
    self.dataDirs = dataDirs
    self.dataSum = dataSum
    self.rawDirs = rawDirs
    self.rawSum = rawSum
 

    return S_OK()

#....................................................................................................
  def getStorageUsage_fromDBDump( self ):
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
       96000001 : 'NOBIAS'
    }

    now = Time.dateTime()
    eventTypeDesc = self.dict1['EventTypeDescription' ] # needed to send it to accounting (instead of numeric ID)
    eventTypeDesc = str( self.dict1['EventTypeId'] ) + '-' + eventTypeDesc
    self.log.debug( "eventTypeDescription %s" % eventTypeDesc )
    #dataTypeFlag = '' # either RealData or SimData
    LFCBasePath = {'RealData': '/lhcb/', 'SimData': '/lhcb/MC/'}
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
    self.log.notice( " Get productions for dict3 : %s " % dict3 )
    res = self.bkClient.getProductions( dict3 )
    if not res[ 'OK' ]:
      self.log.error( "ERROR: could not get productions for dict %s Error: %s" % ( dict3, res['Message'] ) )
      return S_ERROR( res )
    productions = sortList( [x[0] for x in res[ 'Value' ][ 'Records' ]] )
    if productions == []:
      self.log.notice( "WARNING: EMPTY QUERY ! no Production available for dict3= %s" % dict3 )
      return S_OK()
    self.log.notice( "Got the productions list: %s" % productions )
    for prodID in productions:
      if int( prodID ) < 0:
        dict3[ 'rawDataFlag'] = True
        if not dict3[ 'rawDataFlag']:
          self.log.warning("rawDataFlag was wrongly set to False. Now set to True")
      # get File Types (only for MC and reconstructed data, as for RAW data is only RAW)
      if not dict3[ 'rawDataFlag']:
        res = self.bkClient.getFileTypes( dict3 )
        if not res['OK']:
          self.log.notice( "ERROR getting file types for prod %s, Error: %s" % ( prodID, res['Message'] ) )
          continue
        prodFileTypes = reduce(lambda x,y:x+y, res['Value']['Records'])
        prodFileTypes = [ x for x in prodFileTypes if not re.search( "HIST", x ) ]
      # for raw data set the only file type as 'RAW'.
      # for non-raw data, add the HIST, SETC, DST file type to the list (to be checked why)
        prodFileTypes.append( 'HIST' )
        if 'DST' not in prodFileTypes:
          prodFileTypes.append( 'DST' )
        if 'SETC' not in prodFileTypes:
          prodFileTypes.append( 'SETC' )
      else:
        prodFileTypes = [ 'RAW' ]
      self.log.notice( "For prod %d list of event types: %s" % ( prodID, prodFileTypes ) )
 
      # select the dictionary: either raw data, or reconstructed data or MC:
      if dict3[ 'rawDataFlag']: # raw data
        myDirs = self.rawDirs
        # Bkk returns a negative number for raw data runs, change it to positive
        prodID = str(-1*prodID)
      else:
        prodID = str(prodID)
        if len(prodID)< 8:
          diff = 8 - len(prodID)
          prodID = diff*'0' + prodID
        if dict3[ 'dataTypeFlag' ] == 'RealData':
          myDirs =  self.dataDirs
        else:
          myDirs = self.mcDirs 


      # for MC and reconstructed data:
      if not dict3[ 'rawDataFlag']:
        for fType in prodFileTypes:
          if prodID not in myDirs.keys():
            self.log.warn("The storage usage for production %s  was not stored in dictionary!" % (prodID ) )
            continue
          if fType not in myDirs[ prodID ].keys():
            self.log.warn("The storage usage for production %s and fileType %s  was not stored in dictionary!" % (prodID, fType ) )
            continue
          
          for seName in sortList( myDirs[ prodID ][ fType ].keys() ):
            if seName == 'Dirs':
              continue
            try:
              physicalFiles = myDirs[ prodID ][ fType ][ seName ][ 'Files' ]
              physicalSize = myDirs[ prodID ][ fType ][ seName ][ 'Size' ]
            except:
              self.log.warn("The storage usage for production %s fileType %s SE %s was not stored in dictionary!" % (prodID, fType, seName ) )
              continue
            # TO BE FIXED!!
            logicalSize = 0
            logicalFiles = 0
            # create a record to be sent to the accounting:
            self.log.notice( ">>>>>>>>Send DataStorage record to accounting for fields: DataType: %s Activity: %s FileType: %s Production: %s ProcessingPass: %s Conditions: %s EventType: %s StorageElement: %s --> physFiles: %d  physSize: %d " % ( dict3['ConfigName'] , dict3['ConfigVersion'], fType, prodID, dict3['ProcessingPass'], dict3['ConditionDescription'], dict3['EventTypeDescription'], seName, physicalFiles, physicalSize ) )
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
        if prodID not in myDirs.keys():
          self.log.warn("Run %s was not in the storage usage dictionary! " % prodID )
          continue
        fType = 'RAW'
        streamInLFC = mapBkk2LFC[ dict3[ 'EventTypeId' ] ]
        if streamInLFC not in myDirs[ prodID ].keys():
          self.log.warn("The stream %s is not in the storage dictionary for run %s " %(dict3[ 'EventTypeId' ], prodID))
          continue
        for seName in myDirs[ prodID ][ streamInLFC ].keys():
          if seName == 'Dirs':
            continue
          try:
            physicalFiles = myDirs[ prodID ][ streamInLFC ][ seName ][ 'Files' ]
            physicalSize = myDirs[ prodID ][ streamInLFC ][ seName ][ 'Size' ]
          except:
            self.log.warn("The storage usage for production %s stream %s SE %s was not stored in dictionary!" % (prodID, streamInLFC, seName ) )
            continue
          #res = __fillAccountingRecord( )        
          
          # TO BE FIXED!!
          logicalSize = 0
          logicalFiles = 0

          self.log.notice( ">>>>>>>>Send DataStorage record to accounting for fields: DataType: %s Activity: %s FileType: %s Production: %s ProcessingPass: %s Conditions: %s EventType: %s StorageElement: %s --> physFiles: %d  physSize: %d " % ( dict3['ConfigName'] , dict3['ConfigVersion'], fType, prodID, dict3['ProcessingPass'], dict3['ConditionDescription'], dict3['EventTypeDescription'], seName, physicalFiles, physicalSize ) )
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



    if self.recordsToCommit > self.limitForCommit:
      res = gDataStoreClient.commit()
      if not res[ 'OK' ]:
        self.log.error( "Accounting ERROR: commit returned %s" % res )
      else:
        self.log.notice( "%d records committed " % self.recordsToCommit )
        self.recordsToCommit = 0
      self.log.notice( "In getStorageUsage commit returned: %s" % res )
 

    return S_OK()

#....................................................................................................
  def getStorageUsage( self ):

    """ Takes in input a dictionary containing ConfigName, ConfigVersion, EventType, Conditions, ProcessingPass and gets the productions/runs
    and file type, and then queries the StorageUsage service to get the space usage per SE
    """

    now = Time.dateTime()
    eventTypeDesc = self.dict1['EventTypeDescription' ] # needed to send it to accounting (instead of numeric ID)
    eventTypeDesc = str( self.dict1['EventTypeId'] ) + '-' + eventTypeDesc
    self.log.debug( "eventTypeDescription %s" % eventTypeDesc )
    #dataTypeFlag = '' # either RealData or SimData
    LFCBasePath = {'RealData': '/lhcb/', 'SimData': '/lhcb/MC/'}
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
    self.log.notice( " Get productions for dict3 : %s " % dict3 )
    res = self.bkClient.getProductions( dict3 )
    if not res[ 'OK' ]:
      self.log.error( "ERROR: could not get productions for dict %s Error: %s" % ( dict3, res['Message'] ) )
      return S_ERROR( res )
    productions = sortList( [x[0] for x in res[ 'Value' ][ 'Records' ]] )
    if productions == []:
      self.log.notice( "WARNING: EMPTY QUERY ! no Production available for dict3= %s" % dict3 )
      return S_OK()
    self.log.notice( "Got the productions list: %s" % productions )

    
    rawDataPFNUsage = {}
    if dict3[ 'rawDataFlag' ]:
      self.log.notice( "For RAW data, do a bulk query to get PFN usage and store in a dictionary ")
      runsList = []
      for p in productions:
        run = -1*p
        runsList.append( run )
      resBulkQuery = self.__stDB.getRunSummaryPerSE( runsList )
      if not resBulkQuery[ 'OK' ]:
        self.log.error( "ERROR: failed to retrieve PFN usage with message: %s" % ( resBulkQuery['Message'] ) )
        return S_ERROR( resBulkQuery )
      else:
        rawDataPFNUsage = resBulkQuery[ 'Value' ]
        #self.log.notice( "Physical storage usage %s" % res['Value'] )
      
    for prodID in productions:
      # get File Types (only for MC and reconstructed data, as for RAW data is only RAW)
      if not dict3[ 'rawDataFlag']:
        res = self.bkClient.getFileTypes( dict3 )
        if not res['OK']:
          self.log.notice( "ERROR getting file types for prod %s, Error: %s" % ( prodID, res['Message'] ) )
          continue
        prodFileTypes = reduce(lambda x,y:x+y, res['Value']['Records'])
        prodFileTypes = [ x for x in prodFileTypes if not re.search( "HIST", x ) ]
      # for raw data set the only file type as 'RAW'.
      # for non-raw data, add the HIST, SETC, DST file type to the list (to be checked why)
        prodFileTypes.append( 'HIST' )
        if 'DST' not in prodFileTypes:
          prodFileTypes.append( 'DST' )
        if 'SETC' not in prodFileTypes:
          prodFileTypes.append( 'SETC' )
      else:
        prodFileTypes = [ 'RAW' ]
      self.log.notice( "For prod %d list of event types: %s" % ( prodID, prodFileTypes ) )
      dataPath = LFCBasePath[ dict3[ 'dataTypeFlag'] ]
      for prodFileType in prodFileTypes:
        # get the LOGICAL usage with getSummary, except for raw data
        if not dict3[ 'rawDataFlag']:
          res = self.__stDB.getSummary( dataPath, prodFileType, prodID )
          if not res[ 'OK' ]:
            self.log.error( "ERROR: getSummary failed with message: %s" % ( res['Message'] ) )
            return S_ERROR( res )
          logicalFiles = 0
          logicalSize = 0
          logicalUsage = res[ 'Value' ]
          for dir in logicalUsage.keys():
            logicalFiles += logicalUsage[ dir]['Files']
            logicalSize += logicalUsage[ dir ]['Size']
        else: # for RAW data ask the Bkk to get the logical storage usage (size and number of LFNs)
          if prodID < 0:
            runNo = -1 * prodID
          else:
            self.log.error( "ERROR: For RAW data the returned productionID should be negative! prodID = %d" % prodID )
            continue
          res = self.bkClient.getRunInformations( runNo )
          if not res[ 'OK' ]:
            self.log.error( "ERROR: getRunInformations failed with message: %s" % ( res['Message'] ) )
            return S_ERROR( res )
          logicalFiles = 0
          logicalSize = 0
          for size in res[ 'Value']['File size']:
            logicalSize += size
          for file in res['Value']['Number of file']:
            logicalFiles += file

        self.log.notice( "Logical usage for fileType = %s, prod/run = %d => files: %d size: %f GB" % ( prodFileType, prodID, logicalFiles, logicalSize / byteToGB ) )

        # get the storage usage in terms of PFNs:
        if not dict3[ 'rawDataFlag']:
          self.log.notice( "Calling getStorageSummary with path= %s prodFileType= %s prodID= %d " % ( dataPath, prodFileType, prodID ) )
          res = self.__stDB.getStorageSummary( dataPath, prodFileType, prodID, self.sesOfInterest )
          if not res[ 'OK' ]:
            self.log.error( "ERROR: failed to retrieve PFN usage with message: %s" % ( res['Message'] ) )
            return S_ERROR( res )
          usageDict = res[ 'Value' ]
        else: # RAW data
          # the storage usage is stored in a dictionary (output of a bulk query)
          #res = self.__stDB.getRunSummaryPerSE( runNo )
          runNo = -1 * prodID
          usageDict = rawDataPFNUsage[ runNo ]

        for seName in sortList( usageDict.keys() ):
          files = usageDict[seName]['Files']
          size = usageDict[seName]['Size']
          # create a record to be sent to the accounting:
          self.log.notice( ">>>>>>>>Send DataStorage record to accounting for fields: DataType: %s Activity: %s FileType: %s Production: %d ProcessingPass: %s Conditions: %s EventType: %s StorageElement: %s --> physFiles: %d  physSize: %d " % ( dict3['ConfigName'] , dict3['ConfigVersion'], prodFileType, prodID, dict3['ProcessingPass'], dict3['ConditionDescription'], dict3['EventTypeDescription'], seName, files, size ) )
          dataRecord = DataStorage()
          dataRecord.setStartTime( now )
          dataRecord.setEndTime( now )
          dataRecord.setValueByKey( "DataType", dict3['ConfigName'] )
          dataRecord.setValueByKey( "Activity", dict3['ConfigVersion'] )
          dataRecord.setValueByKey( "FileType", prodFileType )
          res = dataRecord.setValueByKey( "Production", prodID )
          if not res[ 'OK' ]:
            self.log.notice( "Accounting ERROR prod %s , res= %s" % ( prodID, res ) )
          dataRecord.setValueByKey( "ProcessingPass", dict3['ProcessingPass'] )
          dataRecord.setValueByKey( "Conditions", dict3['ConditionDescription'] )
          dataRecord.setValueByKey( "EventType", dict3['EventTypeDescription'] )
          dataRecord.setValueByKey( "StorageElement", seName )
          dataRecord.setValueByKey( "PhysicalSize", size )
          dataRecord.setValueByKey( "PhysicalFiles", files )
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

#..............................................................................................................



  def getProcessingPass( self, thisDict, path ):
    # retrieves all processing pass recursively from the initial path '/', for the given bkk dictionary

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



