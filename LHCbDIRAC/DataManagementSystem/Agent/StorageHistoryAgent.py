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
from LHCbDIRAC.DataManagementSystem.Client.StorageUsageClient           import StorageUsageClient
import sys, re

from DIRAC  import S_OK, S_ERROR

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
    self.suClient = StorageUsageClient()
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

    ###
    # Accounting records relative to the DataStorage type:
    ###
    # work flow:
    # 1- queries the BKK client to get the set of data taking conditions relative to: eventType=93000000,ConfigVersion': 'Collision10', 'ConfigName': 'LHCb'.
    # 2- loops on the data taking conditions and retrieve for each of them the set of all existing processing passes.
    #    Loops on all processing passes:
    #    2.1 - for every ( data taking condition, proc. pass) queries the BKK client to get the list of productions
    #    2.2 - for production, gets the list of file types and adds the HIST, DST and SETC (not clear..) and calls StorageUsageClient to get the usage per SE.

    reportForDataManager = '/tmp/reportForDataManager.txt'
    self.report = open( reportForDataManager, "w" )
    self.sesOfInterest = ''

    bkDict = {}
    self.numDataRows = 0 # count the total number of records sent to the accounting
    self.eventTypeDescription = {}
    numRows = 0
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
        res = self.bkClient.getEventTypes ( bkDict )
        if not res[ 'OK' ]:
          return S_ERROR( res )
        eventTypes = res['Value']['Records']
        self.log.notice( " ConfigName, ConfigVersion:  %s , %s" % ( configName, configVersion ) )
        self.log.notice( "--> EventTypes: %s " % eventTypes )
        for ev in eventTypes:
          eventTypeId = ev[ 0 ]
          bkDict[ 'EventTypeId' ] = eventTypeId
          self.eventTypeDescription[ eventTypeId ] = ev[ 1 ]
          self.log.notice( "Call getDataTakingConditionsAndSU with dict: %s " % bkDict )
          self.dict1 = bkDict
          res = self.getDataTakingConditionsAndSU()
          if not res[ 'OK' ]:
            self.log.error( "getDataTakingConditionsAndSU returned ERROR! %s" % res )
            #return S_ERROR( res )
            continue
          if QUICKLOOP:# for debugging purpose (will be removed later)
            break
        if QUICKLOOP:# for debugging purpose (will be removed later)
          break
      if QUICKLOOP:# for debugging purpose (will be removed later)
        break
    self.log.notice( "Sending %d records to DataStore for the DataStorage type" % self.numDataRows )
    res = gDataStoreClient.commit()
    if not res[ 'OK' ]:
      self.log.notice( "ERROR: commit returned: %s " % res )
      return S_ERROR( res )
    return S_OK()




  def getDataTakingConditionsAndSU( self ):

    """ Takes in input a dictionary containing eventType and configName and configVersion, and compute the possible dataTaking conditions
    - then calls the getProcPassWithCondTaking and get all the possible processing pass in a recursive way
    - then gets all possible productions and event types and queries the StorageUsage service to get the space usage per SE
    """

    res = self.bkClient.getConditions( self.dict1 )
    now = Time.dateTime()
    eventTypeDesc = self.eventTypeDescription[ self.dict1['EventTypeId'] ] # needed to send it to accounting (instead of numeric ID)
    eventTypeDesc = str( self.dict1['EventTypeId'] ) + '-' + eventTypeDesc
    self.log.notice( "eventTypeDescription %s" % eventTypeDesc )
    if not res[ 'OK' ]:
      return S_ERROR( res )
    # res['Value'][0] stores SIMULATION CONDITIONS: filled only for simulated data
    # res['Value'][1] stores DATA TAKING CONDITIONS: filled only for real data
    if ( res['Value'][1]['TotalRecords'] + res['Value'][0]['TotalRecords'] == 0 ):
      self.log.notice( "WARNING: empty getConditions query for dict1= %s" % self.dict1 )
      return S_OK( res )
    dataTypeFlag = '' # either RealData or SimData
    LFCBasePath = {'RealData': '/lhcb/', 'SimData': '/lhcb/MC/'}
    if res['Value'][1]['TotalRecords'] > 0:
      self.log.notice( "Returned conditions are relative to REAL DATA for dict1= %s" % self.dict1 )
      index = 1
      dataTypeFlag = 'RealData'
    elif res['Value'][0]['TotalRecords'] > 0:
      self.log.notice( "Returned conditions are relative to SIMULATED DATA for dict1= %s" % self.dict1 )
      index = 0
      dataTypeFlag = 'SimData'
    records = res[ 'Value' ][ index ][ 'Records' ]
    # records is a list of Data taking conditions tuples
    dataTakingConds = {}
    for dataTakingTuple in records:
      dataTakingCondID = dataTakingTuple[ 0 ] # numeric ID
      dataTakingCond = dataTakingTuple[ 1 ]   # Description
      if not dataTakingCond in dataTakingConds.keys():
        dataTakingConds[ dataTakingCond ] = dataTakingCondID

    processingPassTotals = {}
    self.log.notice( "Starts loop on Conditions list: %s" % dataTakingConds.keys() )
    for dataTakingCond in sortList( dataTakingConds.keys() ):
      self.log.notice( "Processing DataTaking conditions: %s" % dataTakingCond )
      self.report.write( "\n===============\ndataTakingCond= %s\n===============\n" % ( dataTakingCond ) )
      self.proPassTuples = []
      self.getProcPassWithCondTaking( dataTakingCond, '/' )  # this function stores in the self.proPassTuples variable a list of processing pass tuples.
      processedProcessingPasses = []
      for proPassTuple in sortList( self.proPassTuples ): # loop on processing passes
        rawDataFlag = False # True only for RAW data
        self.log.notice( "proPassTuple: %s" % proPassTuple )
        if proPassTuple == '/Real Data': #RAW DATA
          rawDataFlag = True
        dict3 = {}
        dict3[ 'ConfigName' ] = self.dict1[ 'ConfigName' ]
        dict3[ 'ConfigVersion' ] = self.dict1[ 'ConfigVersion' ]
        dict3[ 'EventTypeId' ] = self.dict1[ 'EventTypeId']
        dict3[ 'ProcessingPass' ] = proPassTuple
        dict3[ 'ConditionDescription'] = dataTakingCond
        # get list of productions for every tuple (ConfigName, ConfigVersion, ProcessingPass, ConditionDescription)
        res = self.bkClient.getProductions( dict3 )
        self.log.notice( " dict3 : %s " % dict3 )
        if not res[ 'OK' ]:
          self.log.notice( "ERROR: could not get productions for dict %s Error: %s" % ( dict3, res['Message'] ) )
          return S_ERROR( res )
        productions = sortList( [x[0] for x in res[ 'Value' ][ 'Records' ]] )
        if productions == []:
          self.log.notice( "WARNING: EMPTY QUERY ! no Production available for dict3= %s" % dict3 )
          continue
        self.report.write( "\n\t%s (%s)\n" % ( proPassTuple, intListToString( sortList( productions ) ) ) )
        self.log.notice( "Got the productions list: %s" % productions )
        my_total = 0
        my_total_perSE = {}
        for prodID in productions:
          #self.log.notice( "Getting number of evts for prod: %s " % prodID )
          res = self.bkClient.getNumberOfEvents( prodID )
          # returns a list of tuples of type: ('DAVINCIHIST', None, 91000000, 33154)
          if not res['OK']:
            self.log.notice( "ERROR getting number of events for prod %s, Error: %s" % ( prodID, res['Message'] ) )
            continue
          else:
            for eventNumberTuple in res['Value']:
              self.log.notice( "for prod %d got this events: %s" % ( prodID, eventNumberTuple ) )
            #resFileType, resEvtNumber, resEventType, physEventNumber = eventNumberTuple

          prodFileTypes = sortList( [x[0] for x in res['Value']] )
          prodFileTypes = [ x for x in prodFileTypes if not re.search( "HIST", x ) ]
          # for raw data set the only file type as 'RAW'.
          # for non-raw data, add the HIST, SETC, DST file type to the list (to be checked why)
          if not rawDataFlag:
            prodFileTypes.append( 'HIST' )
            if 'DST' not in prodFileTypes:
              prodFileTypes.append( 'DST' )
            if 'SETC' not in prodFileTypes:
              prodFileTypes.append( 'SETC' )
          else:
            prodFileTypes = [ 'RAW' ]
          self.log.notice( "For prod %d list of event types: %s" % ( prodID, prodFileTypes ) )
          dataPath = LFCBasePath[ dataTypeFlag ]
          for prodFileType in prodFileTypes:
            # get the LOGICAL usage with getSummary, except for raw data
            if not rawDataFlag:
              res = self.suClient.getSummary( dataPath, prodFileType, prodID )
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
            if not rawDataFlag:
              self.log.notice( "Calling getStorageSummary with path= %s prodFileType= %s prodID= %d " % ( dataPath, prodFileType, prodID ) )
              res = self.suClient.getStorageSummary( dataPath, prodFileType, prodID, self.sesOfInterest )
            else: # RAW data
              self.log.notice( "Calling getRunSummaryPerSE with run= %d " % ( runNo ) )
              res = self.suClient.getRunSummaryPerSE( runNo )
            if not res[ 'OK' ]:
              self.log.error( "ERROR: failed to retrieve PFN usage with message: %s" % ( res['Message'] ) )
              return S_ERROR( res )
            else:
              self.log.notice( "Physical storage usage %s" % res['Value'] )
            usageDict = res[ 'Value' ]
            for seName in sortList( usageDict.keys() ):
              files = usageDict[seName]['Files']
              size = usageDict[seName]['Size']
              # create a record to be sent to the accounting:
              self.log.notice( ">>>>>>>>Send DataStorage record to accounting for fields: DataType: %s Activity: %s FileType: %s Production: %d ProcessingPass: %s Conditions: %s EventType: %s StorageElement: %s --> physFiles: %d  physSize: %d " % ( self.dict1['ConfigName'] , self.dict1['ConfigVersion'], prodFileType, prodID, proPassTuple, dataTakingCond, eventTypeDesc, seName, files, size ) )
              dataRecord = DataStorage()
              dataRecord.setStartTime( now )
              dataRecord.setEndTime( now )
              dataRecord.setValueByKey( "DataType", self.dict1['ConfigName'] )
              dataRecord.setValueByKey( "Activity", self.dict1['ConfigVersion'] )
              dataRecord.setValueByKey( "FileType", prodFileType )
              #if rawDataFlag:
              #  res = dataRecord.setValueByKey( "Production", runNo )
              #else:
              res = dataRecord.setValueByKey( "Production", prodID )
              if not res[ 'OK' ]:
                self.log.notice( "Accounting ERROR prod %s , res= %s" % ( prodID, res ) )
              dataRecord.setValueByKey( "ProcessingPass", proPassTuple )
              dataRecord.setValueByKey( "Conditions", dataTakingCond )
              dataRecord.setValueByKey( "EventType", eventTypeDesc )
              dataRecord.setValueByKey( "StorageElement", seName )
              dataRecord.setValueByKey( "PhysicalSize", size )
              dataRecord.setValueByKey( "PhysicalFiles", files )
              dataRecord.setValueByKey( "LogicalSize", logicalSize )
              dataRecord.setValueByKey( "LogicalFiles", logicalFiles )
              res = gDataStoreClient.addRegister( dataRecord )
              if not res[ 'OK']:
                self.log.notice( "ERROR: In getDataTakingConditionsAndSU addRegister returned: %s" % res )
              self.numDataRows += 1
              if seName not in my_total_perSE.keys():
                my_total_perSE[ seName ] = {}
                my_total_perSE[seName][ 'Size' ] = 0
                my_total_perSE[seName][ 'Files' ] = 0
              my_total_perSE[seName][ 'Size' ] += size
              my_total_perSE[seName][ 'Files' ] += files


        self.report.write( "Totals per SE ( DataTakingCond=%s, ProcPass=%s )\n" % ( dataTakingCond, proPassTuple ) )
        self.report.write( "   SE                       Size(GB) \n" )
        tot_geral = 0
        for SeTOT in sortList( my_total_perSE.keys() ):     # sortList(fileDict.keys()):
          tot_SE = my_total_perSE[SeTOT][ 'Size' ] / byteToGB
          tot_geral += tot_SE
          self.report.write( "%s %s\n" % ( str( SeTOT ).ljust( 15 ), str( "%.1f" % ( tot_SE ) ).rjust( 20 ) ) )
          self.report.write( "------------------------------------ \n" )
        self.report.write( "%s %s\n\n" % ( str( "TOTAL" ).ljust( 15 ), str( "%.1f" % ( tot_geral ) ).rjust( 20 ) ) )
        if not processingPassTotals.has_key( dataTakingCond ):
          continue
        if not processingPassTotals[dataTakingCond].has_key( proPassName ):
          continue
      if QUICKLOOP: # for debugging
        break
    res = gDataStoreClient.commit()
    if not res[ 'OK' ]:
      self.log.error( "Accounting ERROR: commit returned %s" % res )
    self.log.notice( "In getDataTakingConditionsAndSU commit returned: %s" % res )
    return S_OK()

#..............................................................................................................



  def getProcPassWithCondTaking( self, dataTakingCond, path ):
    # retrieves all processing pass recursively from the initial path '/', for a given data taking condition

    dict2 = {}
    dict2[ 'ConfigName'] = self.dict1[ 'ConfigName' ]
    dict2[ 'ConfigVersion'] = self.dict1[ 'ConfigVersion' ]
    dict2[ 'EventTypeId' ] = self.dict1[ 'EventTypeId' ]
    dict2[ 'ConditionDescription'] = dataTakingCond


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
        self.getProcPassWithCondTaking( dataTakingCond, newpath )

    return S_OK()



