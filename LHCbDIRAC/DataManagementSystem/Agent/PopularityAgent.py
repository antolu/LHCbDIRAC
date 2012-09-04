########################################################################
# $HeadURL $
# File: PopularityAgent.py
########################################################################
""" :mod: PopularityAgent
    =====================
 
    .. module: PopularityAgent
    :synopsis: The Popularity Agent creates reports about per LFC directory data usage.

    The Popularity Agent creates reports about per LFC directory data usage, based on the 
    StorageUsageDB/Popularity table. Then it creates an accounting record for each directory, 
    adding all the relevant directory metadata, obtained from the StorageUsageDB/DirMetadata table.
    The accounting records are stored in the AccountingDB and then displayed via the web portal.
"""
# imports
import os
from datetime import datetime, timedelta
## from DIRAC
from DIRAC  import S_OK, S_ERROR, gLogger
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.Core.Utilities import Time
from LHCbDIRAC.AccountingSystem.Client.Types.Popularity import Popularity
from DIRAC.AccountingSystem.Client.DataStoreClient import gDataStoreClient
from DIRAC.Core.DISET.RPCClient import RPCClient
## from LHCbDIRAC
from LHCbDIRAC.DataManagementSystem.Client.DataUsageClient import DataUsageClient
from LHCbDIRAC.DataManagementSystem.DB.StorageUsageDB import StorageUsageDB

__RCSID__ = "$Id$"

AGENT_NAME = "DataManagement/PopularityAgent"

class PopularityAgent( AgentModule ):
  """
  .. class:: PopularityAgent
  
  """
  ## DataUsageClient
  __dataUsageClient = None
  ## StorageUsageDB instance or DMS/DataUsage RPS client 
  __stDB = None
  ## BKK Client
  __bkClient = None
  ## work directory
  __workDirectory = None
  ## counter for records to be sent to the accouting
  numPopRows = None

  def initialize( self ):
    """ agent initialisation """
    self.am_setOption( 'PollingTime', 43200 )
    if self.am_getOption( 'DirectDB', False ):
      self.__stDB = StorageUsageDB()
      #self.bkClient = BookkeepingClient()#the necessary method is still not available in Bookk. client
    else:
      self.__stDB = RPCClient( 'DataManagement/DataUsage' )
      timeout = 600
      self.__bkClient = RPCClient('Bookkeeping/BookkeepingManager', timeout=timeout)
    self.__dataUsageClient = DataUsageClient()
    self.__workDirectory =  self.am_getOption( "WorkDirectory" )
    if not os.path.isdir( self.__workDirectory ):
      os.makedirs( self.__workDirectory )
    gLogger.info( "Working directory is %s" % self.__workDirectory )
    self.timeIntervalForPopularityRecords = self.am_getOption( 'timeIntervalForPopularityRecords', 1 ) # by default, collects raw records from Popularity table inserted in the last day
    
    return S_OK()

#.........................................................................................

  def execute( self ):
    """ Main loop of Popularity agent """
   
    now = datetime.now()
    endTime = datetime(now.year, now.month, now.day, 0, 0, 0)
    startTime = endTime - timedelta(days=self.timeIntervalForPopularityRecords)
    endTimeQuery = endTime.isoformat()
    startTimeQuery = startTime.isoformat()
    # query all traces in popularity in the time rage startTime,endTime and status =new
    # the condition to get th etraces is the AND of the time range and the status new 
    gLogger.info("Querying Pop db to retrieve entries in time range %s - %s " % ( startTimeQuery, endTimeQuery ) )
    status = 'New'
    res = self.__dataUsageClient.getDataUsageSummary( startTimeQuery, endTimeQuery, status )
    if not res['OK']:
      gLogger.error("Error querying Popularity table.. %s" % res['Message'] )
      return S_ERROR( res['Message'] )
    val = res[ 'Value' ]
    self.log.verbose("val %s " % (val,) )
    gLogger.info("Retrieved %d entries from Pop table" % len( val ))
    # Build popularity report, and store the Ids in a  list:
    idList = []  
    traceDict = {}
    for row in val:
      self.log.verbose("row: %s" % (row,) )
      rowId, dirLfn, site, count, insertTime = row
      if rowId not in idList:
        idList.append( rowId )
      else:
        gLogger.error("Same Id found twice! %d " % rowId )
        continue
      # get the day (to do )
      dayBin = self.computeDayBin( startTime, insertTime )
      if dayBin not in traceDict.keys():
        traceDict[ dayBin ] = {}
      if dirLfn not in traceDict[ dayBin ].keys():
        traceDict[ dayBin ][ dirLfn ] = {}
      if site not in  traceDict[ dayBin ][ dirLfn ].keys():
        traceDict[ dayBin ][ dirLfn ][ site ] = 0
      traceDict[ dayBin ][ dirLfn ][ site ] += count

    # print a summary
    dayList = traceDict.keys()
    dayList.sort()
    for day in dayList: 
      gLogger.info( " ###### day %s (starting from %s ) " % (day, startTimeQuery) )
      for lfn in traceDict[ day ].keys():
        gLogger.info( " ---- lfn %s " % lfn )
        for site in traceDict[ day ][ lfn ].keys():
          gLogger.info( " -------- site  %s  count: %d " %( site , traceDict[ day ][ lfn ][ site ] ))
    
    

    gLogger.info( "Retrieve meta-data information for each directory " )
    now = Time.dateTime()
    self.numPopRows = 0 # keep a counter of the records to send to accounting data-store
    for day in traceDict.keys():
      timeForAccounting = self.computeTimeForAccounting( startTime, day )
      gLogger.info( "Processing day %s - time for accounting %s " % (day, timeForAccounting ) )
      for dirLfn in traceDict[ day ].keys():
      #did = configName = configVersion = conditions = processingPass = eventType = fileType = production = "na"
      # retrieve the directory meta-data from the DirMetadata table
        gLogger.info( "Processing dir %s " % dirLfn )
        if dirLfn.startswith('/lhcb/user/'):
          gLogger.info("Private user directory. No metadata stored in Bkk %s " % dirLfn )
          continue

        dirList = [ dirLfn ]
        # this could be done in a bulk query for a list of directories... TBD
        res = self.__dataUsageClient.getDirMetadata( dirList ) 
        if not res[ 'OK' ]:
          gLogger.error("Error retrieving directory meta-data %s " % res['Message'] )
          continue
        if not res['Value']:
          self.log.warn( "No result returned for directory %s from the getDirMetadata method" % dirList )
          gLogger.info( "Query Bookkeeping to retrieve '%s' folder metadata and store them in the cache" % dirList ) 
          res = self.__bkClient.getDirectoryMetadata( dirLfn )
          if not res[ 'OK' ]:
            gLogger.error( "Failed to query Bookkeeping %s" %res[ 'Message' ] )
            continue
          gLogger.info( "Successfully queried Bookkeeping, result: %s " % res )
          if not res['Value']:
            self.log.warn( "Directory is not registered in Bookkeeping! %s " % dirLfn )
            configName = configVersion = conditions = processingPass = eventType = fileType = production = "na" 
          else:
            metadata = res['Value'][ 0 ]
            configName = metadata[ 'ConfigName' ]
            configVersion = metadata[ 'ConfigVersion' ]
            conditions = metadata[ 'ConditionDescription' ]
            processingPass = metadata[ 'ProcessingPass' ]
            eventType = metadata[ 'EventType' ]
            fileType = metadata[ 'FileType' ]
            production = metadata[ 'Production' ]
        
            gLogger.info( "Cache this entry in DirMetadata table.." )
            dirMetadataDict = {}
            dirMetadataDict[ dirLfn ] = metadata
            res = self.__dataUsageClient.insertToDirMetadata( dirMetadataDict )
            if not res[ 'OK' ]:
              gLogger.error( "Failed to insert metadata in DirMetadata table! %s " % res[ 'Message' ] )
            else:
              gLogger.info( "Successfully inserted metadata for directory %s in DirMetadata table " % dirMetadataDict )
              gLogger.verbose( "result: %s " % res )
  
        else:
          gLogger.info( "Directory %s was cached in DirMetadata table" % dirLfn )     
          for row in res['Value']:
            did, configName, configVersion, conditions, processingPass, eventType, fileType, production = row
        
        for site in traceDict[ day ][ dirLfn ]:
          usage = traceDict[ day ][ dirLfn ][ site ]
          gLogger.info("%s %s %d" %( dirLfn, site, usage ))
          # compute the normalized usage, dividing by the number of files in the directory:
          normUsage = usage # to be done! after we have decided how to normalize
          # Build record for the accounting
          popRecord = Popularity()
          popRecord.setStartTime( timeForAccounting ) 
          popRecord.setEndTime( timeForAccounting )
          popRecord.setValueByKey( "DataType", configName )
          popRecord.setValueByKey( "Activity", configVersion )
          popRecord.setValueByKey( "FileType", fileType )
          popRecord.setValueByKey( "Production", production )
          popRecord.setValueByKey( "ProcessingPass", processingPass )
          popRecord.setValueByKey( "Conditions", conditions )
          popRecord.setValueByKey( "EventType", eventType )
          popRecord.setValueByKey( "StorageElement", site )
          popRecord.setValueByKey( "Usage", usage )
          popRecord.setValueByKey( "NormalizedUsage", normUsage )
          res = gDataStoreClient.addRegister( popRecord )
          if not res[ 'OK']:
            gLogger.error( "ERROR: addRegister returned: %s" % res['Message'] )
            continue
          self.numPopRows += 1
          gLogger.info(">>> Sending record to accounting for: %s %s %s %s %s %s %s %s %s %d %d " % (timeForAccounting, configName, configVersion, fileType, production, processingPass, conditions, eventType, site, usage, normUsage ) )
    gLogger.info(" %d records to be sent to Popularity accounting" %self.numPopRows )        
    res = gDataStoreClient.commit()
    if not res[ 'OK' ]:
      gLogger.error( "ERROR: committing Popularity records: %s " % res )
      return S_ERROR( res )
    else:
      gLogger.info( "%s records for Popularity type successfully committed" %self.numPopRows )
      # then set the status to Used
      gLogger.info("Set the status to Used for %d entries" % len( idList ) )
      res = self.__dataUsageClient.updatePopEntryStatus( idList, 'Used' )

      if not res['OK']:
        gLogger.error("Error to update status in  Popularity table.. %s" % res['Message'] )
        return S_ERROR( res['Message'] )
      else:
        gLogger.info("Status updated to Used correctly for %s entries " % res[ 'Value' ] )
  
    return S_OK()
 

#.........................................................................................

  def computeDayBin( self, queryStartTime , creationTime):
    """ compute the time bin for the record, from 0 to the interval time of the query 
    """
    gLogger.verbose( "find day bin for: %s %s " % ( queryStartTime, creationTime ) ) 
    day = creationTime - queryStartTime
    dayBin = day.days
    gLogger.verbose( "day bin %s " % dayBin )
    return dayBin

#.........................................................................................

  def computeTimeForAccounting( self, startTime, day ):
    """ Compute the time for the accounting record, starting from the start time of the query and the day bin 
    """ 
    gLogger.info( "find time for accounting for startTime: %s + day %s " % ( startTime, day ) )
    daysToAdd = timedelta(days= day, hours=12 ) # add 12h just to put the value in the middle of time bin
    gLogger.info( "timedelta to add: %s " % daysToAdd )
    accTime = startTime + daysToAdd
    gLogger.info( "accTime = %s " % accTime )
    return accTime
    
