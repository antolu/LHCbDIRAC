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
from DIRAC  import S_OK, S_ERROR
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
    self.log.info( "Working directory is %s" % self.__workDirectory )

    return S_OK()

  def execute( self ):
    """ Main loop of Popularity agent """
   
    now = datetime.now()
    endTime = datetime(now.year, now.month, now.day, 0, 0, 0)
    startTime = endTime - timedelta(days=1)
    endTimeQuery = endTime.isoformat()
    startTimeQuery = startTime.isoformat()
    # query all traces in popularity in the time rage startTime,endTime and status =new
    # the condition to get th etraces is the AND of the time range and the status new 
    self.log.info("Querying Pop db to retrieve entries in time range %s - %s " % ( startTimeQuery, endTimeQuery ) )
    status = 'New'
    res = self.__dataUsageClient.getDataUsageSummary( startTimeQuery, endTimeQuery, status )
    if not res['OK']:
      self.log.error("Error querying Popularity table.. %s" % res['Message'] )
      return S_ERROR( res['Message'] )
    val = res[ 'Value' ]
    self.log.info("Retrieved %d entries from Pop table" % len( val ))
  
    # Build popularity report, and store the Ids in a  list:
    idList = []  
    traceDict = {}
    for row in val:
      rowId, dirLfn, site, count = row
      if rowId not in idList:
        idList.append( rowId )
      else:
        self.log.error("Same Id found twice! %d " % rowId )
        continue
      if dirLfn not in traceDict.keys():
        traceDict[ dirLfn ] = {}
      if site not in traceDict[ dirLfn ].keys():
        traceDict[ dirLfn ][ site ] = 0
      traceDict[ dirLfn ][ site ] += count
    now = Time.dateTime()
    self.numPopRows = 0 # keep a counter of the records to send to accounting data-store
    for dirLfn in traceDict:
      #did = configName = configVersion = conditions = processingPass = eventType = fileType = production = "na"
      # retrieve the directory meta-data from the DirMetadata table
      self.log.info( "Processing dir %s " % dirLfn )
      dirList = [ dirLfn ]
      # this could be done in a bulk query for a list of directories... TBF
      res = self.__dataUsageClient.getDirMetadata( dirList ) 
      if not res[ 'OK' ]:
        self.log.error("Error retrieving directory meta-data %s " % res['Message'] )
        continue
      if not res['Value']:
        self.log.warn( "No result returned for directory %s from the getDirMetadata method" % dirList )
        self.log.info( "Query Bookkeeping to retrieve '%s' folder metadata and store them in the cache" % dirList ) 
        res = self.__bkClient.getDirectoryMetadata( dirLfn )
        if not res[ 'OK' ]:
          self.log.error( "Failed to query Bookkeeping %s" %res[ 'Message' ] )
          continue
        self.log.info( "Successfully queried Bookkeeping, result: %s " % res )
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
        
          self.log.info( "Cache this entry in DirMetadata table.." )
          dirMetadataDict = {}
          dirMetadataDict[ dirLfn ] = metadata
          res = self.__dataUsageClient.insertToDirMetadata( dirMetadataDict )
          if not res[ 'OK' ]:
            self.log.error( "Failed to insert metadata in DirMetadata table! %s " % res[ 'Message' ] )
          else:
            self.log.info( "Successfully inserted metadata for directory %s in DirMetadata table " % dirMetadataDict )
            self.log.verbose( "result: %s " % res )
  
      else:
        self.log.info( "Directory %s was cached in DirMetadata table" % dirLfn )     
        for row in res['Value']:
          did, configName, configVersion, conditions, processingPass, eventType, fileType, production = row
        
      for site in traceDict[ dirLfn ]:
        usage = traceDict[ dirLfn ][ site ]
        self.log.info("%s %s %d" %( dirLfn, site, usage ))
        # compute the normalized usage, dividing by the number of files in the directory:
        normUsage = usage # tp be done!
        # Build record for the accounting
        popRecord = Popularity()
        popRecord.setStartTime( now )
        popRecord.setEndTime( now )
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
          self.log.error( "ERROR: addRegister returned: %s" % res['Message'] )
          continue
        self.numPopRows += 1

    self.log.info(" %d records to be sent to Popularity accounting" %self.numPopRows )        
    res = gDataStoreClient.commit()
    if not res[ 'OK' ]:
      self.log.notice( "ERROR: committing Popularity records: %s " % res )
      return S_ERROR( res )
    else:
      self.log.info( "%s records for Popularity type successfully committed" %self.numPopRows )
      # then set the status to Used
      self.log.info("Set the status to Used for %d entries" % len( idList ) )
      res = self.__dataUsageClient.updatePopEntryStatus( idList, 'Used' )

      if not res['OK']:
        self.log.error("Error to update status in  Popularity table.. %s" % res['Message'] )
        return S_ERROR( res['Message'] )
      else:
        self.log.info("Status updated to Used correctly for %s entries " % res[ 'Value' ] )
  
    return S_OK()
 
