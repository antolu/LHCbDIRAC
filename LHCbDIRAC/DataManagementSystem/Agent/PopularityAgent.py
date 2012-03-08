"""   The Popularity Agent creates reports about per LFC directory data usage, based on the StorageUsageDB/Popularity table
      Then it creates an accounting record for each directory, adding all the relevant directory metadata, obtained from the 
      StorageUsageDB/DirMetadata table
      The accounting records are stored in the AccountingDB and then displayed via the web portal.
"""

__RCSID__ = "$Id$"

from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.Core.Utilities import Time
from DIRAC.Core.Utilities.List                           import sortList, intListToString
from LHCbDIRAC.AccountingSystem.Client.Types.Popularity import Popularity
from DIRAC.AccountingSystem.Client.DataStoreClient import gDataStoreClient
#from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient            import BookkeepingClient
import os, sys, time
from datetime import datetime, timedelta
from DIRAC  import S_OK, S_ERROR, gLogger



class PopularityAgent( AgentModule ):

  def initialize( self ):
    """Sets defaults
    """
    self.am_setOption( 'PollingTime', 43200 )
    if self.am_getOption( 'DirectDB', False ):
      from LHCbDIRAC.DataManagementSystem.DB.StorageUsageDB import StorageUsageDB
      self.__stDB = StorageUsageDB()
    else:
      from DIRAC.Core.DISET.RPCClient import RPCClient
      self.__stDB = RPCClient( 'DataManagement/DataUsage' )
      #self.bkClient = BookkeepingClient()
      timeout = 600
      self.__bkClient = RPCClient('Bookkeeping/BookkeepingManager', timeout=timeout)

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
    gLogger.info("Querying Pop db to retrieve entries in time range %s - %s " %(  startTimeQuery, endTimeQuery) )
    status = 'New'
    res = self.__stDB.getDataUsageSummary( startTimeQuery, endTimeQuery, status )
    #res = self.__stDB.getDataUsageSummary_2( startTimeQuery, endTimeQuery, status )
    if not res['OK']:
      gLogger.error("Error querying Popularity table.. %s" % res['Message'] )
      return S_ERROR( res['Message'] )
    val = res[ 'Value' ]
    gLogger.info("Retrieved %d entries from Pop table" % len( val ))
  
    # Build popularity report, and store the Ids in a  list:
    IdList = []  
    traceDict = {}
    for row in val:
      id, dirLfn, site, count = row
      if id not in IdList:
        IdList.append( id )
      else:
        gLogger.error("Same Id found twice! %d " % id )
        continue
      if dirLfn not in traceDict.keys():
        traceDict[ dirLfn ] = {}
      if site not in traceDict[ dirLfn ].keys():
        traceDict[ dirLfn ][ site ] = 0
      traceDict[ dirLfn ][ site ] += count
    gLogger.info( "Report from Pop table: " )
    now = Time.dateTime()
    self.numPopRows = 0 # keep a counter of the records to send to accounting data-store
    for dirLfn in traceDict.keys():
      #did, configName, configVersion, conditions, processingPass, eventType, fileType, production = ('na', 'na', 'na', 'na', 'na', 'na', 'na', 'na' )
      # retrieve the directory meta-data from the DirMetadata table
      dirList = [ dirLfn ]
      res = self.__stDB.getDirMetadata( dirList ) # this could be done in a bulk query for a list of directories... TBF
      if not res[ 'OK' ]:
        gLogger.error("Error retrieving directory meta-data %s " % res['Message'] )
        continue
      if not res['Value']:
        gLogger.warn( "No result returned for directory %s from the getDirMetadata method" % dirList )
        gLogger.info( " --> Query Bookkeeping to retrieve meta-data for %s directory and then cache them in the DirMetadata table.." % dirList ) 
        res = self.__bkClient.getDirectoryMetadata( dirLfn )
        if not res[ 'OK' ]:
          gLogger.error( "Failed to query Bookkeeping %s" %res[ 'Message' ] )
          continue
        gLogger.info( "Successfully queried Bookkeeping, result: %s " % res )
        if not res['Value']:
          gLogger.warn( "Directory is not registered in Bookkeeping! %s " % dirLfn )
          configName, configVersion, conditions, processingPass, eventType, fileType, production = ( 'na', 'na', 'na', 'na', 'na', 'na', 'na' )
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
          res = self.__stDB.insertToDirMetadata( dirMetadataDict )
          if not res[ 'OK' ]:
            gLogger.error( "Failed to insert metadata in DirMetadata table! %s " % res[ 'Message' ] )
          else:
            gLogger.info( "Successfully inserted metadata for directory %s in DirMetadata table " % dirMetadataDict )
            gLogger.verbose( "result: %s " % res )
  
      else:
        gLogger.info( "Directory %s was cached in DirMetadata table" % dirLfn )     
        for row in res['Value']:
          did, configName, configVersion, conditions, processingPass, eventType, fileType, production = row
        
      for site in traceDict[ dirLfn ].keys():
        usage = traceDict[ dirLfn ][ site ]
        gLogger.info("%s %s %d" %( dirLfn, site, usage ))
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
          self.log.notice( "ERROR: In getStorageUsage addRegister returned: %s" % res )
          continue
        self.numPopRows += 1

    gLogger.info(" %d records to be sent to Popularity accounting" %self.numPopRows )        
    res = gDataStoreClient.commit()
    if not res[ 'OK' ]:
      self.log.notice( "ERROR: committing Popularity records: %s " % res )
      return S_ERROR( res )
    else:
      gLogger.info( "%s records for Popularity type successfully committed" %self.numPopRows )
      # then set the status to Used
      gLogger.info("Set the status to Used for %d entries" % len( IdList ) )
      res = self.__stDB.updatePopEntryStatus( IdList, 'Used' )

      if not res['OK']:
        gLogger.error("Error to update status in  Popularity table.. %s" % res['Message'] )
        return S_ERROR( res['Message'] )
      else:
        gLogger.info("Status updated to Used correctly for %s entries " % res[ 'Value' ] )
  
    return S_OK()
 
