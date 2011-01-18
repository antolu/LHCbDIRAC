########################################################################
# $HeadURL: svn+ssh://svn.cern.ch/reps/dirac/LHCbDIRAC/trunk/LHCbDIRAC/WorkloadManagementSystem/Agent/BKInputDataAgent.py $
# File :   StorageSummaryAgent.py
########################################################################

"""   The Storage Summary Agent will create a summary of the 
      storage usage DB grouped by processing pass or other 
      interesting parameters.
      
      Initially this will dump the information to a file but eventually
      can be inserted in a new DB table and made visible via the web portal.
"""

__RCSID__ = "$Id: StorageSummaryAgent.py 31247 2010-12-04 10:32:34Z rgracian $"

from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.Core.Utilities import List, Time
from LHCbDIRAC.DataManagementSystem.DB.StorageUsageDB import StorageUsageDB
from LHCbDIRAC.AccountingSystem.Client.Types.UserStorage import UserStorage
from DIRAC.AccountingSystem.Client.DataStoreClient import gDataStoreClient

from DIRAC  import gConfig, S_OK, S_ERROR

class StorageHistoryAgent( AgentModule ):

  def initialize( self ):
    """Sets defaults
    """
    self.am_setOption( 'PollingTime', 43200 )
    self.__stDB = StorageUsageDB()
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
    self.log.notice( "Sending %s records to accounting" % numRows )

    return gDataStoreClient.commit()






