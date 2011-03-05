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
from DIRAC.Core.Utilities import Time
from LHCbDIRAC.AccountingSystem.Client.Types.UserStorage import UserStorage
from LHCbDIRAC.AccountingSystem.Client.Types.Storage import Storage
from DIRAC.AccountingSystem.Client.DataStoreClient import gDataStoreClient

from DIRAC  import S_OK

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
    ftb = 1000000000000.0
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

    return gDataStoreClient.commit()






