""" StorageUsageHandler is the implementation of the Storage Usage service in the DISET framework
"""
__RCSID__ = "$Id: DataIntegrityHandler.py 18161 2009-11-11 12:07:09Z acasajus $"
from DIRAC import gLogger, gConfig, rootPath, S_OK, S_ERROR

from types import *
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from LHCbDIRAC.DataManagementSystem.DB.StorageUsageDB import StorageUsageDB

# This is a global instance of the DataIntegrityDB class
storageUsageDB = False

def initializeStorageUsageHandler( serviceInfo ):

  global storageUsageDB
  storageUsageDB = StorageUsageDB()
  return S_OK()

class StorageUsageHandler( RequestHandler ):

  types_publishDirectories = [DictType]
  def export_publishDirectories( self, directoryDict ):
    return storageUsageDB.publishDirectories( directoryDict )

  types_removeDirectory = [ ( StringType, ListType, TupleType ) ]
  def export_removeDirectory( self, dirPaths ):
    if type( dirPaths ) == StringType:
      dirPaths = ( dirPaths, )
    for dirPath in dirPaths:
      result = storageUsageDB.removeDirectory( dirPath )
      if not result[ 'OK' ]:
        gLogger.error( "Could not delete directory", "%s : %s" % ( dirPath, result[ 'Message' ] ) )
        return result
    return S_OK()

  ##################################################################
  #
  # These are the methods for monitoring the usage
  #

  types_getStorageSummary = []
  def export_getStorageSummary( self, directory = '', filetype = '', production = '', sites = [] ):
    """ Retieve a summary for the storage usage
    """
    return storageUsageDB.getStorageSummary( directory, filetype, production, sites )

  types_getStorageDirectorySummary = []
  def export_getStorageDirectorySummary( self, directory = '', filetype = '', production = '', sites = [] ):
    """ Retieve a directory summary for the storage usage
    """
    result = storageUsageDB.getStorageDirectorySummary( directory, filetype, production, sites )
    if not result[ 'OK' ]:
      return result
    dl = []
    for dirPath in result[ 'Value' ]:
      dl.append( ( dirPath, result[ 'Value' ][ dirPath ][ 'Size' ], result[ 'Value' ][ dirPath ][ 'Files' ] ) )
    return S_OK( dl )


  types_getStorageDirectoryData = []
  def export_getStorageDirectoryData( self, directory = '', filetype = '', production = '', sites = [] ):
    """ Retieve a directory summary for the storage usage
    """
    return storageUsageDB.getStorageDirectorySummary( directory, filetype, production, sites )


  types_getStorageDirectories = []
  def export_getStorageDirectories( self, directory = '', filetype = '', production = '', sites = [] ):
    """ Retieve the directories for the supplied selection
    """
    return storageUsageDB.getStorageDirectories( directory, filetype, production, sites )

  types_getStorageDirectorySummaryWeb = []
  def export_getStorageDirectorySummaryWeb( self, selectDict, sortList, startItem, maxItems ):
    """ Get the summary of the directory storage summary
    """
    resultDict = {}

    # Sorting instructions. Only one for the moment.
    if sortList:
      orderAttribute = sortList[0][0] + ":" + sortList[0][1]
    else:
      orderAttribute = None

    directory = ''
    if selectDict.has_key( 'Directory' ):
      directory = selectDict['Directory']
    filetype = ''
    if selectDict.has_key( 'FileType' ):
      filetype = selectDict['FileType']
    production = ''
    if selectDict.has_key( 'Production' ):
      production = selectDict['Production']
    ses = []
    if selectDict.has_key( 'SEs' ):
      ses = selectDict['SEs']

    res = storageUsageDB.getStorageDirectorySummary( directory, filetype, production, ses )
    if not res['OK']:
      gLogger.error( "StorageUsageHandler.getStorageDirectorySummaryWeb: Failed to obtain directory summary.", res['Message'] )
      return res
    dirList = res['Value']
    dirList = [ ( path, dirList[ path ][ 'Size' ], dirList[ path ][ 'Files' ] ) for path in dirList ]

    nDirs = len( dirList )
    resultDict['TotalRecords'] = nDirs
    if nDirs == 0:
      return S_OK( resultDict )
    iniDir = startItem
    lastDir = iniDir + maxItems
    if iniDir >= nDirs:
      return S_ERROR( 'Item number out of range' )
    if lastDir > nDirs:
      lastDir = nDirs

    # prepare the extras count
    res = storageUsageDB.getStorageSummary( directory, filetype, production, ses )
    if not res['OK']:
      gLogger.error( "StorageUsageHandler.getStorageDirectorySummaryWeb: Failed to obtain usage summary.", res['Message'] )
      return res
    resultDict['Extras'] = res['Value']

    # prepare the standard structure now
    resultDict['ParameterNames'] = ['Directory Path', 'Size', 'Files']
    resultDict['Records'] = dirList[iniDir:lastDir]
    return S_OK( resultDict )

  types_getStorageElementSelection = []
  def export_getStorageElementSelection( self ):
    """ Retrieve the possible selections 
    """
    return storageUsageDB.getStorageElementSelection()

  types_getUserStorageUsage = []
  def export_getUserStorageUsage( self, userName = False ):
    """ Retieve a summary of the user usage
    """
    return storageUsageDB.getUserStorageUsage( userName )

  types_getUserSummaryPerSE = []
  def export_getUserSummaryPerSE( self, userName = False ):
    """ Retieve a summary of the user usage per SE
    """
    return storageUsageDB.getUserSummaryPerSE( userName )
  
  types_getDirectorySummaryPerSE = []
  def export_getDirectorySummaryPerSE(self, directory ):
    """Retrieve a summary (total files and total size) for a given directory, grouped by storage element """
    return storageUsageDB.getDirectorySummaryPerSE( directory )
  
  types_getIDs = []
  def export_getIDs( self, dirList ):
    """ Check if the directories exist in the su_Directory table and if yes returns the IDs 
    """
    return storageUsageDB.getIDs( dirList )

  types_getAllReplicasInFC = []
  def export_getAllReplicasInFC(self, path ):
    """ Export the DB method to query the su_seUsage table to get all the entries relative to a given path registered 
    in the FC. Returns for every replica the SE, the update, the files and the size  """
    return storageUsageDB.getAllReplicasInFC( path )
  
  ####
  # Catalog
  ####

  types_getSummary = [ StringType ]
  def export_getSummary( self, path, fileType = False, production = False ):
    return storageUsageDB.getSummary( path, fileType, production )

  types_getUserSummary = []
  def export_getUserSummary( self, userName = False ):
    return storageUsageDB.getUserSummary( userName )

  ####
  # Purge
  ####

  types_purgeOutdatedEntries = [ StringType, ( IntType, LongType ) ]
  def export_purgeOutdatedEntries( self, rootDir, outdatedSeconds, preserveDirsList = [] ):
    """ Purge entries that haven't been updated in the last outdated seconds
    """
    return storageUsageDB.purgeOutdatedEntries( rootDir, outdatedSeconds, preserveDirsList )

  ###
  # methods to deal with DarkData directory: se_DarkDirectories
  ###
  types_publishToDarkDir = []
  def export_publishToDarkDir(self, directoryDict ):
    """ Export the publishToDarkDir DB method, which inserts/updates row into the  se_DarkDirectories """
    return storageUsageDB.publishToDarkDir( directoryDict )
  ###
  # methods to deal with se_Usage table
  ###
  types_publishToSEReplicas = []
  def export_publishToSEReplicas(self, directoryDict ):
    """ Export the publishToSEReplicas DB method, which inserts/updates replicas on the SE to the se_Usage table """
    return storageUsageDB.publishToSEReplicas( directoryDict )
  
  ####
  # Tier1 SE status for web
  ####
  types_getTier1SEStatus = [ DictType, ListType, IntType, IntType ]
  def export_getTier1SEStatusWeb( self, selectDict={}, sortList=[ "SE", "DESC" ], startItem=0, maxItems=56 ):
    """get Tier1 SE status  
    
    :warning: 
    Always returning information about all T1 SEs but could be easly modified to see only few of them.

    :todo: 
    Read space tokens quota from DIRAC config.

    :param self: self reference
    :param dict SelectionDict: not used, required by the web interface
    :param list SortList: as above
    :param int StartItem: as above
    :param MaxItems: as above
    """    
    ## TB
    TB = 1000.0 * 1000.0 * 1000.0 * 1000.0
    ## space tokens quota, all values are in TB
    seQuota = {
      "CERN" : { "-RAW" : 380, "-RDST" : 325, "_M-DST"  : 350, "-DST" : 0, "_MC_M-DST" : 580, "_MC-DST" : 0, 
                 "-USER" : 205, "-HIST" : None, "-FAILOVER" : None },
      "CNAF" : { "-RAW" : 50, "-RDST" : 90, "_M-DST"  : 70, "-DST" : 240, "_MC_M-DST" : 55, "_MC-DST" : 55, 
                 "-USER" : 30, "-HIST" : None, "-FAILOVER" : None },
      "GRIDKA" : { "-RAW" : 65, "-RDST" : 125, "_M-DST"  : 90, "-DST" : 220, "_MC_M-DST" : 70, "_MC-DST" : 75, 
                   "-USER" : 40, "-HIST" : None, "-FAILOVER" : None },
      "IN2P3" : { "-RAW" : 105, "-RDST" : 195, "_M-DST"  : 145, "-DST" : 165, "_MC_M-DST" : 110, "_MC-DST" : 125, 
                  "-USER" : 65, "-HIST" : None, "-FAILOVER" : None },
      "NIKHEF" : { "-RAW" : 80, "-RDST" : 145, "_M-DST"  : 110, "-DST" : 200, "_MC_M-DST" : 85, "_MC-DST" : 115, 
                   "-USER" : 50, "-HIST" : None, "-FAILOVER" : None }, 
      "PIC" : { "-RAW" : 25, "-RDST" : 45, "_M-DST"  : 35, "-DST" : 140, "_MC_M-DST" : 25, "_MC-DST" : 25, 
                "-USER" : 15, "-HIST" : None, "-FAILOVER" : None },
      "RAL" : { "-RAW" : 70, "-RDST" : 135, "_M-DST"  : 100, "-DST" : 210, "_MC_M-DST" : 75, "_MC-DST" : 75, 
                "-USER" : 45, "-HIST" : None, "-FAILOVER" : None } }

    ## read storage usage summary
    resStorageSummary =  RPCClient("DataManagement/StorageUsage").getStorageSummary( "", "", "", [] )
    if not resStorageSummary["OK"]:
      return S_ERROR( resStorageSummary["Message"] )

    seStatus = { "ParameterNames" : [ "SE", "ReadAccess", "WriteAccess", "Used [TB]", "Quota [TB]", "Free [%]" ],
                 "Records" : [],
                 "TotalRecords" : 0,
                 "Extras" : "" }
    
    for tier1, disks in sorted(seQuota.items()):
      for seDisk in sorted(disks):
        SE = tier1 + seDisk
        readAccess = writeAccess = quota = used = free = None

        if ( SE in resStorageSummary["Value"] ):
          quota = seQuota[tier1][seDisk] 
          used =  resStorageSummary["Value"][SE]["Size"] / TB

          if quota and used:
            free = 100.0 - (used * 100.0 / quota)   
            if quota < used:
              gLogger.warn("wrong quota for %s, space used (%4.2f TB) > quota (%4.2f TB)" % ( SE, used, quota ) )
         
          cfgPath = "/Resources/StorageElements/" + SE
          res = gConfig.getOptionsDict( cfgPath )
          if not res["OK"]:
            gLogger.error("can't read config dict from path " + cfgPath )
          else:
            if "ReadAccess" in res["Value"]:
              readAccess = res["Value"]["ReadAccess"]
            if "WriteAccess" in res["Value"]:
              writeAccess = res["Value"]["WriteAccess"]
       
          rec = [ SE, 
                  readAccess if readAccess else "-", 
                  writeAccess if writeAccess else "-", 
                  "%4.2f" % used, 
                  "%4.2f" % quota if quota  else "-",
                  "%4.2f" % free if free else "-" ]
          seStatus["Records"].append( rec )
          seStatus["TotalRecords"] += 1

    return S_OK( seStatus )
    
