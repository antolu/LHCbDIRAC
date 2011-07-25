###################################################################################################
# $HeadURL$
###################################################################################################

""" StorageUsageHandler is the implementation of the Storage Usage service in the DISET framework.
"""
__RCSID__ = "$Id$"

##
from types import *

## from DIRAC
from DIRAC import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.Resources.Storage.StorageElement import StorageElement

## from LHCbDIRAC
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

  types_removeDirFromSe_Usage = []
  def export_removeDirFromSe_Usage( self, dirPaths ):
    """ Exports the method to remove entries from the se_Usage table """
    return storageUsageDB.removeDirFromSe_Usage( dirPaths )

  types_removeDirFromProblematicDirs = []
  def export_removeDirFromProblematicDirs( self, dirPaths ):
    """ Exports the method to remove entries from the problematicDirs table """
    return storageUsageDB.removeDirFromProblematicDirs( dirPaths )

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
    """ Retrieve a directory summary for the storage usage
    """
    return storageUsageDB.getStorageDirectorySummary( directory, filetype, production, sites )


  types_getStorageDirectories = []
  def export_getStorageDirectories( self, directory = '', filetype = '', production = '', sites = [] ):
    """ Retrieve the directories for the supplied selection
    """
    return storageUsageDB.getStorageDirectories( directory, filetype, production, sites )

  types_getStorageDirectorySummaryWeb = []
  def export_getStorageDirectorySummaryWeb( self, selectDict, sortList, startItem, maxItems ):
    """ Get the summary of the directory storage summary
    """
    resultDict = {}

    # Sorting instructions. Only one for the moment.

    directory = ''
    if "Directory" in selectDict:
      directory = selectDict['Directory']
    filetype = ''
    if "FileType" in selectDict:
      filetype = selectDict['FileType']
    production = ''
    if "Production" in selectDict:
      production = selectDict['Production']
    ses = []
    if "SEs" in selectDict:
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
    """ Retrieve a summary of the user usage
    """
    return storageUsageDB.getUserStorageUsage( userName )

  types_getUserSummaryPerSE = []
  def export_getUserSummaryPerSE( self, userName = False ):
    """ Retrieve a summary of the user usage per SE
    """
    return storageUsageDB.getUserSummaryPerSE( userName )

  types_getDirectorySummaryPerSE = []
  def export_getDirectorySummaryPerSE( self, directory ):
    """Retrieve a summary (total files and total size) for a given directory, grouped by storage element """
    return storageUsageDB.getDirectorySummaryPerSE( directory )

  types_getRunSummaryPerSE = []
  def export_getRunSummaryPerSE( self, run ):
    """Retrieve a summary (total files and total size) for a given run, grouped by storage element """
    return storageUsageDB.getRunSummaryPerSE( run )

  types_getIDs = []
  def export_getIDs( self, dirList ):
    """ Check if the directories exist in the su_Directory table and if yes returns the IDs
    """
    return storageUsageDB.getIDs( dirList )

  types_getAllReplicasInFC = []
  def export_getAllReplicasInFC( self, path ):
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
  # methods to deal with problematicDirs directory: problematicDirs
  ###
  types_publishToProblematicDirs = []
  def export_publishToProblematicDirs( self, directoryDict ):
    """ Export the publishToProblematicDirs DB method, which inserts/updates row into the  problematicDirs """
    return storageUsageDB.publishToProblematicDirs( directoryDict )
  ###
  # methods to deal with se_Usage table
  ###
  types_publishToSEReplicas = []
  def export_publishToSEReplicas( self, directoryDict ):
    """ Export the publishToSEReplicas DB method, which inserts/updates replicas on the SE to the se_Usage table """
    return storageUsageDB.publishToSEReplicas( directoryDict )

  ####
  # Tier1 SE status for web
  ####
  types_getTier1SEStatusWeb = [ DictType, ListType, IntType, IntType ]
  def export_getTier1SEStatusWeb( self, selectDict = {}, sortList = [ "SE", "DESC" ], startItem = 0, maxItems = 56 ):
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
    res = gConfig.getOptionsDict( "/Resources/StorageElementGroups" )
    if not res["OK"]:
      return S_ERROR( res["Message"] )
    tier1SEs = list()
    for seStr in [ seStr for seGroup, seStr in res["Value"].items() if seGroup.startswith( "Tier1" ) ]:
      tier1SEs += [ se.strip() for se in seStr.split( "," ) if not se.endswith( "-disk" ) ]
    SEs = { "ParameterNames" : [ "SE", "ReadAccess", "WriteAccess", "Used", "Quota", "Free" ],
            "Records" : [],
            "TotalRecords" : 0,
            "Extras" : "" }
    for seName in sorted( tier1SEs ):
      storageElement = StorageElement( seName )
      if not storageElement.valid:
        gLogger.error( "invalid StorageElement '" + seName + "' reason: " + storageElement.errorReason )
      else:
        seStatus = storageElement.getStatus()
        if not seStatus["OK"]:
          return S_ERROR( seStatus["Message"] )
        seStatus = seStatus["Value"]
        seFree = seStatus["DiskCacheTB"] * 100.0 / seStatus["TotalCapacityTB"]

        SEs["Records"].append( [ seName,
                                 "Active" if seStatus["Read"] else "InActive",
                                 "Active" if seStatus["Write"] else "InActive",
                                 "%4.2f" % seStatus["DiskCacheTB"],
                                 "%4.2f" % seStatus["TotalCapacityTB"],
                                 "%4.2f" % seFree ] )
        SEs["TotalRecords"] += 1
    return S_OK( SEs )

