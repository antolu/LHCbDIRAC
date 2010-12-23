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

  types_removeDirectory = [StringType]
  def export_removeDirectory( self, dirPath ):
    return storageUsageDB.removeDirectory( dirPath )

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
  def export_purgeOutdatedEntries( self, rootDir, outdatedSeconds ):
    """ Purge entries that haven't been updated in the last outdated seconds
    """
    return storageUsageDB.purgeOutdatedEntries( rootDir, outdatedSeconds )

