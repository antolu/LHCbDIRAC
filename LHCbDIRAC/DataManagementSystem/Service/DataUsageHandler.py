########################################################################
# File: DataUsageHandler.py
########################################################################
""" :mod: DataUsageHandler
    ======================

    .. module: DataUsageHandler
    :synopsis: Implementation of the Data Usage service in the DISET framework.
"""
# # imports
from types import StringTypes, DictType, ListType
# # from DIRAC
from DIRAC import S_OK
from DIRAC.Core.DISET.RequestHandler import RequestHandler
# # from LHCbDIRAC
from LHCbDIRAC.DataManagementSystem.DB.StorageUsageDB import StorageUsageDB

# # RCSID
__RCSID__ = "$Id:  $"

# global instance of the StorageUsageDB class
gStorageUsageDB = False

def initializeDataUsageHandler( _serviceInfo ):
  """ service initalisation """
  global gStorageUsageDB
  gStorageUsageDB = StorageUsageDB()
  return S_OK()

class DataUsageHandler( RequestHandler ):
  """
  .. class:: DataUsageHandler
  """
  types_sendDataUsageReport = [ StringTypes, DictType, StringTypes  ]
  @staticmethod
  def export_sendDataUsageReport( site , directoryDict, status = 'New' ):
    """ export of sendDataUsageReport """
    return gStorageUsageDB.sendDataUsageReport( site, directoryDict, status )

  types_getDataUsageSummary = [ StringTypes, StringTypes, StringTypes  ]
  @staticmethod
  def export_getDataUsageSummary( startTime, endTime, status = 'New' ):
    """ export of getDataUsageSummary """
    return gStorageUsageDB.getDataUsageSummary( startTime, endTime, status )

  types_getDataUsageForDirectory = [ StringTypes ]
  @staticmethod
  def export_getDataUsageForDirectory( path ):
    """ export of getDataUsageForDirectory """
    return gStorageUsageDB.getDataUsageForDirectory( path )

  types_sendDataUsageReport_2 = [ ( DictType ) ]
  @staticmethod
  def export_sendDataUsageReport_2( directoryDict ):
    """ export of sendDataUsageReport (new version) """
    return gStorageUsageDB.sendDataUsageReport_2( directoryDict )

  types_updatePopEntryStatus = [ ListType, StringTypes ]
  @staticmethod
  def export_updatePopEntryStatus( idList, newStatus ):
    """ export of updatePopEntryStatus """
    return gStorageUsageDB.updatePopEntryStatus( idList, newStatus )

  types_insertToDirMetadata = [ DictType ]
  @staticmethod
  def export_insertToDirMetadata( directoryDict ):
    """ export of insertToDirMetadata """
    return gStorageUsageDB.insertToDirMetadata( directoryDict )

  types_getDirMetadata = [ ListType  ]
  @staticmethod
  def export_getDirMetadata( directoryList ):
    """ export of getDirMetadata """
    return gStorageUsageDB.getDirMetadata( directoryList )




