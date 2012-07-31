########################################################################
# $HeadURL $
# File: DataUsageHandler.py
########################################################################
""" :mod: DataUsageHandler
    ======================
 
    .. module: DataUsageHandler
    :synopsis: Implementation of the Data Usage service in the DISET framework.
"""
## imports
from types import StringType, DictType, ListType
## from DIRAC
from DIRAC import S_OK
from DIRAC.Core.DISET.RequestHandler import RequestHandler
## from LHCbDIRAC
from LHCbDIRAC.DataManagementSystem.DB.StorageUsageDB import StorageUsageDB

## RCSID
__RCSID__ = "$Id:  $"

# global instance of the StorageUsageDB class
gStorageUsageDB = False

def initializeDataUsageHandler( serviceInfo ):
  """ service initalisation """
  global gStorageUsageDB
  gStorageUsageDB = StorageUsageDB()
  return S_OK()

class DataUsageHandler( RequestHandler ):
  """
  .. class:: DataUsageHandler
  """
  types_sendDataUsageReport = [ ( StringType, DictType, StringType ) ]
  def export_sendDataUsageReport( self, site , directoryDict, status ='New' ):
    """ export of sendDataUsageReport """
    return gStorageUsageDB.sendDataUsageReport( site, directoryDict, status )

  types_getDataUsageSummary = [ ( StringType, StringType, StringType ) ]
  def export_getDataUsageSummary( self, startTime, endTime, status = 'New' ):
    """ export of getDataUsageSummary """
    return gStorageUsageDB.getDataUsageSummary( startTime, endTime, status )

  types_updatePopEntryStatus = [ ( ListType, StringType ) ]
  def export_updatePopEntryStatus( self, idList, newStatus ):
    """ export of updatePopEntryStatus """
    return gStorageUsageDB.updatePopEntryStatus( idList, newStatus )

  types_insertToDirMetadata = [ ( DictType ) ]
  def export_insertToDirMetadata( self, directoryDict ):
    """ export of insertToDirMetadata """
    return gStorageUsageDB.insertToDirMetadata( directoryDict )

  types_getDirMetadata = [ ( ListType ) ]
  def export_getDirMetadata( self, directoryList ):
    """ export of getDirMetadata """
    return gStorageUsageDB.getDirMetadata( directoryList )

 

 
