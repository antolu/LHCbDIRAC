""" DataUsageHandler is the implementation of the Data Usage service in the DISET framework.
"""
__RCSID__ = "$Id:  $"

##
from types import *

## from DIRAC
from DIRAC import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.DISET.RequestHandler import RequestHandler

## from LHCbDIRAC
from LHCbDIRAC.DataManagementSystem.DB.StorageUsageDB import StorageUsageDB

# This is a global instance of the StorageUsageDB class
storageUsageDB = False

def initializeDataUsageHandler( serviceInfo ):

  global storageUsageDB
  storageUsageDB = StorageUsageDB()
  return S_OK()

class DataUsageHandler( RequestHandler ):
  types_sendDataUsageReport = [ ( StringType, DictType, StringType ) ]
  def export_sendDataUsageReport( self, se , directoryDict, status ='New' ):
    return storageUsageDB.sendDataUsageReport( se, directoryDict, status )

  types_getDataUsageSummary = [ ( StringType, StringType, StringType ) ]
  def export_getDataUsageSummary( self, startTime, endTime, status = 'New' ):
    return storageUsageDB.getDataUsageSummary( startTime, endTime, status )

  types_insertToDirMetadata = [ ( DictType ) ]
  def export_insertToDirMetadata( self, directoryDict ):
    return storageUsageDB.insertToDirMetadata( directoryDict )


  types_getDirMetadata = [ ( ListType ) ]
  def export_getDirMetadata( self, directoryList ):
    return storageUsageDB.getDirMetadata( directoryList )

 

 
