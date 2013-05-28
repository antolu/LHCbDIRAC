########################################################################
# File: DataUsageClient.py
########################################################################

""" :mod: DataUsageClient
    =====================

    .. module: DataUsageClient
    :synopsis: Class that contains client access to the StorageUsageDB handler. 
"""

## imports
from types import DictType, StringType, ListType
from DIRAC import S_ERROR
from DIRAC.Core.Base.Client import Client

__RCSID__ = "$Id: DataUsageClient.py 36989 2011-03-29 17:06:04Z dremensk $"

class DataUsageClient( Client ):
  """
  .. class:: DataUsageClient
  """

  def __init__( self ):
    """ c'tor """
    Client.__init__( self )
    self.setServer( 'DataManagement/DataUsage' )

  def sendDataUsageReport( self, site, directoryDict, rpc = None, url = '', timeout = 120 ):
    """ send data usage report """
    if type( directoryDict ) != DictType:
      return S_ERROR( 'Supplied dictionary is not in correct format!' )
    rpcClient = self._getRPC( rpc = rpc, url = url, timeout = timeout )
    return rpcClient.sendDataUsageReport( site, directoryDict )

  def getDataUsageSummary( self, startTime, endTime, status, rpc = None, url = '', timeout = 120 ):
    """ get usage summary """
    if ( type( startTime ) != StringType or type( endTime ) != StringType or type( status) != StringType ):
      return S_ERROR( 'Supplied arguments not in correct format!' )
    rpcClient = self._getRPC( rpc = rpc, url = url, timeout = timeout )
    return rpcClient.getDataUsageSummary( startTime, endTime, status )

  def insertToDirMetadata( self, directoryDict, url = '', timeout = 120 ):
    """ insert metadata to dir or maybe other way around """
    if type( directoryDict ) != DictType:
      return S_ERROR( 'Supplied dictionary is not in correct format!' )
    rpcClient = self._getRPC( rpc = None, url = url, timeout = timeout )
    return rpcClient.insertToDirMetadata( directoryDict )

  def getDirMetadata( self, directoryList, url = '', timeout = 120 ):
    """ get directory metadata """
    if type( directoryList ) != ListType:
      return S_ERROR( 'Supplied dictionary is not in correct format!' )
    rpcClient = self._getRPC( rpc = None, url = url, timeout = timeout )
    return rpcClient.getDirMetadata( directoryList )

  def updatePopEntryStatus( self, idList, newStatus, url = '', timeout = 120 ):
    """ whatever, pop new status """
    if type( idList ) != ListType or type( newStatus ) != StringType:
      return S_ERROR( 'Supplied dictionary is not in correct format!' )
    rpcClient = self._getRPC( rpc = None, url = url, timeout = timeout )
    return rpcClient.updatePopEntryStatus( idList, newStatus )
