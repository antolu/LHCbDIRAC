""" :mod: DataUsageClient
    =====================

    .. module: DataUsageClient
    :synopsis: Class that contains client access to the StorageUsageDB handler.
"""

# # imports
from DIRAC import S_ERROR
from DIRAC.Core.Base.Client import Client

__RCSID__ = "$Id$"

class DataUsageClient( Client ):
  """
  .. class:: DataUsageClient
  """

  def __init__( self ):
    """ c'tor """
    Client.__init__( self )
    self.setServer( 'DataManagement/DataUsage' )

  def sendDataUsageReport( self, site, directoryDict, status = 'New', rpc = None, url = '', timeout = 120 ):
    """ send data usage report """
    if not isinstance( directoryDict, dict ):
      return S_ERROR( 'Supplied dictionary is not in correct format!' )
    rpcClient = self._getRPC( rpc = rpc, url = url, timeout = timeout )
    return rpcClient.sendDataUsageReport( site, directoryDict, status )

  def getDataUsageSummary( self, startTime, endTime, status, rpc = None, url = '', timeout = 120 ):
    """ get usage summary """
    if  not ( isinstance( startTime, basestring ) and isinstance( endTime, basestring ) and isinstance( status, basestring ) ):
      return S_ERROR( 'Supplied arguments not in correct format!' )
    rpcClient = self._getRPC( rpc = rpc, url = url, timeout = timeout )
    return rpcClient.getDataUsageSummary( startTime, endTime, status )

  def insertToDirMetadata( self, directoryDict, url = '', timeout = 120 ):
    """ insert metadata to dir or maybe other way around """
    if not isinstance( directoryDict, dict ):
      return S_ERROR( 'Supplied dictionary is not in correct format!' )
    rpcClient = self._getRPC( rpc = None, url = url, timeout = timeout )
    return rpcClient.insertToDirMetadata( directoryDict )

  def getDirMetadata( self, directoryList, url = '', timeout = 120 ):
    """ get directory metadata """
    if isinstance( directoryList, basestring ):
      directoryList = [directoryList]
    elif isinstance( directoryList, ( set, tuple, dict ) ):
      directoryList = list( directoryList )
    elif not isinstance( directoryList, list ):
      return S_ERROR( 'Supplied argument is not in correct format!' )
    rpcClient = self._getRPC( rpc = None, url = url, timeout = timeout )
    return rpcClient.getDirMetadata( directoryList )

  def updatePopEntryStatus( self, idList, newStatus, url = '', timeout = 120 ):
    """ whatever, pop new status """
    if not isinstance( idList, list ) or not isinstance( newStatus, basestring ):
      return S_ERROR( 'Supplied arguments are not in correct format!' )
    rpcClient = self._getRPC( rpc = None, url = url, timeout = timeout )
    return rpcClient.updatePopEntryStatus( idList, newStatus )
