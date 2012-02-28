""" Class that contains client access to the StorageUsageDB handler. """
########################################################################
# $Id: StorageManagerClient.py 36989 2011-03-29 17:06:04Z sposs $
# $HeadURL: http://svn.cern.ch/guest/dirac/DIRAC/tags/DIRAC/v5r13p43/StorageManagementSystem/Client/StorageManagerClient.py $
########################################################################
__RCSID__ = "$Id: DataUsageClient.py 36989 2011-03-29 17:06:04Z dremensk $"

from DIRAC                                          import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.Base.Client                         import Client
from DIRAC.Core.DISET.RPCClient                     import RPCClient
from DIRAC.Core.Utilities.List                      import breakListIntoChunks
import types
from types import *

rpc = None
url = None

class DataUsageClient( Client ):

  def __init__( self ):
    self.setServer( 'DataManagement/DataUsage' )

  def sendDataUsageReport( self, site, directoryDict, rpc = '', url = '', timeout = 120 ):
    if type( directoryDict ) != types.DictType:
      return S_ERROR( 'Supplied dictionary is not in correct format!' )
    rpcClient = self._getRPC( rpc = rpc, url = url, timeout = timeout )
    return rpcClient.sendDataUsageReport( site, directoryDict )

  def getDataUsageSummary( self, startTime, endTime, rpc = '', url = '', timeout = 120 ):
    if ( type( startTime ) != StringType or type( endTime ) != StringType ):
      return S_ERROR( 'Supplied arguments not in correct format!' )
    rpcClient = self._getRPC( rpc = rpc, url = url, timeout = timeout )
    return rpcClient.getDataUsageSummary( startTime, endTime )

  def getDataUsageSummary_2( self, startTime, endTime, rpc = '', url = '', timeout = 120 ):
    if ( type( startTime ) != StringType or type( endTime ) != StringType ):
      return S_ERROR( 'Supplied arguments not in correct format!' )
    rpcClient = self._getRPC( rpc = rpc, url = url, timeout = timeout )
    return rpcClient.getDataUsageSummary_2( startTime, endTime )

  def insertToDirMetadata( self, directoryDict, url = '', timeout = 120 ):
    if type( directoryDict ) != types.DictType:
      return S_ERROR( 'Supplied dictionary is not in correct format!' )
    rpcClient = self._getRPC( rpc = rpc, url = url, timeout = timeout )
    return rpcClient.insertToDirMetadata( directoryDict )

  def getDirMetadata( self, directoryList, url = '', timeout = 120 ):
    if type( directoryList ) != types.ListType:
      return S_ERROR( 'Supplied dictionary is not in correct format!' )
    rpcClient = self._getRPC( rpc = rpc, url = url, timeout = timeout )
    return rpcClient.getDirMetadata( directoryList )

  def updatePopEntryStatus( self, IdList, newStatus, url = '', timeout = 120 ):
    if type( IdList ) != types.ListType or type( newStatus ) != types.StringType:
      return S_ERROR( 'Supplied dictionary is not in correct format!' )
    rpcClient = self._getRPC( rpc = rpc, url = url, timeout = timeout )
    return rpcClient.updatePopEntryStatus( IdList, newStatus )
