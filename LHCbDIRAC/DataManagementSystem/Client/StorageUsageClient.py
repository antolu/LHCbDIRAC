########################################################################
# File:  StorageUsageClient.py
########################################################################
""" :mod: StorageUsageClient
    ========================
 
    .. module: StorageUsageClient
    :synopsis: Lightweight possbile client to the StorageUsageDB.
"""

__RCSID__ = "$Id$"

from DIRAC.Core.Base.Client import Client

class StorageUsageClient( Client ):
  """
  .. class:: StorageUsageClient
  """
  def __init__( self ):
    """ c'tor """
    Client.__init__( self )
    self.setServer( 'DataManagement/StorageUsage' )
