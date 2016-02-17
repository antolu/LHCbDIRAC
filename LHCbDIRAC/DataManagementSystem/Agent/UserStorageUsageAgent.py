""" :mod: UserStorageUsageAgent
    ===========================

    .. module: UserStorageUsageAgent
    :synopsis: UserStorageUsageAgent simply inherits the StorageUsage agent
    and loops over the /lhcb/user directory, removing empty ones.
"""

# # imports
from DIRAC import S_OK
from LHCbDIRAC.DataManagementSystem.Agent.StorageUsageAgent import StorageUsageAgent
from DIRAC.Core.Utilities import List

__RCSID__ = "$Id$"

class UserStorageUsageAgent( StorageUsageAgent ):
  """
  .. class:: UserStorageUsageAgent

  """
  def removeEmptyDir( self, dirPath ):
    """ remove empty directories, but skip home

    :param self: self reference
    :param str dirPath: directory to remove
    """
    # Do not remove user's home dir
    if len( List.fromChar( dirPath, "/" ) ) > 4:
      return StorageUsageAgent.removeEmptyDir( self, dirPath )
    return S_OK()

