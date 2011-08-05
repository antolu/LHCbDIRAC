"""  UserStorageUsageAgent simply inherits the StorageUsage agent and loops over the /lhcb/user directory
"""
# $HeadURL$
__RCSID__ = "$Id$"

from DIRAC import S_OK
from LHCbDIRAC.DataManagementSystem.Agent.StorageUsageAgent import StorageUsageAgent
from DIRAC.Core.Utilities import List
from types import *

class UserStorageUsageAgent( StorageUsageAgent ):

  def removeEmptyDir( self, dirPath ):
    #Do not remove user's home dir
    if len( List.fromChar( dirPath, "/" ) ) > 4:
      return StorageUsageAgent.removeEmptyDir( self, dirPath )
    return S_OK()

