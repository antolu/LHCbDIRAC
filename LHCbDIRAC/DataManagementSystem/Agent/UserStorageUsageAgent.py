"""  UserStorageUsageAgent simply inherits the StorageUsage agent and loops over the /lhcb/user directory
"""
# $HeadURL: svn+ssh://svn.cern.ch/reps/dirac/DIRAC/trunk/DIRAC/DataManagementSystem/Agent/UserStorageUsageAgent.py $
__RCSID__ = "$Id: UserStorageUsageAgent.py 18161 2009-11-11 12:07:09Z acasajus $"

from DIRAC import S_OK
from LHCbDIRAC.DataManagementSystem.Agent.StorageUsageAgent import StorageUsageAgent
from DIRAC.Core.Utilities import List
import time, os
from types import *

class UserStorageUsageAgent( StorageUsageAgent ):

  def removeEmptyDir( self, dirPath ):
    #Do not remove user's home dir
    if len( List.fromChar( dirPath, "/" ) ) > 4:
      return StorageUsageAgent.removeEmptyDir( self, dirPath )
    return S_OK()

