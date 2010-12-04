"""  UserStorageUsageAgent simply inherits the StorageUsage agent and loops over the /lhcb/user directory
"""
# $HeadURL: svn+ssh://svn.cern.ch/reps/dirac/DIRAC/trunk/DIRAC/DataManagementSystem/Agent/UserStorageUsageAgent.py $
__RCSID__ = "$Id: UserStorageUsageAgent.py 18161 2009-11-11 12:07:09Z acasajus $"

from DIRAC  import gLogger, gMonitor, S_OK, S_ERROR, rootPath, gConfig
from DIRAC.Core.Base.AgentModule import AgentModule

from DIRAC.Core.Utilities.List import sortList

from DIRAC.DataManagementSystem.Agent.NamespaceBrowser import NamespaceBrowser
from DIRAC.DataManagementSystem.Client.ReplicaManager import CatalogDirectory

from LHCbDIRAC.DataManagementSystem.Agent.StorageUsageAgent import StorageUsageAgent

import time, os
from types import *

AGENT_NAME = 'DataManagement/UserStorageUsageAgent'

class UserStorageUsageAgent( StorageUsageAgent ):

  def initialize( self ):
    self.catalog = CatalogDirectory()
    if self.am_getOption( 'DirectDB', False ):
      from LHCbDIRAC.DataManagementSystem.DB.StorageUsageDB import StorageUsageDB
      self.StorageUsageDB = StorageUsageDB()
    else:
      from DIRAC.Core.DISET.RPCClient import RPCClient
      self.StorageUsageDB = RPCClient( 'DataManagement/StorageUsage' )

    # This sets the Default Proxy to used as that defined under 
    # /Operations/Shifter/DataManager
    # the shifterProxy option in the Configuration can be used to change this default.
    self.am_setOption( 'shifterProxy', 'DataManager' )

    return S_OK()

  def removeEmptyDir( self, directory ):
    gLogger.info( "removeEmptyDir: Attempting to remove empty directory from Storage Usage database" )
    res = self.StorageUsageDB.publishEmptyDirectory( directory )
    if not res['OK']:
      gLogger.error( "removeEmptyDir: Failed to remove empty directory from Storage Usage database.", res['Message'] )
    else:
      if len( directory.split( '/' ) ) > 5:
        res = self.catalog.removeCatalogDirectory( directory )
        if not res['OK']:
          gLogger.error( "removeEmptyDir: Failed to remove empty directory from File Catalog.", res['Message'] )
        elif res['Value']['Failed'].has_key( directory ):
          gLogger.error( "removeEmptyDir: Failed to remove empty directory from File Catalog.", res['Value']['Failed'][directory] )
        else:
          gLogger.info( "removeEmptyDir: Successfully removed empty directory from File Catalog." )
      else:
        gLogger.info( "removeEmptyDir: Not removing user base directory." )
    return S_OK()
