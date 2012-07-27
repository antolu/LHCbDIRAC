# $HeadURL:  $
''' ResourceManagementDB module, extension of the DIRAC one.

  Module that extends basic methods to access the ResourceManagementDB.

'''

from DIRAC.ResourceStatusSystem.DB.ResourceManagementDB import \
  ResourceManagementDB as DIRACResourceManagementDB
  
__RCSID__ = '$Id:  $'

class ResourceManagementDB( DIRACResourceManagementDB ):
  
  _tablesDB    = DIRACResourceManagementDB._tablesDB
  _tablesLike  = DIRACResourceManagementDB._tablesLike
  _likeToTable = DIRACResourceManagementDB._likeToTable
  
  def __init__( self, maxQueueSize = 10, mySQL = None ):
    
    super( ResourceManagementDB, self ).__init__( maxQueueSize, mySQL )
    
    
  
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF  