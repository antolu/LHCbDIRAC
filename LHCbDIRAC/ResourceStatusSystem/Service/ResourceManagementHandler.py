# $HeadURL $
""" ResourceManagementHandler

  Module that allows users to access the LHCbDIRAC ResourceManagementDB remotely.

"""

# DIRAC
from DIRAC                                                        import gConfig, S_OK
from DIRAC.ResourceStatusSystem.Service.ResourceManagementHandler import ResourceManagementHandler \
     as DIRACResourceManagementHandler

# LHCbDIRAC
from LHCbDIRAC.ResourceStatusSystem.DB.ResourceManagementDB import ResourceManagementDB
from LHCbDIRAC.ResourceStatusSystem.Utilities               import Synchronizer

__RCSID__ = "$Id$"
db        = False

def initializeResourceManagementHandler( _serviceInfo ):
  """
    Handler initialization, where we set the ResourceManagementDB as global db.
  """

  global db
  db = ResourceManagementDB()

  # Regenerates DB tables if needed
  db._checkTable()

  syncObject = Synchronizer.Synchronizer()
  gConfig.addListenerToNewVersionEvent( syncObject.sync )

  return S_OK()

class ResourceManagementHandler( DIRACResourceManagementHandler ):
  """
  We need this service to point the server to the right RSS database code.
  In this case, the LHCbDIRAC DB extension. Otherwise, we will be missing
  the tables defined on the LHCb extension.
  """

  def __init__( self, *args, **kwargs ):
    """
    constructor, setting the global db to the LHCbDIRAC.ResourceManagementDB
    """
    super( ResourceManagementHandler, self ).__init__( *args, **kwargs )
    global db
    self.setDatabase( db )

#...............................................................................
#EOF
