''' UpdateRemotePilotFileHandler

  Module that update the pilot file according to the update of the CS information.

'''

from DIRAC                                        import gConfig, S_OK
from DIRAC.Core.DISET.RequestHandler              import RequestHandler
from LHCbDIRAC.WorkloadManagementSystem.Utilities import pilotSynchronizer


__RCSID__ = '$Id: $'


def initializeUpdateRemotePilotFileHandler( _serviceInfo ):
  '''
    Handler initialization, where we set the ResourceManagementDB as global db.
  '''


  syncObject = pilotSynchronizer.pilotSynchronizer()
  gConfig.addListenerToNewVersionEvent( syncObject.sync )
  return S_OK()

class UpdateRemotePilotFileHandler( RequestHandler ):

  def __init__( self, *args, **kwargs ):
    super( UpdateRemotePilotFileHandler, self ).__init__( *args, **kwargs )
