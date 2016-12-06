"""
  Module that update the pilot json file according to the update of the CS information.
  It also uploads the last version of the pilot scripts to the web server defined in the dirac.cfg.
"""

from DIRAC                                        import gConfig, S_OK
from DIRAC.Core.DISET.RequestHandler              import RequestHandler, getServiceOption
from LHCbDIRAC.WorkloadManagementSystem.Utilities import pilotSynchronizer


__RCSID__ = '$Id: $'


def initializeUpdateRemotePilotFileHandler( _serviceInfo ):
  '''
    Handler initialization.
    The service options need to be defined in the dirac.cfg.
  '''

  paramDict = {}
  paramDict['pilotFileServer'] = getServiceOption( _serviceInfo, "pilotFileServer", '' )
  paramDict['pilotRepo'] = getServiceOption( _serviceInfo, "pilotRepo", '' )
  paramDict['pilotVORepo'] = getServiceOption( _serviceInfo, "pilotVORepo", '' )
  paramDict['projectDir'] = getServiceOption( _serviceInfo, "projectDir", '' )
  paramDict['pilotVOScriptPath'] = getServiceOption( _serviceInfo, "pilotVOScriptPath", '' )
  paramDict['pilotScriptsPath'] = getServiceOption( _serviceInfo, "pilotScriptsPath", '' )
  syncObject = pilotSynchronizer.pilotSynchronizer( paramDict )
  gConfig.addListenerToNewVersionEvent( syncObject.sync )
  return S_OK()

class UpdateRemotePilotFileHandler( RequestHandler ):

  def __init__( self, *args, **kwargs ):
    super( UpdateRemotePilotFileHandler, self ).__init__( *args, **kwargs )
