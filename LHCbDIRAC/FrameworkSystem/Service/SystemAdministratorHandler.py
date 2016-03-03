""" SystemAdministrator service is a tool to control and monitor the DIRAC services and agents
"""

__RCSID__ = "$Id$"

from types import StringTypes
from DIRAC.FrameworkSystem.Service.SystemAdministratorHandler import SystemAdministratorHandler as DIRACSystemAdministratorHandler
from LHCbDIRAC.FrameworkSystem.Client.ComponentInstaller import gComponentInstaller

class SystemAdministratorHandler( DIRACSystemAdministratorHandler ):

  types_startService = [ StringTypes ]
  def export_startService( self, service ):
    """ Start the specified service
    """
    return gComponentInstaller.runsvctrlComponent( service )

  types_stopService = [ StringTypes ]
  def export_stopService( self, service ):
    """ Stop the specified service
    """
    return gComponentInstaller.stopService( service )

  types_restartService = [ StringTypes ]
  def export_restartService( self, service ):
    """ Restart the specified service
    """
    return gComponentInstaller.restartService( service )

  types_statusService = [ StringTypes ]
  def export_statusService( self, service ):
    """ Check the status of the specified service
    """
    return gComponentInstaller.statusService( service )
