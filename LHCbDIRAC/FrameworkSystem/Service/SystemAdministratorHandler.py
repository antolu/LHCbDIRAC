""" SystemAdministrator service is a tool to control and monitor the DIRAC services and agents
"""

__RCSID__ = "$Id$"

from types import StringTypes
from DIRAC.FrameworkSystem.Service.SystemAdministratorHandler import SystemAdministratorHandler as DIRACSystemAdministratorHandler
from LHCbDIRAC.FrameworkSystem.Client.ComponentInstaller import gComponentInstaller

class SystemAdministratorHandler( DIRACSystemAdministratorHandler ):

  types_startComponent = [ StringTypes, StringTypes ]
  def export_startComponent( self, system, component ):
    """ Start the specified component, running with the runsv daemon
    """
    return gComponentInstaller.runsvctrlComponent( system, component, 'u' )

  types_stopComponent = [ StringTypes, StringTypes ]
  def export_stopComponent( self, system, component ):
    """ Stop the specified component, running with the runsv daemon
    """
    return gComponentInstaller.runsvctrlComponent( system, component, 'd' )

  types_restartComponent = [ StringTypes, StringTypes ]
  def export_restartComponent( self, system, component ):
    """ Restart the specified component, running with the runsv daemon
    """
    return gComponentInstaller.runsvctrlComponent( system, component, 't' )
