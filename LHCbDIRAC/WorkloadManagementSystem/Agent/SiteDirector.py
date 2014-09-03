""" Extension of DIRAC SiteDirector. Simply defines what to send.
"""

__RCSID__ = "$Id$"

import os

from DIRAC import S_OK, rootPath
from DIRAC.WorkloadManagementSystem.Agent.SiteDirector import SiteDirector as DIRACSiteDirector

DIRAC_MODULES = [ os.path.join( rootPath, 'DIRAC', 'WorkloadManagementSystem', 'PilotAgent', 'pilotCommands.py' ),
                  os.path.join( rootPath, 'DIRAC', 'WorkloadManagementSystem', 'PilotAgent', 'pilotTools.py' ),
                  os.path.join( rootPath, 'LHCbDIRAC', 'WorkloadManagementSystem', 'PilotAgent', 'LHCbPilotCommands.py' ) ]


class SiteDirector( DIRACSiteDirector ):
  """ Simple extension of the DIRAC site director to send LHCb specific pilots (with a custom list of commands)
  """
  
  def beginExecution(self):
    """ just simple redefinition
    """
    res = DIRACSiteDirector.beginExecution( self )
    if not res['OK']:
      return res

    self.extraModules = self.am_getOption( 'ExtraPilotModules', [] ) + DIRAC_MODULES

    return S_OK()

  def _getPilotOptions( self, queue, pilotsToSubmit ):
    """ Adding LHCb specific options
    """
    pilotOptions, newPilotsToSubmit = DIRACSiteDirector._getPilotOptions( self, queue, pilotsToSubmit )

    pilotOptions.append( '-E LHCbPilot' )
    pilotOptions.append( '-X GetLHCbPilotVersion,InstallLHCbDIRAC,ConfigureDIRAC,ConfigureLHCbArchitecture,LaunchAgent' )

    return [pilotOptions, newPilotsToSubmit]
