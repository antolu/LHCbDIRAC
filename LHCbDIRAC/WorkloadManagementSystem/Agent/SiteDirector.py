""" Extension of DIRAC SiteDirector. Simply defines what to send.
"""

__RCSID__ = "$Id$"

import os

from DIRAC import S_OK, rootPath
from DIRAC.WorkloadManagementSystem.Agent.SiteDirector import SiteDirector as DIRACSiteDirector


class SiteDirector(DIRACSiteDirector):
  """ Simple extension of the DIRAC site director to send LHCb specific pilots (with a custom list of commands)
  """

  def beginExecution(self):
    """ just simple redefinition
    """
    res = DIRACSiteDirector.beginExecution(self)
    if not res['OK']:
      return res

    if not self.pilot3:
      self.pilotFiles = [os.path.join(rootPath,
                                      'DIRAC', 'WorkloadManagementSystem', 'PilotAgent', 'dirac-pilot.py'),
                         os.path.join(rootPath,
                                      'DIRAC', 'WorkloadManagementSystem', 'PilotAgent', 'pilotCommands.py'),
                         os.path.join(rootPath,
                                      'DIRAC', 'WorkloadManagementSystem', 'PilotAgent', 'pilotTools.py'),
                         os.path.join(rootPath,
                                      'LHCbDIRAC', 'WorkloadManagementSystem', 'PilotAgent', 'LHCbPilotCommands.py')]

    self.lbRunOnly = self.am_getOption('lbRunOnly', False)

    return S_OK()

  def _getPilotOptions(self, queue, pilotsToSubmit):
    """ Adding LHCb specific options
    """
    pilotOptions, newPilotsToSubmit = DIRACSiteDirector._getPilotOptions(self, queue, pilotsToSubmit)

    lhcbPilotCommands = ['LHCbGetPilotVersion',
                         'CheckWorkerNode',
                         'LHCbInstallDIRAC',
                         'LHCbConfigureBasics',
                         'CheckCECapabilities',
                         'CheckWNCapabilities',
                         'LHCbConfigureSite',
                         'LHCbConfigureArchitecture',
                         'LHCbConfigureCPURequirements',
                         'LaunchAgent']

    pilotOptions.append('-E LHCbPilot')
    pilotOptions.append('-X %s' % ','.join(lhcbPilotCommands))
    if self.lbRunOnly:
      pilotOptions.append('-o lbRunOnly')

    return [pilotOptions, newPilotsToSubmit]
