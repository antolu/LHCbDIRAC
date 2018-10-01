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

    self.devLbLogin = self.am_getOption('devLbLogin', False)
    self.lbRunOnly = self.am_getOption('lbRunOnly', False)

    return S_OK()

  # FIXME: Commented out because of  https://its.cern.ch/jira/browse/LHCBDIRAC-711
  # def _getTQDictForMatching(self):
  #   """ We skip the check of platforms

  #       :returns dict: tqDict of task queue descriptions
  #   """
  #   tqDict = DIRACSiteDirector._getTQDictForMatching(self)
  #   tqDict.pop('Platform', None)

  #   return tqDict

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
    if self.devLbLogin or self.lbRunOnly:
      opt = ''
      if self.devLbLogin:
        opt = 'devLbLogin'
      if self.lbRunOnly:
        opt = '.'.join([opt, 'lbRunOnly'])
      pilotOptions.append('-o %s' % opt)

    return [pilotOptions, newPilotsToSubmit]
