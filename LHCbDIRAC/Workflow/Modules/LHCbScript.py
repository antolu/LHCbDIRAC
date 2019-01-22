""" LHCbScript is very similar to DIRAC Script module, but consider LHCb environment
"""

import os

from DIRAC import gLogger
from DIRAC.Core.Utilities import DErrno
from DIRAC.Workflow.Modules.Script import Script

from LHCbDIRAC.Core.Utilities.RunApplication import LbRunError


class LHCbScript(Script):
  """ A simple extension to the DIRAC script module
  """

  #############################################################################
  def __init__(self):
    """ c'tor
    """
    self.log = gLogger.getSubLogger('LHCbScript')
    super(LHCbScript, self).__init__(self.log)

    self.systemConfig = 'ANY'
    self.environment = {}

  def _resolveInputVariables(self):
    """ By convention the workflow parameters are resolved here.
    """

    super(LHCbScript, self)._resolveInputVariables()
    super(LHCbScript, self)._resolveInputStep()

    self.systemConfig = self.step_commons.get('SystemConfig', self.systemConfig)

  def _executeCommand(self):
    """ Executes the self.command (uses systemCall) with binary tag (CMTCONFIG) requested (if not 'ANY')
    """

    if self.systemConfig != 'ANY':
      self.environment = os.environ
      self.environment['CMTCONFIG'] = self.systemConfig

    super(LHCbScript, self)._executeCommand()

  def _exitWithError(self, status):
    """ Extended here for treating case of lb-run error codes (and executable name).
    """
    # this is an lb-run specific error, available from LbScripts v9r1p8
    if status & 0x40 and not status & 0x80:
      self.log.error("Status %d is an lb-run specific error" % status)
      raise LbRunError("Problem setting the environment: lb-run exited with status %d" % status,
                       DErrno.EWMSRESC)
    else:
      super(LHCbScript, self)._exitWithError(status)
