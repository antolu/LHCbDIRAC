""" LHCbScript is very similar to DIRAC Script module, but consider LHCb environment
"""

import os

from DIRAC import gLogger
from DIRAC.Workflow.Modules.Script import Script

class LHCbScript( Script ):
  """ A simple extension to the DIRAC script module
  """

  #############################################################################
  def __init__( self ):
    """ c'tor
    """
    self.log = gLogger.getSubLogger( 'LHCbScript' )
    super( LHCbScript, self ).__init__( self.log )

    self.systemConfig = 'ANY'
    self.environment = {}

  def _resolveInputVariables( self ):
    """ By convention the workflow parameters are resolved here.
    """

    super( LHCbScript, self )._resolveInputVariables()
    super( LHCbScript, self )._resolveInputStep()

    self.systemConfig = self.step_commons.get( 'SystemConfig', self.systemConfig )


  def _executeCommand( self ):
    """ Executes the self.command (uses systemCall) with CMT Config requested (if not 'ANY')
    """

    if self.systemConfig != 'ANY':
      self.environment = os.environ
      self.environment['CMTCONFIG'] = self.systemConfig

    super( LHCbScript, self )._executeCommand()
