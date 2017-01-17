""" LHCbScript is very similar to DIRAC Script module, but consider LHCb environment
"""

from DIRAC import gLogger
from DIRAC.Workflow.Modules.Script import Script

from LHCbDIRAC.Core.Utilities.ProductionEnvironment import getCMTConfig, getScriptsLocation, runEnvironmentScripts, addCommandDefaults

class LHCbScript( Script ):
  """ A dimple extension to DIRAC script module
  """

  #############################################################################
  def __init__( self ):
    """ c'tor
    """
    self.log = gLogger.getSubLogger( 'LHCbScript' )
    super( LHCbScript, self ).__init__( self.log )

    self.systemConfig = 'ANY'

  def _resolveInputVariables( self ):
    """ By convention the workflow parameters are resolved here.
    """

    super( LHCbScript, self )._resolveInputVariables()
    super( LHCbScript, self )._resolveInputStep()

    self.systemConfig = self.step_commons.get( 'SystemConfig', self.systemConfig )


  def _executeCommand( self ):
    """ Executes the self.command (uses shellCall) inside environment of LbLogin,
        with CMT Config requested (if not 'ANY')
    """

    # First, getting lbLogin location, and run it
    result = getScriptsLocation()
    if not result['OK']:
      return result
    lbLogin = result['Value']['LbLogin.sh']
    lbLoginEnv = runEnvironmentScripts( [lbLogin] )
    if not lbLoginEnv['OK']:
      raise RuntimeError( lbLoginEnv['Message'] )
    self.environment = lbLoginEnv['Value']

    if self.systemConfig != 'ANY':
      self.environment['CMTCONFIG'] = self.systemConfig

    super( LHCbScript, self )._executeCommand()
