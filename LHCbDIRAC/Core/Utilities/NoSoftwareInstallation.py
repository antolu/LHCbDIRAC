""" NoSoftwareInstallation

    An instance of this module is created by the ModuleFactory. It just makes few basic checks
    This relies on JDL parameters in LHCb Workflows "Platform" to determine the required value of CMTCONFIG

    DIRAC assumes an execute() method will exist during usage.
"""


from DIRAC import gConfig, gLogger, S_ERROR, S_OK
from LHCbDIRAC.Core.Utilities import ProductionEnvironment

__RCSID__ = "$Id$"


class NoSoftwareInstallation( object ):
  """ This class is a temporary solution until a proper fix is set at the level
      of DIRAC. It's main objective is to replace the class CombinedSoftwareInstallation.
      At the moment, it is set on the CS. However, if we clear that entry on the CS
      the JobAgent will crash as it is expecting something. That something is this class.
  """

  def __init__( self, argumentsDict ):
    """ Constructor
    """

    self.log = gLogger.getSubLogger( self.__class__.__name__ )
    self.job = argumentsDict.get( 'Job', {} )

  def execute( self ):
    """ Main method of the class executed by DIRAC JobAgent. It checks the parameters
        in case there is a mis-configuration and returns S_OK / S_ERROR.
    """

    # platform ...........................................................
    if 'Platform' in self.job:
      platform = self.job['Platform']
    else:
      platform = 'ANY'

    # localArch ...........................................................
    localArch = gConfig.getValue( '/LocalSite/Architecture', '' )
    if not localArch:
      raise RuntimeError( "/LocalSite/Architecture is missing and must be specified" )

    if platform.lower() == 'any':
      self.log.info( "Platform requested by the job is set to 'ANY', using the local platform" )
    else:
      self.log.info( "Platform requested by the job is set to '%s'" % platform )
      if not ProductionEnvironment.getPlatformsCompatibilities( platform, localArch ):
        self.log.error( "The platform request is different from the local one... something is very wrong!" )
        return S_ERROR( "The platform request is different from the local one... something is very wrong!" )

    compatibleConfigs = self._getSupportedConfigs( localArch )
    self.log.info( "This node supports the following configs: %s" % ', '.join( compatibleConfigs ) )

    return S_OK()


  def _getSupportedConfigs( self, platform ):
    """ returns getLHCbConfigsForPlatform
    """
    self.log.info( "Node supported platform is: %s" % platform )
    compatibleConfigs = ProductionEnvironment.getLHCbConfigsForPlatform( platform )
    if not compatibleConfigs['OK']:
      raise RuntimeError( compatibleConfigs['Message'] )
    if not compatibleConfigs['Value']:
      raise RuntimeError( "No configs can be run here. Something is wrong in our configuration" )

    return compatibleConfigs['Value']
