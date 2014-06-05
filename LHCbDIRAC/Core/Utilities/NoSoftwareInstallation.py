""" NoSoftwareInstallation

    An instance of this module is created by the ModuleFactory. It just makes few basic checks
    This relies on JDL parameters in LHCb Workflows "Platform" to determine the required value of CMTCONFIG

    DIRAC assumes an execute() method will exist during usage.
"""

__RCSID__ = "$Id: $"


from DIRAC import gConfig, gLogger, S_ERROR, S_OK
from LHCbDIRAC.Core.Utilities import ProductionEnvironment


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
    if 'SystemConfig' in self.job:
      # out-dated
      systemConfig = self.job['SystemConfig']
      if systemConfig.lower() == 'any':
        platform = 'ANY'
      else:
        platform = ProductionEnvironment.getPlatformFromConfig( systemConfig )[0]
    elif 'Platform' in self.job:
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

    compatibleCMTConfigs = self._getSupportedCMTConfigs( localArch )
    self.log.info( "This node supports the following CMT configs: %s" % ', '.join( compatibleCMTConfigs ) )

    return S_OK()


  def _getSupportedCMTConfigs( self, platform ):
    """ returns getCMTConfigsCompatibleWithPlatforms
    """
    self.log.info( "Node supported platform is: %s" % platform )
    compatibleCMTConfigs = ProductionEnvironment.getCMTConfigsCompatibleWithPlatforms( platform )
    if not compatibleCMTConfigs['OK']:
      raise RuntimeError( compatibleCMTConfigs['Message'] )
    if not compatibleCMTConfigs['Value']:
      raise RuntimeError( "No CMT configs can be run here. Something is wrong in our configuration" )

    return compatibleCMTConfigs['Value']
