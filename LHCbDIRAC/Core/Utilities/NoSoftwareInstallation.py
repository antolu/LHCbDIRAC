""" NoSoftwareInstallation

  An instance of this module is created by the ModuleFactory. It just makes few basic checks

  This relies on two JDL parameters in LHCb Workflows:
    - SoftwarePackages - applications with their versions to be checked
    - Platform - to determine the required value of CMTCONFIG

    DIRAC assumes an execute() method will exist during usage.
"""

__RCSID__ = "$Id: $"


from DIRAC import gConfig, gLogger, S_ERROR, S_OK
from LHCbDIRAC.Core.Utilities.ProductionEnvironment import getCMTConfigsCompatibleWithPlatforms, getPlatformFromConfig


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

    apps = self.job.get( 'SoftwarePackages', [] )
    if not apps:
      # There is nothing to do
      self.log.info( "There are no Applications defined on SoftwarePackages" )
      return S_OK()
    if type( apps ) == str:
      apps = [ apps ]

    for app in apps:
      self.log.info( 'Requested Package %s' % app )

    # platform ...........................................................
    if 'SystemConfig' in self.job:
      # out-dated
      systemConfig = self.job['SystemConfig']
      if systemConfig.lower() == 'any':
        platform = ['ANY']
      else:
        platform = getPlatformFromConfig( systemConfig )
        if not platform['OK'] or not platform['Value']:
          raise RuntimeError( "No platform detected" )
        platform = platform['Value']
    elif 'Platform' in self.job:
      platform = [self.job['Platform']]
    else:
      platform = ['ANY']

    # localArch ...........................................................
    localArch = gConfig.getValue( '/LocalSite/Architecture', '' )
    if not localArch:
      raise RuntimeError( "/LocalSite/Architecture is missing and must be specified" )


    if 'any' in [p.lower() for p in platform]:
      # If there is no architecture specified on the Job ( with other words,
      # SystemConfig == ANY in the JobDescription, it means we actually do not care which one).
      # So, we take the architecture that is defined as local on the worker node.

      self.log.info( "Platform requested by the job is set to 'ANY', using the local platform" )

    else:
      # Either we set SystemConfig == ANY, of it happened that SystemConfig is the
      # same as our LocalArchitecture. Either way, we make sure that if there is
      # OS compatibility, the config is added to platforms ( CompatiblePlatforms )
      self.log.info( "Platform requested by the job is set to '%s'" % ', '.join( platform ) )
      
      if localArch not in [p.lower() for p in platform]:
        self.log.error( "The platform request is different from the local one... something is very wrong!" )
        return S_ERROR( "The platform request is different from the local one... something is very wrong!" )

    compatibleCMTConfigs = self._getSupportedCMTConfigs( localArch )
    self.log.info( "This node supports the following CMT configs: %s" % ', '.join( compatibleCMTConfigs ) )

    return S_OK()


  def _getSupportedCMTConfigs( self, platform ):
    """ returns getCMTConfigsCompatibleWithPlatforms
    """
    self.log.info( "Node supported platform is: %s" % platform )
    compatibleCMTConfigs = getCMTConfigsCompatibleWithPlatforms( platform )
    if not compatibleCMTConfigs['OK']:
      raise RuntimeError( compatibleCMTConfigs['Message'] )
    if not compatibleCMTConfigs['Value']:
      raise RuntimeError( "No CMT configs can be run here. Something is wrong in our configuration" )

    return compatibleCMTConfigs['Value']
