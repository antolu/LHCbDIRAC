""" NoSoftwareInstallation

  The NoSoftwareInstallation module is used by the DIRAC JobAgent to check
  the presence on the shared areas of the necessary software. An instance of
  this module is created by the ModuleFactory.

  This relies on two JDL parameters in LHCb Workflows:
    - SoftwarePackages - applications with their versions to be checked
    - SystemConfig - to determine the required value of CMTCONFIG
    It checks also the provided CE:
    - CompatiblePlatforms
    and returns error if requested SystemConfig is not in CompatiblePlatforms

    DIRAC assumes an execute() method will exist during usage.

"""

# DIRAC
from DIRAC import gConfig, gLogger, S_ERROR, S_OK
from DIRAC.ConfigurationSystem.Client.Helpers.Resources import getCompatiblePlatforms

# LHCbDIRAC
from LHCbDIRAC.Core.Utilities.DetectOS import NativeMachine


__RCSID__ = "$Id: $"


class NoSoftwareInstallation( object ):
  """NoSoftwareInstallation

  This class is a temporary solution until a proper fix is set at the level
  of DIRAC. It's main objective is to replace the class CombinedSoftwareInstallation.
  At the moment, it is set on the CS. However, if we clear that entry on the CS
  the JobAgent will crash as it is expecting something. That something is this class.

  """


  def __init__( self, argumentsDict ):
    """ Constructor

    """

    self.log = gLogger.getSubLogger( self.__class__.__name__ )

    ce = argumentsDict.get( 'CE', {} )
    job = argumentsDict.get( 'Job', {} )

    # self.apps ................................................................
    apps = job.get( 'SoftwarePackages', [] )
    if type( apps ) == str:
      apps = [ apps ]

    self.apps = []
    for app in apps:
      self.log.verbose( 'Requested Package %s' % app )
      self.apps.append( app.split( '.' ) )

    # self.sysConfig ...........................................................
    if 'SystemConfig' in job:
      self.sysConfig = job[ 'SystemConfig' ]
    else:
      self.sysConfig = NativeMachine().CMTSupportedConfig()[0]

    # self.platforms ...........................................................
    self.platforms = ce.get( 'CompatiblePlatforms', self.sysConfig )
    if type( self.platforms ) == str:
      self.platforms = [ self.platforms ]

    # self.localArch ...........................................................
    self.localArch = gConfig.getValue( '/LocalSite/Architecture', '' )


  def execute( self ):
    """ execute

    Main method of the class executed by DIRAC JobAgent. It checks the parameters
    in case there is a missconfiguration and returns S_OK / S_ERROR.

    """

    if not self.apps:
      # There is nothing to do
      self.log.info( 'There are no Applications defined on SoftwarePackages' )
      return S_OK()

    if not self.sysConfig:
      # As there is no architecture, there is no way to know which software we
      # have to load.
      self.log.warn( 'No architecture requested' )
      return S_ERROR( 'No architecture requested' )

    if self.sysConfig.lower() == 'any':
      # If there is no architecture specified on the Job ( with other words,
      # SystemConfig == ANY in the JobDescription, it means we actually do not
      # care which one. So, we take the architecture that is defined as
      # local on the worker node.

      self.log.info( 'SystemConfig set to "ANY"' )

      self.sysConfig = self.localArch
      self.platforms = [ self.localArch ]
      if not self.sysConfig:
        return S_ERROR( '/LocalSite/Architecture is missing and must be specified' )

    if not self.platforms:
      self.log.info( 'There are no CompatiblePlatforms defined' )
      return S_OK()

    self.log.info( 'CE supported platforms are: %s' % ( ', '.join( self.platforms ) ) )

    # Either we set SystemConfig == ANY, of it happened that SystemConfig is the
    # same as our LocalArchitecture. Either way, we make sure that if there is
    # OSCompatibility, the config is added to platforms ( CompatiblePlatforms )
    if self.sysConfig == self.localArch:

      self.log.info( 'Job SystemConfiguration is set to /LocalSite/Architecture' )
      self.log.info( 'Checking compatible platforms' )

      compatibleArchs = getCompatiblePlatforms( self.sysConfig )
      if not compatibleArchs['OK']:
        return compatibleArchs

      self.sysConfig = compatibleArchs['Value'][ 0 ]
      if not self.sysConfig in self.platforms:
        self.log.info( 'Setting SystemConfig as CompatiblePlatform %s' % self.sysConfig )
        self.platforms.append( self.sysConfig )

    if not self.sysConfig in self.platforms:
      self.log.error( 'Requested architecture not supported by CE' )
      return S_ERROR( 'Requested architecture not supported by CE' )

    self.log.info( 'CMTCONFIG  = %s' % self.sysConfig )

    self.log.info( 'LIST OF APPLICATIONS' )
    for app in self.apps:
      self.log.info( app )

    return S_OK()
