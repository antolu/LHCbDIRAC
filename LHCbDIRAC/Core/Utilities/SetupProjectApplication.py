""" SetupProjectApplication 
  
  The SetupProjectApplication module is used by the DIRAC JobAgent to check
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


import os

# DIRAC
from DIRAC                           import gConfig, gLogger, S_ERROR, S_OK
from DIRAC.Core.Utilities.Subprocess import shellCall       

# LHCbDIRAC
from LHCbDIRAC.Core.Utilities.DetectOS import NativeMachine


__RCSID__ = "$Id: $"


class SetupProjectApplication( object ):
  """ The LHCb Local Software Install class is used by the DIRAC Job Agent
    to install necessary software via the ModuleFactory"""

  def __init__( self, argumentsDict ):
    """ Constructor
    
    """
    
    self.log = gLogger.getSubLogger( self.__class__.__name__ )

    ce  = argumentsDict.get( 'CE',  {} )    
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
    
    Main method of the class executed by DIRAC jobAgent
    
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
      compatibleArchs = gConfig.getValue( '/Resources/Computing/OSCompatibility/%s' % self.sysConfig, [] )
      if not compatibleArchs:
        self.log.error( '%s OSCompatibility not found' % self.sysConfig )
        return S_ERROR( '%s OSCompatibility not found' % self.sysConfig )
      
      self.sysConfig = compatibleArchs[ 0 ]
      if not self.sysConfig in self.platforms:
        self.log.info( 'Setting SystemConfig as CompatiblePlatform %s' % self.sysConfig )
        self.platforms.append( self.sysConfig )

    if not self.sysConfig in self.platforms:
      self.log.error( 'Requested architecture not supported by CE' )
      return S_ERROR( 'Requested architecture not supported by CE' )

    # Check applications
    # FIXME: do we need this ?
    self.log.info( 'CMTCONFIG  = %s' % self.sysConfig )

    for app in self.apps:
      
      result = self.checkApplication( app )
      
      if not result:
        return S_ERROR( '%s not Installed' % app )

    return S_OK()


  def checkApplication( self, app ):
    """ checkApplication
  
    Method that makes use of SetupProject to check whether the software is
    present on the SharedArea or not.
  
    """

    self.log.info( 'Searching: %s' % app )
      
    cmtEnv = os.environ
    #cmtEnv[ 'MYSITEROOT' ] = siteRoot
    cmtEnv[ 'CMTCONFIG' ]  = self.sysConfig

    appName, appVersion = self.splitApp( app )
    cmdString = ' '.join( [ 'SetupProject.sh', appName, appVersion ] )   
    
    self.log.info( 'Executing %s' % cmdString )   
    ret = shellCall( 1800, cmdString, env = cmtEnv )
    
    if not ret[ 'OK' ]:
      self.log.error( 'Software checking failed', '%s %s' % ( appName, appVersion ) )
      self.log.error( ret[ 'Message' ] )
      return False
    if ret[ 'Value' ][ 0 ]:  # != 0
      self.log.error( 'Software checking failed with non-zero status', '%s %s' % ( appName, appVersion ) )
      self.log.error( ret[ 'Value' ] )
      return False

    self.log.info( 'Found: %s' % app ) 
    self.log.verbose( ret[ 'Value' ] )
    
    return True


  @staticmethod
  def splitApp( app ):
    """ __splitApp
    
    Splits AppName.AppVersion using '.' a separator. If there is no AppVersion,
    it is set as '' by default.
    
    """
    
    appName = app[ 0 ]
    try:
      appVersion = app[ 1 ]
    except IndexError:
      appVersion = ''

    return appName, appVersion


#...............................................................................
#EOF
