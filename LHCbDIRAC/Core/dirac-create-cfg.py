#!/usr/bin/env python
"""
  dirac-create-cfg
    Script created to prepare dirac.cfg files on CloudComputing world.

    Verbosity:
        -o LogLevel=LEVEL     NOTICE by default, levels available: INFO, DEBUG, VERBOSE...

"""

# TODO: this script will not be needed after with pilots 2.0 (DIRAC v6r12)
# Eventual needs should be coded as pilot commands

__RCSID__ = "$Id$"


# Module variables used along the functions
subLogger  = None


def registerSwitches():
  '''
    Registers all switches that can be used while calling the script from the
    command line interface.
  '''

  switches = ( 
    ( 'cfg=', 'Path where to write the cfg file ( mandatory )' ),
              )
  
  for switch in switches:
    Script.registerSwitch( '', switch[ 0 ], switch[ 1 ] )
  Script.setUsageMessage( __doc__ )


def parseSwitches():
  '''
    Parses the arguments passed by the user
  '''

  Script.parseCommandLine( ignoreErrors = True )
  configArgs = Script.getPositionalArgs()

  processSwitches = dict( Script.getUnprocessedSwitches() )

  if not 'cfg' in processSwitches:
    subLogger.error( "Please, check documentation below" )
    Script.showHelp()
    DIRACExit( 1 )

  subLogger.debug( "The switches used are:" )
  map( subLogger.debug, processSwitches.iteritems() )

  return configArgs, processSwitches


#...............................................................................

def setSecurity():
  """
  DIRAC
  {
    Security
    {
      UseServerCertificate = yes
      CertFile = /opt/dirac/etc/grid-security/hostcert.pem
      KeyFile  = /opt/dirac/etc/grid-security/hostkey.pem
    }
  }
  """
  
  secPath = '/DIRAC/Security'
  gridSec = '/opt/dirac/etc/grid-security' 
  
  cfg.setOption( '%s/UseServerCertificate' % secPath, 'yes' )
  cfg.setOption( '%s/CertFile' % secPath, '%s/hostcert.pem' % gridSec )
  cfg.setOption( '%s/KeyFile' % secPath, '%s/hostkey.pem' % gridSec )


def setLocalSiteDefaults():
  """
  LocalSite
  {
    Architecture   = #ARCHITECTURE
    InstancePath   = /opt/dirac
    ReleaseVersion = DEV
    ReleaseProject = LHCb
    SharedArea     = /cvmfs/lhcb.cern.ch/lib
    LocalArea      = /opt/dirac
    
    CPUTime    = 1400000
    MaxCPUTime = 1400000
    
    CPUScalingFactor       = #CPUScalingFactor
    CPUNormalizationFactor = #CPUNormalizationFactor 
  }
  
  """
  
  locPath = '/LocalSite'
  cfg.createNewSection( locPath ) 
  
  cfg.setOption( '%s/Architecture'   % locPath, '#ARCHITECTURE' )
  # Used to get control and work directories
  cfg.setOption( '%s/InstancePath'   % locPath, '/opt/dirac' )
  # LHCbDIRAC version
  cfg.setOption( '%s/ReleaseVersion' % locPath, 'DEV' )
  # DIRAC extension
  cfg.setOption( '%s/ReleaseProject' % locPath, 'LHCb' ) 
  # Site DIRAC name
  #Site=#
  
  # SW Areas
  cfg.setOption( '%s/SharedArea' % locPath, '/cvmfs/lhcb.cern.ch/lib' )
  cfg.setOption( '%s/LocalArea'  % locPath , '/opt/dirac' )
  
  # CPU Queues
  cfg.setOption( '%s/CPUTime'    % locPath, 300000 )
  cfg.setOption( '%s/MaxCPUTime' % locPath, 300000 )
  
  # CPU Factors
  cfg.setOption( '%s/CPUScalingFactor'       % locPath, '#CPUScalingFactor' )
  cfg.setOption( '%s/CPUNormalizationFactor' % locPath, '#CPUNormalizationFactor' )
  

def setConfigOptions( configOptionsList ):
  """ setConfigOptions
  
  Given a list of options, sets them. Note that if the Section does not exist
  on the dirac.cfg this method will not work.
  
  """
  
  for option in configOptionsList:
      
    try:
      cfg.setOption( *option.split( '=' ) )
    except KeyError, e:
      DIRACExit( e )
    except ValueError, e:
      DIRACExit( e )  
  
    subLogger.info( option )
  
  
def run( configOptionsList, cfgFile ):
  """ run
  
  Main method. Loads the default config and populates with our additions.
  
  """

  cfg.loadFromFile( gConfig.diracConfigFilePath )
    
  setSecurity() 
  setLocalSiteDefaults()
  
  setConfigOptions( configOptionsList )

  cfg.writeToFile( cfgFile )
  

#...............................................................................

if __name__ == '__main__':
  
  # Script initialization
  from DIRAC                            import gConfig, gLogger, exit as DIRACExit
  from DIRAC.Core.Base                  import Script
  subLogger = gLogger.getSubLogger( __file__ )

  registerSwitches()
  configList, switchDict = parseSwitches()

  from DIRAC.Core.Utilities.CFG import CFG
  cfg = CFG()
  
  # Run the script
  run( configList, switchDict[ 'cfg' ] )

  # Bye
  DIRACExit( 0 )

#...............................................................................
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
