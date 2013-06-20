#!/usr/bin/env python
"""
  dirac-create-cfg

    Verbosity:
        -o LogLevel=LEVEL     NOTICE by default, levels available: INFO, DEBUG, VERBOSE...

"""

__RCSID__ = '$Id$'


# Module variables used along the functions
cfg        = None
subLogger  = None
switchDict = {}


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
  args = Script.getPositionalArgs()
  if args:
    subLogger.error( "Found the following positional args '%s', but we only accept switches" % args )
    subLogger.error( "Please, check documentation below" )
    Script.showHelp()
    DIRACExit( 1 )

  processSwitches = dict( Script.getUnprocessedSwitches() )

  if not 'cfg' in processSwitches:
    subLogger.error( "Please, check documentation below" )
    Script.showHelp()
    DIRACExit( 1 )

  subLogger.debug( "The switches used are:" )
  map( subLogger.debug, processSwitches.iteritems() )

  return processSwitches

#...............................................................................

def setSecurity():
  
  cfg.setOption( '/DIRAC/Security/UseServerCertificate', 'yes' )
  cfg.setOption( 'CertFile', '/opt/dirac/etc/grid-security/hostcert.pem' )

def run():

  cfg.loadFromFile( gConfig.diracConfigFilePath )
  

  cfg.writeToFile( switchDict[ 'cfg' ] )
  

#...............................................................................

if __name__ == '__main__':
  
  # Script initialization
  from DIRAC                            import gConfig, gLogger, exit as DIRACExit
  from DIRAC.Core.Base                  import Script
  subLogger = gLogger.getSubLogger( __file__ )

  registerSwitches()
  switchDict = parseSwitches()

  from DIRAC.Core.Utilities.CFG import CFG
  cfg = CFG()
  
  # Run the script
  run()

  # Bye
  DIRACExit( 0 )

#...............................................................................
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF