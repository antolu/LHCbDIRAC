#!/usr/bin/env python

"""
  dirac-lhcb-view-software

    This script merges the dirac-lhcb-list-software and dirac-lhcb-check-software scripts into one.

    Usage:
      dirac-lhcb-view-software
        --systemConfig        Optional System Configuration To Query For
                             (by default it does not look for a specific configuration)
        --name                Name of the LHCb software package
        --version             Version of the LHCb software package

      Used without arguments, the script will list all the software in all the configurations.
      A given configuration can be queried using the 'systemConfig' arg.
      It is also possible to check whether a precise software version is deployed using the 'name' and 'version' args.


"""


__RCSID__ = "$Id: $"


runMode = None
subLogger = None
switchDict = {}

software = None
active = None
deprecated = None
systemConfigs = None


def registerSwitches():
  '''
    Registers all switches that can be used while calling the script from the
    command line interface.
  '''

  switches = ( 
    ( 'systemConfig=', 'Optional System Configuration To Query For' ),
    ( 'name=', 'Name of the LHCb software package' ),
    ( 'version=', 'Version of the LHCb software package' )
              )
  for switch in switches:
    Script.registerSwitch( '', switch[ 0 ], switch[ 1 ] )
  Script.setUsageMessage( __doc__ )


def parseSwitches():
  '''
    Parses the arguments passed by the user
  '''
  global runMode
  Script.parseCommandLine( ignoreErrors = True )
  args = Script.getPositionalArgs()
  if args:
    subLogger.error( "Found the following positional args '%s', but we only accept switches" % args )
    subLogger.error( "Please, check documentation below" )
    Script.showHelp()
    DIRACExit( 1 )

  switches = dict( Script.getUnprocessedSwitches() )



  subLogger.debug( "The switches used are:" )
  map( subLogger.debug, switches.iteritems() )

  if len( switches ) == 0:
    runMode = "list"
  elif ( len( switches ) == 1 ) and ( 'systemConfig' in switches ):
    runMode = "list"
  elif ( len( switches ) == 2 ) and ( 'name' in switches ) and ( 'version' in switches ):
    runMode = "check"
  else :
    Script.showHelp()
    DIRACExit( 1 )

  switches.setdefault( 'systemConfig', None )

  return switches


def printSoftware( package, packageArch ):
  '''
    Helper function to format the output
    Prints the software name, the version, and the list of available architectures
  '''
  adj = 30
  gLogger.notice( package.split( '.' )[0].ljust( adj ) \
                  + package.split( '.' )[1].ljust( adj )\
                  + ','.join( packageArch ).ljust( adj ) )


def printHeader( header ):
  '''
    Helper function to format the output : prints a header
  '''
  gLogger.notice( '=========> %s ' % header )


def getSoftwareDistribution():
  '''
    Get information about software distribution and stores it into global variables :
    software : a dictionary with keys being the system config, and the values the software list
    systemConfig : the keys of software, without 'active' and 'deprecated'
    active : list of active software
    deprecated : list of deprecated software
  '''

  softwareDistribution = gConfig.getOptionsDict( '/Operations/SoftwareDistribution' )
  if not softwareDistribution['OK']:
    gLogger.notice( 'ERROR: Could not get values for /Operations/SoftwareDistribution section with message:\n%s'\
                     % ( softwareDistribution['Message'] ) )
    DIRACExit( 2 )

  global systemConfigs, active, deprecated, software

  software = softwareDistribution['Value']
  systemConfigs = software.keys()
  systemConfigs.remove( 'Active' )
  systemConfigs.remove( 'Deprecated' )


  # active = software['Active'].replace( ' ', '' ).split( ',' )
  # active.sort()
  # deprecated = software['Deprecated'].replace( ' ', '' ).split( ',' )
  # deprecated.sort()
  active = List.fromChar( software['Active'], ',' )
  active.sort()
  deprecated = List.fromChar( software['Deprecated'], ',' )
  deprecated.sort()


def checkPackage( name, version ):
  '''
    Check whether a given package version is installed
  '''

  packageChecked = name + '.' + version
  if not packageChecked in active:
    gLogger.notice( 'This package was not distributed on the GRID' )
    DIRACExit( 1 )
  else:
    gLogger.notice( 'This package is distributed on the GRID' )


def listConfig( configuration = None ):
  '''
    List the installed packages.
    If the 'configuration' argument is set, only this system configuration is listed
  '''

  if configuration:
    printHeader( 'Active LHCb Software For System Configuration %s' % ( configuration ) )
  else:
    printHeader( 'Active LHCb Software For All System Configurations' )

  for systemConfig in systemConfigs:
    try:
      software[systemConfig] = software[systemConfig].replace( ' ', '' ).split( ',' )
    except:
      gLogger.error( "No system configuration called %s" % systemConfig )
      DIRACExit( 3 )
  for package in active:
    packageArch = []
    for systemConfig in systemConfigs:
      if package in software[systemConfig]:
        packageArch.append( systemConfig )
    if packageArch:
      printSoftware( package, packageArch )
    else:
      if not configuration:
        gLogger.notice( 'WARNING: %s %s is not defined for any system configurations' % ( package.split( '.' )[0], package.split( '.' )[1] ) )

  if not configuration:
    printHeader( 'Deprecated LHCb Software' )
    gLogger.notice( '%s' % ( ', '.join( deprecated ) ) )


if __name__ == "__main__":

  # Script initialization
  from DIRAC.Core.Base import Script
  from DIRAC           import gLogger, exit as DIRACExit
  subLogger = gLogger.getSubLogger( __file__ )

  registerSwitches()
  switchDict = parseSwitches()

  from DIRAC.Core.Utilities import List
  from DIRAC import gConfig

  getSoftwareDistribution()
  if runMode == "list":
    if switchDict['systemConfig'] :
      systemConfigs = [switchDict['systemConfig']]
    listConfig( switchDict['systemConfig'] )
  elif runMode == "check":
    checkPackage( switchDict['name'], switchDict['version'] )
  # Bye
  DIRACExit( 0 )
