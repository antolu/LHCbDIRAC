#!/usr/bin/env python
"""
  Perform comprehensive checks on the supplied log file if it exists.
"""
__RCSID__ = "$Id$"


import DIRAC
from DIRAC.Core.Base import Script

Script.registerSwitch( "f:", "LogFile=", "Path to log file you wish to analyze (mandatory)" )
Script.registerSwitch( "p:", "Project=", "Optional: project name (will be guessed if not specified)" )

Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ... CE' % Script.scriptName ] ) )
Script.parseCommandLine( ignoreErrors = True )

from DIRAC import gLogger
from LHCbDIRAC.Core.Utilities.ProductionLogs import analyseLogFile

args = Script.getPositionalArgs()

logFile = ''
projectName = ''

#Start the script and perform checks
if args or not Script.getUnprocessedSwitches():
  Script.showHelp()

for switch in Script.getUnprocessedSwitches():
  if switch[0].lower() in ( 'p', 'project' ):
    projectName = switch[1]
  elif switch[0].lower() in ( 'f', 'logfile' ):
    logFile = switch[1]

exitCode = 0
try:
  result = analyseLogFile( logFile, projectName )
except Exception, x:
  gLogger.exception( 'Log file analysis failed with exception: "%s"' % x )
  exitCode = 2
  DIRAC.exit( exitCode )

if not result['OK']:
  gLogger.warn( result )
  gLogger.error( 'Problem found with log file %s: "%s"' % ( logFile, result['Message'] ) )
  exitCode = 2
else:
  gLogger.verbose( result )
  gLogger.info( 'Log file %s, %s' % ( logFile, result['Value'] ) )

DIRAC.exit( exitCode )
