#!/usr/bin/env python
"""
  Perform comprehensive checks on the supplied log file if it exists.
"""
__RCSID__ = "$Id$"


import DIRAC
from DIRAC.Core.Base import Script

Script.registerSwitch( "f:", "XMLSummary=", "Path to XML summary you wish to analyze (mandatory)" )

Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ... CE' % Script.scriptName ] ) )
Script.parseCommandLine( ignoreErrors = True )

from DIRAC import gLogger
from LHCbDIRAC.Core.Utilities.XMLSummaries import analyseXMLSummary

args = Script.getPositionalArgs()

logFile = ''
projectName = ''

#Start the script and perform checks
if args or not Script.getUnprocessedSwitches():
  Script.showHelp()

for switch in Script.getUnprocessedSwitches():
  if switch[0].lower() in ( 'f', 'XMLSummary' ):
    logFile = switch[1]

exitCode = 0
try:
  analyseXMLSummary( logFile, projectName )
except Exception, x:
  gLogger.exception( 'XML summary analysis failed with exception: "%s"' % x )
  exitCode = 2
  DIRAC.exit( exitCode )

DIRAC.exit( exitCode )
