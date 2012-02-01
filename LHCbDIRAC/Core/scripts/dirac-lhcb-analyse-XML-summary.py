#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :    dirac-lhcb-analyse-log-file.py
# Author :  Federico Stagni
########################################################################
"""
  Perform comprehensive checks on the supplied log file if it exists.
"""
__RCSID__ = "$Id$"

import sys, string, os, shutil

import DIRAC
from DIRAC.Core.Base import Script

Script.registerSwitch( "f:", "XMLSummary=", "Path to XML summary you wish to analyze (mandatory)" )

Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ... CE' % Script.scriptName ] ) )
Script.parseCommandLine( ignoreErrors = True )

from DIRAC import gConfig, gLogger, S_OK, S_ERROR
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
  result = analyseXMLSummary( logFile, projectName )
except Exception, x:
  gLogger.exception( 'XML summary analysis failed with exception: "%s"' % x )
  exitCode = 2
  DIRAC.exit( exitCode )

if not result['OK']:
  gLogger.warn( result )
  gLogger.error( 'Problem found with XML summary %s: "%s"' % ( logFile, result['Message'] ) )
  exitCode = 2
else:
  gLogger.verbose( result )
  gLogger.info( 'XML summary %s, %s' % ( logFile, result['Value'] ) )

DIRAC.exit( exitCode )
