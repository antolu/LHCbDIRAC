#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :    dirac-bookkeeping-setdataquality-run-processing-pass.py  
# Author :  Zoltan Mathe
########################################################################
"""
  Set Data Quality Flag for the given run and Processing Pass
"""
__RCSID__ = "$Id$"

import DIRAC
from DIRAC.Core.Base import Script

Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ... Run Pass Flag' % Script.scriptName,
                                     'Arguments:',
                                     '  Run:      Run number',
                                     '  Pass:     Processing Pass',
                                     '  Flag:     Quality Flag' ] ) )
Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()

from LHCbDIRAC.NewBookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
bk = BookkeepingClient()
if len( args ) < 3:
  result = bk.getAvailableDataQuality()
  if not result['OK']:
    print 'ERROR: %s' % ( result['Message'] )
    DIRAC.exit( 2 )
  flags = result['Value']
  print "Available Data Quality Flags"
  for flag in flags:
    print flag
  Script.showHelp()

exitCode = 0
rnb = int( args[0] )
proc = args[1]
flag = args[2]
result = bk.setRunQualityWithProcessing( rnb, proc, flag )

if not result['OK']:
  print 'ERROR: %s' % ( result['Message'] )
  exitCode = 2
else:
  print 'The run is flagged!'

DIRAC.exit( exitCode )
