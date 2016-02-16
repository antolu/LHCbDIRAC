#!/usr/bin/env python
########################################################################
# File :    dirac-bookkeeping-setdataquality-run
# Author :  Zoltan Mathe
########################################################################
"""
  Set Data Quality Flag for the given run
"""
__RCSID__ = "$Id: dirac-bookkeeping-setdataquality-run.py 69359 2013-08-08 13:57:13Z phicharp $"

import DIRAC
from DIRAC.Core.Base import Script

Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ... Run Flag' % Script.scriptName,
                                     'Arguments:',
                                     '  Run:      Run number',
                                     '  Flag:     Quality Flag' ] ) )
Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()

from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
bk = BookkeepingClient()

if len( args ) < 2:
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
flag = args[1]
result = bk.setRunDataQuality( rnb, flag )

if not result['OK']:
  print 'ERROR: %s' % ( result['Message'] )
  exitCode = 2
else:
  succ = result['Value']['Successful']
  failed = result['Value']['Failed']
  print 'The data quality has been set for the following files:'
  for i in succ:
    print i

  if len( failed ) != 0:
    print 'The data quality has not been set for the following files:'
    for i in failed:
      print i

DIRAC.exit( exitCode )
