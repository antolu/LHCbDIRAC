#!/usr/bin/env python
########################################################################
# $HeadURL$
# File : dirac-bookkeeping-setdataquality-run-processing-pass.py  
# Author : Zoltan Mathe
########################################################################
__RCSID__ = "$Id$"

import DIRAC
from DIRAC.Core.Base import Script

from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient

Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()

bk = BookkeepingClient()

def usage():
  print 'Available data quality flags:'

  result = bk.getAvailableDataQuality()
  if not result['OK']:
    print 'ERROR %s' % ( result['Message'] )
    exitCode = 2

  for i in result['Value']:
    print i
  print 'Usage: %s <RunNumber> <Processing pass> <DataQualityFlag>' % ( Script.scriptName )
  DIRAC.exit( 2 )

if len( args ) < 3:
  usage()

exitCode = 0
rnb = int( args[0] )
proc = str( args[1] )
flag = str( args[2] )
result = bk.setRunQualityWithProcessing( rnb, proc, flag )

if not result['OK']:
  print 'ERROR %s' % ( result['Message'] )
  exitCode = 2
else:
  print 'The run is flagged!'

DIRAC.exit( exitCode )
