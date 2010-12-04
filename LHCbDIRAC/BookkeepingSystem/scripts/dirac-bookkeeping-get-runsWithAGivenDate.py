#!/usr/bin/env python


__RCSID__ = "$Id$"

from DIRAC import gLogger
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.ConfigurationSystem.Client import PathFinder
import sys

from DIRAC.Core.Base import Script
Script.parseCommandLine( ignoreErrors = True )

start = ''
end = ''
if not len( sys.argv ) == 2:
  print 'Usage: dirac-bookkeeping-get-runsWithAGivenDate.py StartDate EndDate'
  print 'For ex: dirac-bookkeeping-get-runsWithAGivenDate.py yyyy-mm-dd'
  print 'For ex: dirac-bookkeeping-get-runsWithAGivenDate.py 2010-04-02'
  print 'For ex: dirac-bookkeeping-get-runsWithAGivenDate.py 2010-04-02 2010-04-10'
  sys.exit( 0 )
else:
  if len( sys.argv ) > 2:
    start = sys.argv[1]
    end = sys.argv[2]
  else:
    start = sys.argv[1]

dict = {}
if start != '':
  dict['StartDate'] = start
if end != '':
  dict['EndDate'] = end

client = RPCClient( 'Bookkeeping/BookkeepingManager' )
res = client.getRunsWithAGivenDates( dict )
if not res['OK']:
  print 'Failed to retrieve runs: %s' % res['Message']
else:
  if not res['Value']:
    print 'No runs found for a given date %s' % runID
  else:
    print 'Runs:', res['Value']['Runs']
    print 'Processed runs:', res['Value']['ProcessedRuns']
    print 'Not processed runs:', res['Value']['NotProcessedRuns']
