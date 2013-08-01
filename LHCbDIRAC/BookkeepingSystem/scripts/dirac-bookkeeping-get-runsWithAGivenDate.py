#!/usr/bin/env python
########################################################################
# File :    dirac-bookkeeping-get-runsWithAGivenDate.py
# Author :  Zoltan Mathe
########################################################################
"""
  Retrieve from the Bookkeeping runs from a given date range
"""
__RCSID__ = "$Id$"

from DIRAC.Core.DISET.RPCClient import RPCClient

from DIRAC.Core.Base import Script
Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ... Start [End]' % Script.scriptName,
                                     'Arguments:',
                                     '  Start:    Start date (Format: YYYY-MM-DD)',
                                     '  End:      End date (Format: YYYY-MM-DD). Default is Start' ] ) )
Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()

start = ''
end = ''
if len( args ) > 2 or not args or not args[0]:
  Script.showHelp()

if len( args ) == 2:
  end = args[1]
start = args[0]

in_dict = {}
in_dict['StartDate'] = start
in_dict['EndDate'] = end if end else start

client = RPCClient( 'Bookkeeping/BookkeepingManager' )
res = client.getRunsForAGivenPeriod( in_dict )
if not res['OK']:
  print 'ERROR: Failed to retrieve runs: %s' % res['Message']
else:
  if not res['Value']['Runs']:
    print 'No runs found for the date range', start, end
  else:
    print 'Runs:', res['Value']['Runs']
    if 'ProcessedRuns' in res['Value']:
      print 'Processed runs:', res['Value']['ProcessedRuns']
    if 'NotProcessedRuns' in res['Value']:
      print 'Not processed runs:', res['Value']['NotProcessedRuns']

