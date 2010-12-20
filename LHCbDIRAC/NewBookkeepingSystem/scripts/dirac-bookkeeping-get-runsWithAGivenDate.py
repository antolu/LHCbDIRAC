#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :    dirac-bookkeeping-get-runsWithAGivenDate.py
# Author :  Zoltan Mathe
########################################################################
"""
  Retrieve from the Bookkeeping runs from a given date range
"""
__RCSID__ = "$Id$"

from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.ConfigurationSystem.Client import PathFinder

from DIRAC.Core.Base import Script
Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ... Start [End]' % Script.scriptName,
                                     'Arguments:',
                                     '  Start:    Start date (Format: YYYY-MM-DD)',
                                     '  End:      End date (Format: YYYY-MM-DD)' ] ) )
Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()

start = ''
end = ''
if len( args ) > 2 or not args:
  Script.showHelp()

if len( args ) == 2:
  end = args[1]
start = args[0]

dict = {}
if start != '':
  dict['StartDate'] = start
if end != '':
  dict['EndDate'] = end

client = RPCClient( 'Bookkeeping/NewBookkeepingManager' )
res = client.getRunsWithAGivenDates( dict )
if not res['OK']:
  print 'ERROR: Failed to retrieve runs: %s' % res['Message']
else:
  if not res['Value']['Runs']:
    print 'No runs found for a given dates', start, end
  else:
    print 'Runs:', res['Value']['Runs']
    if 'ProcessedRuns' in res['Value']:
      print 'Processed runs:', res['Value']['ProcessedRuns']
    if 'NotProcessedRuns' in res['Value']:
      print 'Not processed runs:', res['Value']['NotProcessedRuns']
