#!/usr/bin/env python
########################################################################
# File :    dirac-bookkeeping-run-files.py
# Author :  Zoltan Mathe
########################################################################
"""
  Retrieve files for a given run
"""
__RCSID__ = "$Id: dirac-bookkeeping-run-files.py 76842 2014-07-25 08:24:53Z fstagni $"
import DIRAC
from DIRAC.Core.Base import Script

Script.setUsageMessage('\n'.join([ __doc__.split('\n')[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ... Run' % Script.scriptName,
                                     'Arguments:',
                                     '  Run:      Run number (integer)' ]))
Script.parseCommandLine(ignoreErrors=True)
args = Script.getPositionalArgs()

if len(args) != 1:
  Script.showHelp()

try:
  runID = int(args[0])
except:
  Script.showHelp()

from DIRAC.Core.DISET.RPCClient import RPCClient

exitCode = 0

client = RPCClient('Bookkeeping/BookkeepingManager')
res = client.getRunFiles(runID)
if not res['OK']:
  print 'Failed to retrieve run files: %s' % res['Message']
  exitCode = 2
else:
  if not res['Value']:
    print 'No files found for run %s' % runID
  else:
    print  '%s %s %s %s' % ('FileName'.ljust(100), 'Size'.ljust(10), 'GUID'.ljust(40), 'Replica'.ljust(8))
    for lfn in sorted( res['Value'].keys() ):
      size = res['Value'][lfn]['FileSize']
      guid = res['Value'][lfn]['GUID']
      hasReplica = res['Value'][lfn]['GotReplica']
      print '%s %s %s %s' % (lfn.ljust(100), str(size).ljust(10), guid.ljust(40), str(hasReplica).ljust(8))

DIRAC.exit(exitCode)

