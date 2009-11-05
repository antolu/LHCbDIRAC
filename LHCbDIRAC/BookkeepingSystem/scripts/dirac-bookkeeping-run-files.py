#!/usr/bin/env python
from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()
########################################################################
# $HeadURL$
########################################################################
__RCSID__   = "$Id$"
__VERSION__ = "$ $"

from DIRAC import gLogger
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.Core.Utilities.List import sortList
from DIRAC.ConfigurationSystem.Client import PathFinder
import sys

if not len(sys.argv) == 2:
  print 'Usage: dirac-bookkeeping-run-files.py runID'
  sys.exit()
else:
  runID = int(sys.argv[1])

client = RPCClient('Bookkeeping/BookkeepingManager')
res = client.getRunFiles(runID)
if not res['OK']:
  print 'Failed to retrieve run files: %s' % res['Message']
else:
  if not res['Value']:
    print 'No files found for run %s' % runID
  else:
    print  '%s %s %s %s' % ('FileName'.ljust(100),'Size'.ljust(10),'GUID'.ljust(40),'Replica'.ljust(8))
    for lfn in sortList(res['Value'].keys()):
      size = res['Value'][lfn]['FileSize']
      guid = res['Value'][lfn]['GUID']
      hasReplica = res['Value'][lfn]['GotReplica']
      print '%s %s %s %s' % (lfn.ljust(100),str(size).ljust(10),guid.ljust(40),hasReplica.ljust(8))
