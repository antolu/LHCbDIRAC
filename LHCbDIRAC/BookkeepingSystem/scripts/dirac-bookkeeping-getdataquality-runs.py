#!/usr/bin/env python
########################################################################
# File :    dirac-bookkeeping-getdataquality-runs.py
# Author :  Zoltan Mathe
########################################################################
"""
  Get Data Quality Flag for the given run
"""
__RCSID__ = "$Id$"
import DIRAC
from DIRAC.Core.Base import Script


Script.setUsageMessage('\n'.join([ __doc__.split('\n')[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ... Run ...' % Script.scriptName,
                                     'Arguments:',
                                     '  Run:      Run number' ]))
Script.parseCommandLine(ignoreErrors=True)
ids = Script.getPositionalArgs()

if len(ids) < 1:
  Script.showHelp()

from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
cl = BookkeepingClient()
retVal = cl.getRunFilesDataQuality(int(ids))
if retVal['OK']:
  print "-----------------------------------"
  print "Run Number".ljust(20) + "Flag".ljust(10)
  print "-----------------------------------"
  for i in  retVal["Value"]:
    print str(i[0]).ljust(20) + str(i[1]).ljust(10)
  print "-----------------------------------"
else:
  print retVal["Message"]

DIRAC.exit()

