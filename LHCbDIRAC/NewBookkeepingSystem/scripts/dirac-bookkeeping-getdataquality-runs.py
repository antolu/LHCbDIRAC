#!/usr/bin/env python
########################################################################
# $HeadURL: $
# File :   dirac-bookkeeping-getdataquality-runs.py
# Author : Zoltan Mathe
########################################################################
__RCSID__   = "$Id: $"
__VERSION__ = "$ $"

import DIRAC, sys
from DIRAC.Core.Base import Script

from LHCbDIRAC.NewBookkeepingSystem.Client.BookkeepingClient import BookkeepingClient

Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()

if len(args) < 1:
  print 'Usage: ./dirac-bookkeeping-getdataquality-runs.py <runid | list of runids>'
  print 'Example: ./dirac-bookkeeping-getdataquality-runs.py 123 or ./dirac-bookkeeping-getdataquality-runs.py 123 456 789'
  DIRAC.exit(2)
else:
  ids = args

cl = BookkeepingClient()
retVal = cl.getDataQualityForRuns(ids)
if retVal['OK']:
  print "-----------------------------------"
  print "Run Number".ljust(20)+"Flag".ljust(10)
  print "-----------------------------------"
  for i in  retVal["Value"]:
    print str(i[0]).ljust(20)+str(i[1]).ljust(10)
  print "-----------------------------------" 
else:
  print retVal["Message"]

DIRAC.exit()