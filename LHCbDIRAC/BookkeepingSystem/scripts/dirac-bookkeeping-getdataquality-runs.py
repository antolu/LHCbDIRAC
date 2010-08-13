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

from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient

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
  print retVal
else:
  print retVal["Message"]

DIRAC.exit()