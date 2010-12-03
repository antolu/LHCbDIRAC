#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :   dirac-bookkeeping-simulationconditions-list
# Author : Zoltan Mathe
########################################################################
__RCSID__   = "$Id$"
__VERSION__ = "$ $"

import sys,string,re
import DIRAC
from DIRAC.Core.Base import Script

Script.parseCommandLine( ignoreErrors = True )

from LHCbDIRAC.NewBookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
bk = BookkeepingClient()
exitCode = 0

res=bk.getSimConditions()
if res['OK']:
  dbresult = res['Value']
  for record in dbresult:
    print 'SimId: '+str(record[0]).ljust(10)
    print '  SimDescription: '+str(record[1]).ljust(10)
    print '  BeamCond: '+str(record[2]).ljust(10)
    print '  BeamEnergy: '+str(record[3]).ljust(10)
    print '  Generator: '+str(record[4]).ljust(10)
    print '  MagneticField: '+str(record[5]).ljust(10)
    print '  DetectorCond: '+str(record[6]).ljust(10)
    print '  Luminosity: '+str(record[7]).ljust(10)

DIRAC.exit(exitCode)