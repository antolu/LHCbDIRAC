#!/usr/bin/env python
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/BookkeepingSystem/scripts/dirac-bookkeeping-simulationconditions-list.py,v 1.1 2008/11/11 15:49:56 zmathe Exp $
# File :   dirac-bookkeeping-simulationconditions-list
# Author : Zoltan Mathe
########################################################################
__RCSID__   = "$Id: dirac-bookkeeping-simulationconditions-list.py,v 1.1 2008/11/11 15:49:56 zmathe Exp $"
__VERSION__ = "$ $"

import sys,string,re
from DIRACEnvironment import DIRAC
from DIRAC.Core.Base import Script

Script.parseCommandLine( ignoreErrors = True )

from DIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
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