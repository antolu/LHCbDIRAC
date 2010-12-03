#!/usr/bin/env python
########################################################################
# $HeadURL: svn+ssh://svn.cern.ch/reps/dirac/LHCbDIRAC/branches/LHCbDIRAC/BookkeepingSystem/bk_2010101501/scripts/dirac-bookkeeping-simulationconditions-list.py $
# File :   dirac-bookkeeping-simulationconditions-list
# Author : Zoltan Mathe
########################################################################
__RCSID__   = "$Id: dirac-bookkeeping-simulationconditions-list.py 18177 2009-11-11 14:02:57Z zmathe $"
__VERSION__ = "$ $"

import sys,string,re
import DIRAC
from DIRAC.Core.Base import Script

Script.parseCommandLine( ignoreErrors = True )

from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
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