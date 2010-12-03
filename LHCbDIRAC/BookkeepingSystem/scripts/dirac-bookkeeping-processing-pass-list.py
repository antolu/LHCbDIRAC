#!/usr/bin/env python
########################################################################
# $HeadURL: svn+ssh://svn.cern.ch/reps/dirac/LHCbDIRAC/branches/LHCbDIRAC/BookkeepingSystem/bk_2010101501/scripts/dirac-bookkeeping-processing-pass-list.py $
# File :   dirac-bookkeeping-processing-pass-list.py
# Author : Zoltan Mathe
########################################################################
__RCSID__   = "$Id: dirac-bookkeeping-processing-pass-list.py 18177 2009-11-11 14:02:57Z zmathe $"
__VERSION__ = "$ $"

import sys,string,re
import DIRAC
from DIRAC.Core.Base import Script

Script.parseCommandLine( ignoreErrors = True )

from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
bk = BookkeepingClient()

production = None
args = sys.argv     
if len(args) > 1:
  production = long(args[1]) 

exitCode = 0

res = bk.listProcessingPass(production)
if res['OK']:
  dbresult = res['Value']
  for record in dbresult:
    print 'Production: '+str(record[1]).ljust(10)
    print '  Total Processing pass: '+str(record[0]).ljust(10)
    

DIRAC.exit(exitCode)

exitCode = 0
