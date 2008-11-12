#!/usr/bin/env python
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/BookkeepingSystem/scripts/dirac-bookkeeping-processing-pass-list.py,v 1.1 2008/11/12 13:46:30 zmathe Exp $
# File :   dirac-bookkeeping-processing-pass-list.py
# Author : Zoltan Mathe
########################################################################
__RCSID__   = "$Id: dirac-bookkeeping-processing-pass-list.py,v 1.1 2008/11/12 13:46:30 zmathe Exp $"
__VERSION__ = "$ $"

import sys,string,re
from DIRACEnvironment import DIRAC
from DIRAC.Core.Base import Script

Script.parseCommandLine( ignoreErrors = True )

from DIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
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
