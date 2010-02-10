#!/usr/bin/env python
########################################################################
# $HeadURL:  $
# File :   dirac-bookkeeping-filetypes-list
# Author : Zoltan Mathe
########################################################################

__RCSID__   = "$Id: $"
__VERSION__ = "$ $"

import sys,string,re
import DIRAC
from DIRAC.Core.Base import Script

Script.parseCommandLine( ignoreErrors = True )

from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
bk = BookkeepingClient()
exitCode = 0

res=bk.getAvailableFileTypes()
if res['OK']:
  dbresult = res['Value']
  print 'Filetypes:'
  for record in dbresult:
    print str(record[0]).ljust(10)
    

DIRAC.exit(exitCode)