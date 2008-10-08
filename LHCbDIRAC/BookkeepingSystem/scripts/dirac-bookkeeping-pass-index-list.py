#!/usr/bin/env python
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/BookkeepingSystem/scripts/dirac-bookkeeping-pass-index-list.py,v 1.2 2008/10/08 13:39:00 zmathe Exp $
# File :   dirac-bookkeeping-pass-index-list.py
# Author : Zoltan Mathe
########################################################################
__RCSID__   = "$Id: dirac-bookkeeping-pass-index-list.py,v 1.2 2008/10/08 13:39:00 zmathe Exp $"
__VERSION__ = "$ $"

import sys,string,re
from DIRACEnvironment import DIRAC
from DIRAC.Core.Base import Script

Script.parseCommandLine( ignoreErrors = True )

from DIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
bk = BookkeepingClient()

args = Script.getPositionalArgs()
list = False
insert = False


exitCode = 0


width=20
res=bk.getPass_index()
if res['OK']:
  dbresult = res['Value']
  print str('Description').ljust(15)+str('Group description').ljust(30)+str('Step 0').ljust(40)+str('Step 1').ljust(width)+str('Step 2').ljust(width)+str('Step 2').ljust(width)+str('Step 4').ljust(width)+str('Step 5').ljust(width)
  for record in dbresult:
    print str(record[0]).ljust(15)+str(record[1]).ljust(30)+str(record[2]).ljust(40)+str(record[3]).ljust(width)+str(record[4]).ljust(width)+str(record[5]).ljust(width)
    #print 'Description:'+str(record[0]).ljust(width)+'Step 0'+str(record[1]).ljust(width)+'Step 1'+str(record[2]).ljust(width)+'Step 2'+str(record[3]).ljust(width)+'Step 3'+str(record[4]).ljust(width)+'Step 4'+str(record[5]).ljust(width)