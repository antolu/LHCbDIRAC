#!/usr/bin/env python
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/BookkeepingSystem/scripts/dirac-bookkeeping-processing-pass-insert.py,v 1.1 2008/09/16 13:48:32 zmathe Exp $
# File :   dirac-bookkeeping-processing-pass-insert.py
# Author : Zoltan Mathe
########################################################################
__RCSID__   = "$Id: dirac-bookkeeping-processing-pass-insert.py,v 1.1 2008/09/16 13:48:32 zmathe Exp $"
__VERSION__ = "$ $"

import sys,string,re
from DIRACEnvironment import DIRAC
from DIRAC.Core.Base import Script

Script.parseCommandLine( ignoreErrors = True )

from DIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
bk = BookkeepingClient()


production = raw_input("Production:")  
passid = raw_input("PassId:")  
inputprod = raw_input("Input production:")  
if inputprod=='':
  inputprod = 0

print 'Do you want to add this new pass_index conditions? (yes or no)'
value = raw_input('Choice:')
choice=value.lower()
if choice in ['yes','y']:
  res = bk.insertProcessing(production, passid, inputprod)
  if res['OK']:
    print 'The processing pass added successfully!'
  else:
    print "Error discovered!",res['Message']
elif choice in ['no','n']:
  print 'Aborded!'


exitCode = 0
