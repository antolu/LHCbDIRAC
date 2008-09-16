#!/usr/bin/env python
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/BookkeepingSystem/scripts/dirac-bookkeeping-pass-index-insert.py,v 1.1 2008/09/16 13:48:32 zmathe Exp $
# File :   dirac-bookkeeping-pass-index-insert.py
# Author : Zoltan Mathe
########################################################################
__RCSID__   = "$Id: dirac-bookkeeping-pass-index-insert.py,v 1.1 2008/09/16 13:48:32 zmathe Exp $"
__VERSION__ = "$ $"

import sys,string,re
from DIRACEnvironment import DIRAC
from DIRAC.Core.Base import Script

Script.parseCommandLine( ignoreErrors = True )

from DIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
bk = BookkeepingClient()


groupdesc = raw_input("Group description:")  
step0 = raw_input("Step 0:")  
step1 = raw_input("Step 1:")  
step2 = raw_input("Step 2:")  
step3 = raw_input("Step 3:")  
step4 = raw_input("Step 4:")  
step5 = raw_input("Step 5:")  
step6 = raw_input("Step 6:")  
print 'Do you want to add this new pass_index conditions? (yes or no)'
value = raw_input('Choice:')
choice=value.lower()
if choice in ['yes','y']:
  res = bk.insert_pass_index(groupdesc, step0, step1, step2, step3, step4, step5, step6)
  if res['OK']:
    print 'The pass_index added successfully!'
    print 'The new passid',res['Value']
  else:
    print "Error discovered!",res['Message']
elif choice in ['no','n']:
  print 'Aborded!'


exitCode = 0


