#!/usr/bin/env python
########################################################################
# $HeadURL:  $
# File :   dirac-bookkeeping-filetypes-insert
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

ftype = raw_input("FileType: " )
desc = raw_input("Description: ")
print 'Do you want to add this new file type? (yes or no)'
value = raw_input('Choice:')
choice=value.lower()
if choice in ['yes','y']:
  res = bk.insertFileTypes(ftype, desc)
  if res['OK']:
    print 'The filetypes added successfully!'
  else:
    print "Error discovered!",res['Message']
elif choice in ['no','n']:
  print 'Aborded!'
else:
  print 'Unespected choice:',value
   
DIRAC.exit(exitCode)