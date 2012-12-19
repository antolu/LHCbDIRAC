#!/usr/bin/env python
########################################################################
# File :    dirac-bookkeeping-filetypes-insert.py
# Author :  Zoltan Mathe
########################################################################
"""
  Insert new file types in the Bookkeeping
"""
__RCSID__ = "$Id$"

import DIRAC
from DIRAC.Core.Base import Script

Script.setUsageMessage('\n'.join([ __doc__.split('\n')[1],
                                     'Usage:',
                                     '  %s [option|cfgfile]' % Script.scriptName ]))
Script.parseCommandLine(ignoreErrors=True)

from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
bk = BookkeepingClient()

exitCode = 0

ftype = raw_input("FileType: ")
desc = raw_input("Description: ")
version = raw_input("File type version: ")
print 'Do you want to add this new file type? (yes or no)'
value = raw_input('Choice:')
choice = value.lower()
if choice in ['yes', 'y']:
  res = bk.insertFileTypes(ftype.upper(), desc, version)
  if res['OK']:
    print 'The file types added successfully!'
  else:
    print "Error discovered!", res['Message']
elif choice in ['no', 'n']:
  print 'Aborded!'
else:
  print 'Unexpected choice:', value

DIRAC.exit(exitCode)

