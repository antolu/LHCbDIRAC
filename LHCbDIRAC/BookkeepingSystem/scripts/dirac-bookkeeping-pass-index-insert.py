#!/usr/bin/env python
########################################################################
# $HeadURL: svn+ssh://svn.cern.ch/reps/dirac/LHCbDIRAC/branches/LHCbDIRAC/BookkeepingSystem/bk_2010101501/scripts/dirac-bookkeeping-pass-index-insert.py $
# File :   dirac-bookkeeping-pass-index-insert.py
# Author : Zoltan Mathe
########################################################################
__RCSID__   = "$Id: dirac-bookkeeping-pass-index-insert.py 18177 2009-11-11 14:02:57Z zmathe $"
__VERSION__ = "$ $"

import sys,string,re
import DIRAC
from DIRAC.Core.Base import Script

Script.parseCommandLine( ignoreErrors = True )

from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
bk = BookkeepingClient()

res = bk.getProcessingPassGroups()
if res['OK']:
  width = 20
  print str('GroupId').ljust(15)+str('Group description').ljust(30)
  for record in res['Value']:
    print str(record[0]).ljust(15)+str(record[1]).ljust(30)
else:
  print 'error listing processing descriptions'

print 'Please write a description (existing or not)'
groupdesc = raw_input("Group description: ")
res = raw_input("How many steps do you have?")
nb = int(res)
steps = {}
for i in range(nb):
  print 'Step '+str(i)
  appname = raw_input("Application Name: ")
  appversion = raw_input("Application Version: ")
  opts = raw_input('Option files:')
  dddb = raw_input("DDDb tag: ")
  condb = raw_input("CondDb tag: ")
  extrapack = raw_input("Extra packages: ")
  steps[i]=[appname,appversion,opts,dddb,condb,extrapack]

print 'Do you want to add this new pass_index conditions? (yes or no)'
value = raw_input('Choice: ')
choice=value.lower()
if choice in ['yes','y']:
    keys = steps.keys()
    keys.sort()
    s = ['NULL', 'NULL', 'NULL', 'NULL', 'NULL', 'NULL', 'NULL', 'NULL']
    i = 0
    for i in keys:
      res = bk.insert_aplications(appname, appversion, opts, dddb, condb, extrapack)
      if not res['OK']:
        print res['Message']
      else:
        s[i] = res['Value']
        i += 1
    
    res = bk.insert_pass_index_new(groupdesc, str(s[0]), str(s[1]), str(s[2]), str(s[3]), str(s[4]), str(s[5]),str(s[6]))
    if not res['OK']:
      print res['Message']
    else:
      print 'The pass_index added successfully!'
      print 'The new passid ',res['Value']

elif choice in ['no','n']:
  print 'Aborded!'

exitCode = 0


