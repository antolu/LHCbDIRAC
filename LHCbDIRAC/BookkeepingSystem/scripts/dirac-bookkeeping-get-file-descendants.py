#!/usr/bin/env python
########################################################################
# $HeadURL: svn+ssh://svn.cern.ch/reps/dirac/LHCbDIRAC/branches/LHCbDIRAC/BookkeepingSystem/bk_2010101501/scripts/dirac-bookkeeping-get-file-descendants.py $
# File :   dirac-bookkeeping-get-file-descendants
# Author : Zoltan Mathe
########################################################################
__RCSID__   = "$Id: dirac-bookkeeping-get-file-descendants.py 18177 2009-11-11 14:02:57Z zmathe $"
__VERSION__ = "$Revision: 1.3 $"

import DIRAC
from DIRAC.Core.Base import Script

from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient

Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()

bk = BookkeepingClient()

def usage():
  print 'Usage: %s <lfn> or <file name> <level>' %(Script.scriptName)
  DIRAC.exit(2)

if len(args) < 2: 
  usage()

exitCode = 0
file = str(args[0])
level = int(args[1])
lfns = []
try:
  files =open(file)
  for f in files:
    lfns +=  [f.strip()]
except Exception,ex:
  lfns = [file]

result = bk.getDescendents(lfns, level)

if not result['OK']:
  print 'ERROR %s' %(result['Message'])
  exitCode = 2
else:
  values = result['Value']
  print 'Successful:'
  files = values['Successful']
  for i in files.keys():
    print i+':'
    for j in files[i]:
      print '                 '+j
  print 'Faild:',values['Failed']
  
DIRAC.exit(exitCode)