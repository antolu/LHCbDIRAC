#!/usr/bin/env python
########################################################################
# $HeadURL:  $
# File :   dirac-bookkeeping-get-file-ancestors
# Author : Zoltan Mathe
########################################################################
__RCSID__   = "$Id: $"
__VERSION__ = "$ $"

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

result = bk.getAncestors(lfns, level)

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