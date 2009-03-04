#!/usr/bin/env python
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/BookkeepingSystem/scripts/dirac-bookkeeping-get-file-descendants.py,v 1.1 2009/03/04 08:51:22 zmathe Exp $
# File :   dirac-bookkeeping-get-file-descendants
# Author : Zoltan Mathe
########################################################################
__RCSID__   = "$Id: dirac-bookkeeping-get-file-descendants.py,v 1.1 2009/03/04 08:51:22 zmathe Exp $"
__VERSION__ = "$Revision: 1.1 $"

from DIRACEnvironment import DIRAC
from DIRAC.Core.Base import Script

from DIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient

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
level = str(args[1])
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
  print result['Value']
DIRAC.exit(exitCode)