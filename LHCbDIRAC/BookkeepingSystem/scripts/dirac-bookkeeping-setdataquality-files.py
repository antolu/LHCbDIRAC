#!/usr/bin/env python
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/BookkeepingSystem/scripts/dirac-bookkeeping-setdataquality-files.py,v 1.2 2009/03/03 16:30:52 zmathe Exp $
# File :   dirac-bookkeeping-setdataquality-files
# Author : Zoltan Mathe
########################################################################
__RCSID__   = "$Id: dirac-bookkeeping-setdataquality-files.py,v 1.2 2009/03/03 16:30:52 zmathe Exp $"
__VERSION__ = "$Revision: 1.2 $"

from DIRACEnvironment import DIRAC
from DIRAC.Core.Base import Script

from DIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient

Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()

bk = BookkeepingClient()

def usage():
  print 'Available data quality flags:'
   
  result = bk.getAvailableDataQuality()
  if not result['OK']:
    print 'ERROR %s' %(result['Message'])
    exitCode = 2
  
  for i in result['Value']:
    print i
  print 'Usage: %s <lfn> or <file name> <DataQualityFlag>' %(Script.scriptName)
  DIRAC.exit(2)

if len(args) < 2: 
  usage()

exitCode = 0
file = str(args[0])
flag = str(args[1])
lfns = []
try:
  files =open(file)
  for f in files:
    lfns +=  [f.strip()]
except Exception,ex:
  lfns = [file]

result = bk.setQuality(lfns,flag)

if not result['OK']:
  print 'ERROR %s' %(result['Message'])
  exitCode = 2
else:
  print result['Value']
DIRAC.exit(exitCode)