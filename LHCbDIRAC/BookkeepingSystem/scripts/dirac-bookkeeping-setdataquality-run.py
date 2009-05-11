#!/usr/bin/env python
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/BookkeepingSystem/scripts/dirac-bookkeeping-setdataquality-run.py,v 1.2 2009/05/11 11:21:52 zmathe Exp $
# File :   dirac-bookkeeping-setdataquality-run
# Author : Zoltan Mathe
########################################################################
__RCSID__   = "$Id: dirac-bookkeeping-setdataquality-run.py,v 1.2 2009/05/11 11:21:52 zmathe Exp $"
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
  print 'Usage: %s <RunNumber> <DataQualityFlag>' %(Script.scriptName)
  DIRAC.exit(2)

if len(args) < 2: 
  usage()

exitCode = 0
rnb = int(args[0])
flag = str(args[1])
result = bk.setQualityRun(rnb,flag)

if not result['OK']:
  print 'ERROR %s' %(result['Message'])
  exitCode = 2
else:
  succ = result['Value']['Successful']
  failed = result['Value']['Failed']
  print 'The data quality seted to the following files:'
  for i in succ:
    print i
  
  if len(failed) != 0:
    print 'The data quality has been not seted to the following files:'
    for i in failed:
      print i

DIRAC.exit(exitCode)