#!/usr/bin/env python
########################################################################
# $HeadURL: svn+ssh://svn.cern.ch/reps/dirac/LHCbDIRAC/branches/LHCbDIRAC/BookkeepingSystem/bk_2010101501/scripts/dirac-bookkeeping-setdataquality-production.py $
# File :   dirac-bookkeeping-setdataquality-production
# Author : Zoltan Mathe
########################################################################
__RCSID__   = "$Id: dirac-bookkeeping-setdataquality-production.py 18177 2009-11-11 14:02:57Z zmathe $"
__VERSION__ = "$ $"

import DIRAC
from DIRAC.Core.Base import Script

from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient

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
  print 'Usage: %s <Production> <DataQualityFlag>' %(Script.scriptName)
  DIRAC.exit(2)

if len(args) < 2: 
  usage()

exitCode = 0
prod = int(args[0])
flag = str(args[1])
result = bk.setQualityProduction(prod,flag)

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