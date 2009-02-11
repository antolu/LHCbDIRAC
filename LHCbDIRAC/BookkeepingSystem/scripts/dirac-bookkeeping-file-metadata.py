#!/usr/bin/env python
from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/BookkeepingSystem/scripts/dirac-bookkeeping-file-metadata.py,v 1.1 2009/02/11 11:54:29 acsmith Exp $
# File :   dirac-bookkeeping-get-production-files.py  
########################################################################
__RCSID__   = "$Id: dirac-bookkeeping-file-metadata.py,v 1.1 2009/02/11 11:54:29 acsmith Exp $"
__VERSION__ = "$ $"

from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.Core.Utilities.List import sortList
client = RPCClient('Bookkeeping/BookkeepingManager')
import os,sys

if not len(sys.argv) == 2:
  print 'Usage: ./dirac-bookkeeping-file-metadata.py <lfn | fileContainingLfns>'
  sys.exit()
else:
  inputFileName = sys.argv[1]

if os.path.exists(inputFileName):
  inputFile = open(inputFileName,'r')
  string = inputFile.read()
  lfns = string.splitlines()
  inputFile.close()
else:
  lfns = [inputFileName]

res = client.getFileMetadata(lfns)
print '%s %s %s %s' % ('FileName'.ljust(100),'Size'.ljust(10),'GUID'.ljust(40),'Replica'.ljust(10))
for lfn in res['Value'].keys():
  dict = res['Value'][lfn]
  size = dict['FileSize']
  guid = dict['GUID']
  gotReplica = dict['GotReplica']
  if not gotReplica: 
    gotReplica = 'No'
  print  '%s %s %s %s' % (lfn.ljust(100),str(size).ljust(10),guid.ljust(40),gotReplica.ljust(10))
  lfns.remove(lfn)

if lfns:
  print '\n'
for lfn in lfns:
  if lfn:
    print '%s does not exist in the Bookkeeping.' % lfn

