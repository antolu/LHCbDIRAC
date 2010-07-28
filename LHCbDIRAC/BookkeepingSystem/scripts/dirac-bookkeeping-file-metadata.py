#!/usr/bin/env python
import  DIRAC.Core.Base.Script as Script
Script.parseCommandLine()
########################################################################
# $HeadURL$
########################################################################
__RCSID__   = "$Id$"
__VERSION__ = "$ $"

from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.Core.Utilities.List import sortList
client = RPCClient('Bookkeeping/BookkeepingManager')
import os,sys

args = Script.getPositionalArgs()

if not len(args) == 1:
  print 'Usage: ./dirac-bookkeeping-file-metadata.py <lfn | fileContainingLfns>'
  sys.exit()
else:
  inputFileName = args[0]

if os.path.exists(inputFileName):
  inputFile = open(inputFileName,'r')
  string = inputFile.read()
  lfns = string.splitlines()
  inputFile.close()
else:
  lfns = [inputFileName]

res = client.getFileMetadata(lfns)
if not res['OK']:
  print 'Failed to get file metadata: %s' % res['Message']
  sys.exit()

print '%s %s %s %s %s %s' % ('FileName'.ljust(100),'Size'.ljust(10),'GUID'.ljust(40),'Replica'.ljust(10),'DataQuality'.ljust(10), 'RunNumber'.ljust(10))
for lfn in res['Value'].keys():
  dict = res['Value'][lfn]
  size = dict['FileSize']
  guid = dict['GUID']
  gotReplica = dict['GotReplica']
  dq = dict['DQFlag']
  run = dict['RunNumber']
  if not gotReplica: 
    gotReplica = 'No'
  print  '%s %s %s %s %s %s' % (lfn.ljust(100),str(size).ljust(10),guid.ljust(40),gotReplica.ljust(10),dq.ljust(10), str(run).ljust(10))
  lfns.remove(lfn)

if lfns: print '\n'
for lfn in lfns:
  if lfn:
    print '%s does not exist in the Bookkeeping.' % lfn
