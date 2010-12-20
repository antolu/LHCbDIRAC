#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :    dirac-bookkeeping-eventtype-mgt-update
# Author :  Zoltan Mathe
########################################################################
"""
  Retrieve metadata from the Bookkeeping for the given files
"""
__RCSID__ = "$Id$"
import  DIRAC.Core.Base.Script as Script
Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ... LFN|File' % Script.scriptName,
                                     'Arguments:',
                                     '  LFN:      Logical File Name',
                                     '  File:     Name of the file with a list of LFNs' ] ) )
Script.parseCommandLine()

import DIRAC
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.Core.Utilities.List import sortList
import os, sys

args = Script.getPositionalArgs()

if len( args ) != 1:
  Script.showHelp()

file = args[0]
lfns = []
try:
  files = open( file )
  for f in files:
    lfns += [f.strip()]
except Exception, ex:
  lfns = [file]

client = RPCClient( 'Bookkeeping/NewBookkeepingManager' )
res = client.getFileMetadata( lfns )
if not res['OK']:
  print 'ERROR: Failed to get file metadata: %s' % res['Message']
  DIRAC.exit( 2 )

exitCode = 0

print '%s %s %s %s %s %s' % ( 'FileName'.ljust( 100 ), 'Size'.ljust( 10 ), 'GUID'.ljust( 40 ), 'Replica'.ljust( 10 ), 'DataQuality'.ljust( 10 ), 'RunNumber'.ljust( 10 ) )
for lfn in res['Value'].keys():
  dict = res['Value'][lfn]
  size = dict['FileSize']
  guid = dict['GUID']
  gotReplica = dict['GotReplica']
  dq = dict['DQFlag']
  run = dict['RunNumber']
  if not gotReplica:
    gotReplica = 'No'
  print  '%s %s %s %s %s %s' % ( lfn.ljust( 100 ), str( size ).ljust( 10 ), guid.ljust( 40 ), gotReplica.ljust( 10 ), dq.ljust( 10 ), str( run ).ljust( 10 ) )
  lfns.remove( lfn )

if lfns: print '\n'
for lfn in lfns:
  if lfn:
    print '%s does not exist in the Bookkeeping.' % lfn
    exitCode = 2

DIRAC.exit( exitCode )
