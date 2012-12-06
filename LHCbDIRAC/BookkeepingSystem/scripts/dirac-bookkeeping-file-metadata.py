#!/usr/bin/env python
########################################################################
# File :    dirac-bookkeeping-eventtype-mgt-update
# Author :  Zoltan Mathe
########################################################################
"""
  Retrieve metadata from the Bookkeeping for the given files
"""
__RCSID__ = "$Id$"
import  DIRAC.Core.Base.Script as Script
Script.registerSwitch( '', 'Full', '   Print out all metadata' )
Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ... LFN|File' % Script.scriptName,
                                     'Arguments:',
                                     '  LFN:      Logical File Name',
                                     '  File:     Name of the file with a list of LFNs' ] ) )
Script.parseCommandLine()

import DIRAC
from DIRAC.Core.DISET.RPCClient import RPCClient

full = False
switches = Script.getUnprocessedSwitches()
for switch in switches:
  if switch[0] == 'Full':
    full = True

args = Script.getPositionalArgs()

if len( args ) == 0:
  Script.showHelp()

fileNames = args
lfns = []
for fileName in fileNames:
  try:
    files = open( fileName )
    for f in files:
      lfns += [f.strip()]
  except Exception, ex:
    lfns.append( fileName )

client = RPCClient( 'Bookkeeping/BookkeepingManager' )
res = client.getFileMetadata( lfns )
if not res['OK']:
  print 'ERROR: Failed to get file metadata: %s' % res['Message']
  DIRAC.exit( 2 )

exitCode = 0

lenName = 0
for lfn in lfns:
  lenName = max( lenName, len( lfn ) )
lenName += 2
lenGUID = 38
lenItem = 15
sep = ''

if not full:
  print '%s %s %s %s %s %s %s' % ( 'FileName'.ljust( lenName ),
                                'Size'.ljust( 10 ),
                                'GUID'.ljust( lenGUID ),
                                'Replica'.ljust( 8 ),
                                'DataQuality'.ljust( 12 ),
                                'RunNumber'.ljust( 10 ),
                                '#events'.ljust( 10 ) )
for lfn in res['Value'].keys():
  dict = res['Value'][lfn]
  if full:
    print '%s%s %s' % ( sep, 'FileName'.ljust( lenItem ), lfn )
    sep = '\n'
    for item in sorted( dict ):
      print '%s %s' % ( item.ljust( lenItem ), dict[item] )
  else:
    size = dict['FileSize']
    guid = dict['GUID']
    gotReplica = dict['GotReplica']
    dq = dict['DQFlag']
    run = dict['RunNumber']
    evtStat = dict['EventStat']
    if not gotReplica:
      gotReplica = 'No'
    print  '%s %s %s %s %s %s %s' % ( lfn.ljust( lenName ),
                                   str( size ).ljust( 10 ),
                                   guid.ljust( lenGUID ),
                                   gotReplica.ljust( 8 ),
                                   dq.ljust( 12 ),
                                   str( run ).ljust( 10 ),
                                   str( evtStat ).ljust( 10 ) )
  lfns.remove( lfn )

if lfns:
  print '\n'
for lfn in lfns:
  if lfn:
    print '%s does not exist in the Bookkeeping.' % lfn
    exitCode = 2

DIRAC.exit( exitCode )

