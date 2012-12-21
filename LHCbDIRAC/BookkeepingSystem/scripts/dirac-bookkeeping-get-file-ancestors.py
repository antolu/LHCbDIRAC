#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :    dirac-bookkeeping-get-file-ancestors
# Author :  Zoltan Mathe
########################################################################
"""
  Report ancestors for the given LFNs
"""
__RCSID__ = "$Id$"

import DIRAC
from DIRAC.Core.Base import Script
from LHCbDIRAC.DataManagementSystem.Client.DMScript import DMScript, printDMResult

from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
dmScript = DMScript()
dmScript.registerFileSwitches()
Script.registerSwitch( '', 'All', 'Do not restrict to descendants with replicas' )
Script.registerSwitch( '', 'Full', 'Get full metadata information on ancestors' )
Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ... [LFN|File] [Level]' % Script.scriptName,
                                     'Arguments:',
                                     '  LFN:      Logical File Name',
                                     '  File:     Name of the file with a list of LFNs',
                                     '  Level:    Number of levels to search (default: 1)' ] ) )

Script.parseCommandLine( ignoreErrors=True )

full = False
checkreplica = True
for switch in Script.getUnprocessedSwitches():
  if switch[0] == 'Full':
    full = True
  elif switch[0] == 'All':
    checkreplica = False

args = Script.getPositionalArgs()

try:
  level = int( args[-1] ) + 1
  args.pop()
except:
  level = 2

lfns = dmScript.getOption( 'LFNs', [] )
lfns += args
lfnList = []
for lfn in lfns:
  try:
    f = open( lfn, 'r' )
    lfnList += [l.split( 'LFN:' )[-1].strip().split()[0].replace( '"', '' ).replace( ',', '' ) for l in f.read().splitlines()]
    f.close()
  except:
    lfnList.append( lfn )

result = BookkeepingClient().getFileAncestors( lfnList, level, replica=checkreplica )

if full:
  del result['Value']['Successful']
else:
  del result['Value']['WithMetadata']
  okResult = result['Value']['Successful']
  for lfn in okResult:
    okResult[lfn] = dict( [( d['FileName'], 'Replica-%s' % d['GotReplica'] ) for d in okResult[lfn]] )

DIRAC.exit( printDMResult( result,
                           empty="None", script="dirac-bookkeeping-get-file-ancestors" ) )

