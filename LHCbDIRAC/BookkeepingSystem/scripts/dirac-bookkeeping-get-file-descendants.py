#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :    dirac-bookkeeping-get-file-descendants
# Author :  Zoltan Mathe
########################################################################
"""
  Report descendants for the given LFNs
"""
__RCSID__ = "$Id$"

import DIRAC
from DIRAC.Core.Base import Script

from LHCbDIRAC.DataManagementSystem.Client.DMScript import DMScript, printDMResult

from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
dmScript = DMScript()
dmScript.registerFileSwitches()
Script.registerSwitch( '', 'Production=', 'Restrict to descendants in a given production' )
Script.registerSwitch( '', 'All', 'Do not restrict to descendants with replicas' )
level = 1
Script.registerSwitch( '', 'Depth=', 'Number of processing levels (default:%d)' % level )
Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ... LFN|File [Level]' % Script.scriptName,
                                     'Arguments:',
                                     '  LFN:      Logical File Name',
                                     '  File:     Name of the file with a list of LFNs',
                                     '  Level:    Number of levels to search (default: 1)' ] ) )

Script.parseCommandLine( ignoreErrors=True )

checkreplica = True
prod = 0
for switch in Script.getUnprocessedSwitches():
  if switch[0] == 'All':
    checkreplica = False
  elif switch[0] == 'Depth':
    level = int( switch[1] )
  elif switch[0] == 'Production':
    try:
      prod = int( switch[1] )
    except:
      prod = None

args = Script.getPositionalArgs()

try:
  level = int( args[-1] )
  args.pop()
except:
  pass

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

result = BookkeepingClient().getFileDescendants( lfnList, depth=level, production=prod, checkreplica=checkreplica )

DIRAC.exit( printDMResult( result,
                           empty="None", script="dirac-bookkeeping-get-file-descendants" ) )


