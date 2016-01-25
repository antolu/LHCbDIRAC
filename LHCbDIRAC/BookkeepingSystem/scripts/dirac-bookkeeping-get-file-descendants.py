#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :    dirac-bookkeeping-get-file-descendants
# Author :  Zoltan Mathe
########################################################################
"""
  Returns descendants for a (list of) LFN(s)
"""
__RCSID__ = "$Id$"

import DIRAC
from LHCbDIRAC.DataManagementSystem.Client.DMScript import DMScript, Script, printDMResult

if __name__ == "__main__":
  dmScript = DMScript()
  dmScript.registerFileSwitches()
  Script.registerSwitch( '', 'All', 'Do not restrict to descendants with replicas' )
  Script.registerSwitch( '', 'Full', 'Get full metadata information on ancestors' )
  level = 1
  Script.registerSwitch( '', 'Depth=', 'Number of processing levels (default:%d)' % level )
  Script.registerSwitch( '', 'Production=', 'Restrict to descendants in a given production (at any depth)' )
  Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                       'Usage:',
                                       '  %s [option|cfgfile] ... [LFN|File] [Level]' % Script.scriptName,
                                       'Arguments:',
                                       '  LFN:      Logical File Name',
                                       '  File:     Name of the file with a list of LFNs',
                                       '  Level:    Number of levels to search (default: 1)' ] ) )

  Script.parseCommandLine( ignoreErrors = True )

  checkreplica = True
  prod = 0
  full = False
  for switch in Script.getUnprocessedSwitches():
    if switch[0] == 'All':
      checkreplica = False
    elif switch[0] == 'Depth':
      try:
        level = int( switch[1] )
      except:
        print "Invalid value for --Depth: %s", switch[1]
    elif switch[0] == 'Production':
      try:
        prod = int( switch[1] )
      except:
        print "Invalid production: %s" % switch[1]
        prod = 0
    elif switch[0] == 'Full':
      full = True

  args = Script.getPositionalArgs()

  try:
    level = int( args[-1] )
    args.pop()
  except:
    pass

  for lfn in args:
    dmScript.setLFNsFromFile( lfn )
  lfnList = dmScript.getOption( 'LFNs', [] )

  from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
  result = BookkeepingClient().getFileDescendants( lfnList, depth = level, production = prod, checkreplica = checkreplica )
  if result['OK']:
    noDescendants = set( lfnList ) - set( result['Value']['Successful'] ) - set( result['Value']['Failed'] ) - \
                    set( result['Value']['NotProcessed'] )
    if full:
      del result['Value']['Successful']
    else:
      okResult = result['Value']['WithMetadata']
      for lfn in okResult:
        result['Value']['Successful'][lfn] = \
          dict( ( anc, 'Replica-%s' % meta['GotReplica'] ) for anc, meta in okResult[lfn].iteritems() )
      del result['Value']['WithMetadata']
    if noDescendants:
      result['Value']['NoDescendants'] = list( noDescendants )

  DIRAC.exit( printDMResult( result,
                             empty = "None", script = "dirac-bookkeeping-get-file-descendants" ) )


