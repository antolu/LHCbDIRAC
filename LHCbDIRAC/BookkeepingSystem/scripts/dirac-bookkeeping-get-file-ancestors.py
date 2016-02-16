#!/usr/bin/env python
########################################################################
# $HeadURL: http://svn.cern.ch/guest/dirac/LHCbDIRAC/tags/LHCbDIRAC/v8r2p5/BookkeepingSystem/scripts/dirac-bookkeeping-get-file-ancestors.py $
# File :    dirac-bookkeeping-get-file-ancestors
# Author :  Zoltan Mathe
########################################################################
"""
  returns ancestors for a (list of) LFN(s)
"""
__RCSID__ = "$Id: dirac-bookkeeping-get-file-ancestors.py 69961 2013-09-12 10:36:33Z phicharp $"

import DIRAC
from LHCbDIRAC.DataManagementSystem.Client.DMScript import DMScript, Script, printDMResult

if __name__ == "__main__":
  dmScript = DMScript()
  dmScript.registerFileSwitches()
  level = 1
  Script.registerSwitch( '', 'All', 'Do not restrict to ancestors with replicas' )
  Script.registerSwitch( '', 'Full', 'Get full metadata information on ancestors' )
  Script.registerSwitch( '', 'Depth=', 'Number of processing levels (default:%d)' % level )
  Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                       'Usage:',
                                       '  %s [option|cfgfile] ... [LFN|File] [Level]' % Script.scriptName,
                                       'Arguments:',
                                       '  LFN:      Logical File Name',
                                       '  File:     Name of the file with a list of LFNs',
                                       '  Level:    Number of levels to search (default: %d)' % level ] ) )

  Script.parseCommandLine( ignoreErrors = True )

  full = False
  checkreplica = True
  for switch in Script.getUnprocessedSwitches():
    if switch[0] == 'Full':
      full = True
    elif switch[0] == 'All':
      checkreplica = False
    elif switch[0] == 'Depth':
      try:
        level = int( switch[1] )
      except:
        print "Invalid value for --Depth: %s", switch[1]

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
  result = BookkeepingClient().getFileAncestors( lfnList, level, replica = checkreplica )

  if result['OK']:
    if full:
      del result['Value']['Successful']
    else:
      okResult = result['Value']['WithMetadata']
      for lfn in okResult:
        result['Value']['Successful'][lfn] = \
          dict( [( desc, 'Replica-%s' % meta['GotReplica'] ) for desc, meta in okResult[lfn].items()] )
      del result['Value']['WithMetadata']

  DIRAC.exit( printDMResult( result,
                             empty = "None", script = "dirac-bookkeeping-get-file-ancestors" ) )

