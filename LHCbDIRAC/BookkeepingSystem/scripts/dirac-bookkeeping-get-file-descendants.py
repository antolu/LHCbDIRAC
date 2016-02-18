#!/usr/bin/env python
########################################################################
# File :    dirac-bookkeeping-get-file-descendants
# Author :  Zoltan Mathe
########################################################################
"""
  Returns descendants for a (list of) LFN(s)
"""
__RCSID__ = "$Id$"

import DIRAC
from LHCbDIRAC.DataManagementSystem.Client.DMScript import DMScript, Script, printDMResult, ProgressBar
from DIRAC import S_OK

if __name__ == "__main__":
  dmScript = DMScript()
  dmScript.registerFileSwitches()
  Script.registerSwitch( '', 'All', 'Do not restrict to descendants with replicas' )
  Script.registerSwitch( '', 'Full', 'Get full metadata information on descendants' )
  level = 1
  Script.registerSwitch( '', 'Depth=', 'Number of processing levels (default:%d)' % level )
  Script.registerSwitch( '', 'Production=', 'Restrict to descendants in a given production (at any depth)' )
  Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                       'Usage:',
                                       '  %s [option|cfgfile] ... [LFN|File] [Depth]' % Script.scriptName,
                                       'Arguments:',
                                       '  LFN:      Logical File Name',
                                       '  File:     Name of the file with a list of LFNs',
                                       '  Depth:    Number of levels to search (default: %d)' % level ] ) )

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
  from DIRAC.Core.Utilities.List import breakListIntoChunks
  bkClient = BookkeepingClient()

  chunkSize = 50
  progressBar = ProgressBar( len( lfnList ), chunk = chunkSize, title = 'Getting descendants for %d files (depth %d)' % ( len( lfnList ), level ) + ( ' for production %d' % prod if prod else '' ) )
  fullResult = S_OK( {} )
  for lfnChunk in breakListIntoChunks( lfnList, 50 ):
    progressBar.loop()
    result = bkClient.getFileDescendants( lfnChunk, depth = level, production = prod, checkreplica = checkreplica )
    if result['OK']:
      noDescendants = set( lfnChunk ) - set( result['Value']['Successful'] ) - set( result['Value']['Failed'] ) - \
                      set( result['Value']['NotProcessed'] )
      if noDescendants:
        fullResult['Value'].setdefault( 'NoDescendants', [] ).extend( sorted( noDescendants ) )
      if full:
        fullResult['Value'].setdefault( 'WithMetadata', {} ).update( result['Value']['WithMetadata'] )
      else:
        okResult = result['Value']['WithMetadata']
        for lfn in okResult:
          fullResult['Value'].setdefault( 'Successful', {} )[lfn] = \
            dict( ( desc, 'Replica-%s' % meta['GotReplica'] ) for desc, meta in okResult[lfn].iteritems() )
      fullResult['Value'].setdefault( 'Failed', {} ).update( result['Value']['Failed'] )
      fullResult['Value'].setdefault( 'NotProcessed', [] ).extend( result['Value']['NotProcessed'] )
    else:
      fullResult = result
      break
  progressBar.endLoop()

  DIRAC.exit( printDMResult( fullResult,
                             empty = "None", script = "dirac-bookkeeping-get-file-descendants" ) )
