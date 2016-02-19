#!/usr/bin/env python
########################################################################
# File :    dirac-bookkeeping-get-file-ancestors
# Author :  Zoltan Mathe
########################################################################
"""
  returns ancestors for a (list of) LFN(s)
"""
__RCSID__ = "$Id$"

import DIRAC
from LHCbDIRAC.DataManagementSystem.Client.DMScript import DMScript, Script, printDMResult, ProgressBar
from DIRAC import S_OK

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
  from DIRAC.Core.Utilities.List import breakListIntoChunks
  bkClient = BookkeepingClient()

  chunkSize = 50
  progressBar = ProgressBar( len( lfnList ), chunk = chunkSize, title = 'Getting ancestors for %d files (depth %d)' % ( len( lfnList ), level ) )
  fullResult = S_OK( {} )
  for lfnChunk in breakListIntoChunks( lfnList, 50 ):
    progressBar.loop()
    result = bkClient.getFileAncestors( lfnChunk, level, replica = checkreplica )

    if result['OK']:
      if full:
        fullResult['Value'].setdefault( 'WithMetadata', {} ).update( result['Value']['WithMetadata'] )
      else:
        okResult = result['Value']['WithMetadata']
        for lfn in okResult:
          fullResult['Value'].setdefault( 'Successful', {} )[lfn] = \
            dict( ( desc, 'Replica-%s' % meta['GotReplica'] ) for desc, meta in okResult[lfn].iteritems() )
      fullResult['Value'].setdefault( 'Failed', {} ).update( result['Value']['Failed'] )
    else:
      fullResult = result
      break
  progressBar.endLoop()

  DIRAC.exit( printDMResult( fullResult,
                             empty = "None", script = "dirac-bookkeeping-get-file-ancestors" ) )
