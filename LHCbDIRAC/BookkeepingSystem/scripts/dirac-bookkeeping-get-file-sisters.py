#!/usr/bin/env python
########################################################################
# $HeadURL: http://svn.cern.ch/guest/dirac/LHCbDIRAC/tags/LHCbDIRAC/v8r2/BookkeepingSystem/scripts/dirac-bookkeeping-get-file-sisters.py $
# File :    dirac-bookkeeping-get-file-sisters
# Author :  Zoltan Mathe
########################################################################
"""
  Report sisters or cousins (i.e. descendant of a parent or ancestor) for a (list of) LFN(s)
"""
__RCSID__ = "$Id: dirac-bookkeeping-get-file-sisters.py 69359 2013-08-08 13:57:13Z phicharp $"

import DIRAC
from LHCbDIRAC.DataManagementSystem.Client.DMScript import DMScript, Script, printDMResult

if __name__ == "__main__":

  dmScript = DMScript()
  dmScript.registerFileSwitches()
  Script.registerSwitch( '', 'All', 'Do not restrict to sisters with replicas' )
  Script.registerSwitch( '', 'Full', 'Get full metadata information on sisters' )
  level = 1
  Script.registerSwitch( '', 'Depth=', 'Number of ancestor levels (default:%d), 2 would be cousins, 3 grand-cousins etc...' % level )
  Script.registerSwitch( '', 'Production=', 'Production to check for sisters (default=same production)' )
  Script.registerSwitch( '', 'AllFileTypes', 'Consider also files with a different type' )
  Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                       'Usage:',
                                       '  %s [option|cfgfile] ... [LFN|File] [Level]' % Script.scriptName ] ) )

  Script.parseCommandLine( ignoreErrors = True )

  checkreplica = True
  prod = 0
  full = False
  sameType = True
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
        prod = 0
    elif switch[0] == 'Full':
      full = True
    elif switch[0] == 'AllFileTypes':
      sameType = False

  args = Script.getPositionalArgs()

  try:
    level = int( args[-1] )
    args.pop()
  except:
    pass
  if level == 1:
    relation = 'NoSister'
  else:
    relation = 'NoCousin'

  for lfn in args:
    dmScript.setLFNsFromFile( lfn )
  lfnList = dmScript.getOption( 'LFNs', [] )

  from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
  bk = BookkeepingClient()

  prodLfns = {}
  if not prod:
    # Get the productions for the input files
    directories = {}
    import os
    for lfn in lfnList:
      directories.setdefault( os.path.dirname( lfn ), [] ).append( lfn )
    res = bk.getDirectoryMetadata( directories.keys() )
    if not res['OK']:
      print "Error getting directories metadata", res['Message']
      DIRAC.exit( 1 )
    for dirName in directories:
      prod = res['Value']['Successful'].get( dirName, [{}] )[0].get( 'Production' )
      if not prod:
        print "Error: could not get production number for %s" % dirName
      else:
        prodLfns.setdefault( prod, [] ).extend( directories[dirName] )
  else:
    prodLfns[prod] = lfnList

  if full:
    resItem = 'WithMetadata'
  else:
    resItem = 'Successful'
  fullResult = { 'OK': True, 'Value': {resItem:{}, relation:set()}}
  resValue = fullResult['Value']

  for prod, lfnList in prodLfns.items():
    if sameType:
      res = bk.getFileMetadata( lfnList )
      if not res['OK']:
        print "Error getting file metadata", res['Message']
        DIRAC.exit( 1 )
      lfnTypes = {}
      for lfn in res['Value'].get( 'Successful', res['Value'] ):
        metadata = res['Value'][lfn]
        lfnTypes[lfn] = metadata['FileType']
    else:
      lfnTypes = dict.fromkeys( lfnList, None )

    # First get ancestors
    result = bk.getFileAncestors( lfnTypes.keys(), level, replica = False )
    if not result['OK']:
      print "Error getting ancestors:", res['Message']
      DIRAC.exit( 1 )

    ancestors = dict( [( anc['FileName'], lfn ) for lfn, ancList in result['Value']['Successful'].items() for anc in ancList] )

    res = bk.getFileDescendants( ancestors.keys(), depth = 999999, production = prod, checkreplica = checkreplica )

    fullResult['OK'] = res['OK']
    if res['OK']:
      for anc, sisters in res['Value']['WithMetadata'].items():
        lfn = ancestors[anc]
        found = False
        for sister in sisters:
          metadata = sisters[sister]
          if sister != lfn and ( not sameType or metadata['FileType'] == lfnTypes[lfn] ):
            if full:
              resValue[resItem].setdefault( lfn, {} ).update( metadata )
            else:
              resValue[resItem].setdefault( lfn, set() ).add( sister )
            found = True
        if not found:
          resValue[relation].add( lfn )
        if lfn in lfnList:
          lfnList.remove( lfn )
      for lfn in lfnList:
        resValue[relation].add( lfn )
      for lfn in [lfn for lfn in resValue[resItem] if lfn in resValue[relation]]:
        resValue[relation].remove( lfn )
    else:
      break

  DIRAC.exit( printDMResult( fullResult,
                             empty = "None", script = "dirac-bookkeeping-get-file-sisters" ) )
