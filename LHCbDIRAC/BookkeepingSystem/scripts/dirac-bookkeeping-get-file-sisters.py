#!/usr/bin/env python
########################################################################
# $HeadURL: svn+ssh://svn.cern.ch/reps/dirac/LHCbDIRAC/branches/LHCbDIRAC_v7r10_branch/BookkeepingSystem/scripts/dirac-bookkeeping-get-file-sisters.py $
# File :    dirac-bookkeeping-get-file-sisters
# Author :  Zoltan Mathe
########################################################################
"""
  Report sisters (i.e. descendant of ancestor) for the given LFNs
"""
__RCSID__ = "$Id: dirac-bookkeeping-get-file-sisters.py 60156 2012-12-19 14:48:55Z phicharp $"

import DIRAC
from DIRAC.Core.Base import Script

from LHCbDIRAC.DataManagementSystem.Client.DMScript import DMScript, printDMResult

from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
dmScript = DMScript()
dmScript.registerFileSwitches()
Script.registerSwitch( '', 'Production=', 'Production to check for sisters (default=same production)' )
Script.registerSwitch( '', 'All', 'Do not restrict to sisters with replicas' )
Script.registerSwitch( '', 'Full', 'Get full metadata information on sisters' )
Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ... [LFN|File] [Level]' % Script.scriptName ] ) )

Script.parseCommandLine( ignoreErrors = True )

checkreplica = True
prod = 0
full = False
for switch in Script.getUnprocessedSwitches():
  if switch[0] == 'All':
    checkreplica = False
  elif switch[0] == 'Production':
    try:
      prod = int( switch[1] )
    except:
      prod = None
  elif switch[0] == 'Full':
    full = True

args = Script.getPositionalArgs()

for lfn in Script.getPositionalArgs():
  dmScript.setLFNsFromFile( lfn )
lfnList = dmScript.getOption( 'LFNs', [] )

bk = BookkeepingClient()
# Get file type

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
fullResult = { 'OK': True, 'Value': {resItem:{}, 'NoSister':[]}}
resValue = fullResult['Value']

for prod, lfnList in prodLfns.items():
  res = bk.getFileMetadata( lfnList )
  if not res['OK']:
    print "Error getting file metadata", res['Message']
    DIRAC.exit( 1 )
  lfnTypes = {}
  for lfn in res['Value'].get( 'Successful', res['Value'] ):
    metadata = res['Value'][lfn]
    lfnTypes[lfn] = metadata['FileType']

  #First get ancestors
  result = bk.getFileAncestors( lfnTypes.keys(), 1, replica = False )
  if not result['OK']:
    print "Error getting ancestors:", res['Message']
    DIRAC.exit( 1 )

  ancestors = dict( [( anc['FileName'], lfn ) for lfn, ancList in result['Value']['Successful'].items() for anc in ancList] )

  res = bk.getFileDescendants( ancestors.keys(), depth = 1, production = prod, checkreplica = checkreplica )

  fullResult['OK'] = res['OK']
  if res['OK']:
    for anc, sisters in res['Value']['WithMetadata'].items():
      lfn = ancestors[anc]
      found = False
      for sister, metadata in sisters.items():
        if lfn != sister and metadata['FileType'] == lfnTypes[lfn]:
          if full:
            resValue[resItem].setdefault( lfn, {} ).update( sisters[sister] )
          else:
            resValue[resItem].setdefault( lfn, [] ).append( sister )
          found = True
      if not found and lfn not in resValue['NoSister']:
        resValue['NoSister'].append( lfn )
      if lfn in lfnList:
        lfnList.remove( lfn )
    for lfn in lfnList:
      resValue['NoSister'].append( lfn )
  else:
    break

DIRAC.exit( printDMResult( fullResult,
                           empty = "None", script = "dirac-bookkeeping-get-file-sisters" ) )
