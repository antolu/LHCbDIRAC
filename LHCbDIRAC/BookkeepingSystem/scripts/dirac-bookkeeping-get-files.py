#!/usr/bin/env python
########################################################################
# File :    dirac-bookkeeping-get-files.py
# Author :  Zoltan Mathe
########################################################################
"""
  Retrieve files of a BK query
"""
__RCSID__ = "$Id: dirac-bookkeeping-get-files.py 83859 2015-06-25 12:12:40Z phicharp $"
import DIRAC
import os
from DIRAC import gLogger
from LHCbDIRAC.DataManagementSystem.Client.DMScript import DMScript, Script
from LHCbDIRAC.BookkeepingSystem.Client.BKQuery import BKQuery

dmScript = DMScript()
dmScript.registerBKSwitches()
Script.registerSwitch( '', 'File=', '   Provide a list of BK paths' )
Script.registerSwitch( '', 'Term', '   Provide the list of BK paths from terminal' )
Script.registerSwitch( '', 'Output=', '  Specify a file that will contain the list of files' )
maxFiles = 20
Script.registerSwitch( '', 'MaxFiles=', '   Print only <MaxFiles> lines on stdout (%d if output, else All)' % maxFiles )
Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ... ProdID Type' % Script.scriptName] ) )

Script.parseCommandLine()

output = None
nMax = None
bkFile = None
for switch, val in Script.getUnprocessedSwitches():
  if switch == 'Output':
    output = val
  elif switch == 'File':
    bkFile = val
  elif switch == 'Term':
    bkFile = '/dev/stdin'
  elif switch == 'MaxFiles':
    try:
      nMax = int( val )
    except:
      gLogger.error( 'Invalid integer', val )
      DIRAC.exit( 2 )

bkQueries = []
bkQuery = dmScript.getBKQuery()
if bkQuery:
  bkQueries.append( bkQuery )
if bkFile:
  if os.path.exists( bkFile ):
    lines = open( bkFile, 'r' ).readlines()
    for ll in lines:
      bkQueries.append( BKQuery( ll.strip().split()[0] ) )

if not bkQueries:
  gLogger.always( "No BK query given, use --BK <bkPath> or --BKFile <localFile>" )
  DIRAC.exit( 1 )

fileDict = {}

from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
from DIRAC.Core.Utilities.List import breakListIntoChunks

bk = BookkeepingClient()

for bkQuery in bkQueries:
  gLogger.always( "Using BKQuery:", bkQuery )

  useFilesWithMetadata = False
  if useFilesWithMetadata:
    res = bk.getFilesWithMetadata( bkQuery.getQueryDict() )
    if not res['OK']:
      gLogger.error( 'ERROR getting the files', res['Message'] )
      DIRAC.exit( 1 )
    parameters = res['Value']['ParameterNames']
    for record in res['Value']['Records']:
      dd = dict( zip( parameters, record ) )
      lfn = dd['FileName']
      dd.pop( 'FileName' )
      fileDict[lfn] = dd

  else:
    lfns = bkQuery.getLFNs( printSEUsage = False, printOutput = False )

    for lfnChunk in breakListIntoChunks( lfns, 1000 ):
      res = bk.getFileMetadata( lfnChunk )
      if not res['OK']:
        gLogger.error( 'ERROR: failed to get metadata:', res['Message'] )
        DIRAC.exit( 1 )
      fileDict.update( res['Value']['Successful'] )

if not fileDict:
  gLogger.always( 'No files found for BK query' )
  DIRAC.exit( 0 )

# Now print out
nFiles = len( fileDict )
if output:
  f = open( output, 'w' )
  if not nMax:
    nMax = maxFiles
else:
  if not nMax:
    nMax = nFiles
gLogger.always( '%d files found' % nFiles, '(showing only first %d files):' % nMax if nFiles > nMax else ':' )
outputStr = '%s %s %s %s %s' % ( 'FileName'.ljust( 100 ),
                                 'Size'.ljust( 10 ),
                                 'GUID'.ljust( 40 ),
                                 'Replica'.ljust( 8 ),
                                 'Visibility'.ljust( 8 ) )
gLogger.always( outputStr )
nFiles = 0
for lfn in sorted( fileDict ):
  metadata = fileDict[lfn]
  size = metadata['FileSize']
  guid = metadata['GUID']
  hasReplica = metadata['GotReplica']
  visible = metadata.get( 'VisibilityFlag', '?' )
  outputStr = '%s %s %s %s %s' % ( lfn.ljust( 100 ),
                                   str( size ).ljust( 10 ),
                                   guid.ljust( 40 ),
                                   str( hasReplica ).ljust( 8 ),
                                   str( visible ).ljust( 8 ) )
  if output:
    f.write( outputStr + '\n' )
  if nFiles < nMax:
    nFiles += 1
    gLogger.always( outputStr )

if output:
  f.close()
  gLogger.always( '\nList of %d files saved in' % len( fileDict ), output )
