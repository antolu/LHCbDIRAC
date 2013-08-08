#!/usr/bin/env python
########################################################################
# File :    dirac-bookkeeping-get-files.py
# Author :  Zoltan Mathe
########################################################################
"""
  Retrieve files of a BK query
"""
__RCSID__ = "$Id$"
import DIRAC
from DIRAC import gLogger
from LHCbDIRAC.DataManagementSystem.Client.DMScript import DMScript, Script

dmScript = DMScript()
dmScript.registerBKSwitches()
Script.registerSwitch( '', 'Output=', '  Specify a file that will contain the list of files' )
maxFiles = 20
Script.registerSwitch( '', 'MaxFiles=', '   Print only <MaxFiles> lines on stdout (%d if output, else All)' % maxFiles )
Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ... ProdID Type' % Script.scriptName] ) )

Script.parseCommandLine()

output = None
nMax = None
for switch, val in Script.getUnprocessedSwitches():
  if switch == 'Output':
    output = val
  elif switch == 'MaxFiles':
    try:
      nMax = int( val )
    except:
      gLogger.error( 'Invalid integer', val )
      DIRAC.exit( 2 )

bkQuery = dmScript.getBKQuery()

from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient

bk = BookkeepingClient()
gLogger.always( "Using BKQuery:", bkQuery )

useFilesWithMetadata = False
if useFilesWithMetadata:
  res = bk.getFilesWithMetadata( bkQuery.getQueryDict() )
  if not res['OK']:
    gLogger.error( 'ERROR getting the files', res['Message'] )
    DIRAC.exit( 1 )
  if not res['Value']['TotalRecords']:
    gLogger.always( 'No files found for BK query' )
    DIRAC.exit( 0 )
  parameters = res['Value']['ParameterNames']
  fileDict = {}
  for record in res['Value']['Records']:
    dd = dict( zip( parameters, record ) )
    lfn = dd['FileName']
    dd.pop( 'FileName' )
    fileDict[lfn] = dd

else:
  lfns = bkQuery.getLFNs( printSEUsage = False, printOutput = False )
  if not lfns:
    gLogger.always( 'No files found for BKQuery', str( bkQuery ) )
    DIRAC.exit( 0 )

  res = bk.getFileMetadata( lfns )
  if not res['OK']:
    gLogger.error( 'ERROR: failed to get metadata:', res['Message'] )
    DIRAC.exit( 1 )
  fileDict = res['Value']['Successful']

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
                                 'Visible'.ljust( 8 ) )
gLogger.always( outputStr )
nFiles = 0
for lfn in sorted( fileDict ):
  metadata = fileDict[lfn]
  size = metadata['FileSize']
  guid = metadata['GUID']
  hasReplica = metadata['GotReplica']
  visible = metadata.get( 'Visible', '?' )
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
