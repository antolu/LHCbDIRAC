#! /usr/bin/env python
########################################################################
# $HeadURL$
# File :    dirac-dq-monitor-runs.py
# Author :  Zoltan Mathe
########################################################################
"""
  Retrieve from the Bookkeeping runs from a given date range
"""
__RCSID__ = "$Id$"
import DIRAC
from DIRAC.Core.Base import Script
Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ...' % Script.scriptName ] ) )
Script.parseCommandLine( ignoreErrors = True )
from DIRAC.Core.Utilities.List                                  import sortList, breakListIntoChunks
from LHCbDIRAC.NewBookkeepingSystem.Client.BookkeepingClient    import BookkeepingClient
import sys, os, time

bkDict = {'ConfigName'           :'LHCb',
          'ConfigVersion'        :'Collision10',
          'FileType'             :'RAW',
          'ProcessingPass'       : 'Real Data',
          'DataQualityFlag'      : 'UNCHECKED'}

bkClient = BookkeepingClient()
for eventType in [91000000, 90000000]:
  bkDict['EventType'] = eventType
  res = bkClient.getFilesWithGivenDataSets( bkDict )
  if not res['OK']:
    print 'ERROR:', res['Message']
    DIRAC.exit( 2 )
  lfns = res['Value']
  print 'Found %s UNCHECKED files from %s stream' % ( len( lfns ), eventType )

  allMetadata = {}
  for lfnList in breakListIntoChunks( lfns, 1000 ):
    startTime = time.time()
    res = bkClient.getFileMetadata( lfnList )
    if not res['OK']:
      print 'ERROR:', res['Message']
      DIRAC.exit( 2 )
    allMetadata.update( res['Value'] )

  uncheckedRuns = []
  for lfn in sortList( allMetadata.keys() ):
    runNumber = allMetadata[lfn]['RunNumber']
    if not runNumber in uncheckedRuns:
      uncheckedRuns.append( runNumber )
  if uncheckedRuns:
    print 'Corresponding to %s runs:' % len( uncheckedRuns )
    for run in sortList( uncheckedRuns ):
      print run

DIRAC.exit()
