#! /usr/bin/env python
from DIRAC.Core.Base.Script                                     import parseCommandLine
parseCommandLine()
from DIRAC.Core.Utilities.List                                  import sortList,breakListIntoChunks
from LHCbDIRAC.NewBookkeepingSystem.Client.BookkeepingClient    import BookkeepingClient
import sys,os,time

bkDict = {'ConfigName'           :'LHCb',
          'ConfigVersion'        :'Collision10',
          'FileType'             :'RAW',
          'ProcessingPass'       : 'Real Data',
          'DataQualityFlag'      : 'UNCHECKED'}

bkClient = BookkeepingClient()
for eventType in [91000000,90000000]:
  bkDict['EventType'] = eventType
  res=bkClient.getFilesWithGivenDataSets(bkDict)
  if not res['OK']:
    print res['Message']
    sys.exit()
  lfns = res['Value']
  print 'Found %s UNCHECKED files from %s stream' % (len(lfns),eventType)

  allMetadata = {}
  for lfnList in breakListIntoChunks(lfns,1000):
    startTime = time.time()
    res=bkClient.getFileMetadata(lfnList)
    if not res['OK']:
      print res['Message']
      sys.exit()
    allMetadata.update(res['Value'])

  uncheckedRuns = []
  for lfn in sortList(allMetadata.keys()):
    runNumber = allMetadata[lfn]['RunNumber']
    if not runNumber in uncheckedRuns:
      uncheckedRuns.append(runNumber)
  if uncheckedRuns:
    print 'Corresponding to %s runs:' % len(uncheckedRuns)
    for run in sortList(uncheckedRuns):
      print run
