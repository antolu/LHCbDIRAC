#!/usr/bin/env python
"""
  For a given LFN directory, check the files that are in the storage and in the LFC and check the consistency. Based on ReplicaManager """
import DIRAC
from DIRAC.Core.Base import Script
unit = 'TB'
sites = []
dir = ''
fileType = ''
prods = []
prodID = ''
Script.registerSwitch( "u:", "Unit=", "   Unit to use [%s] (MB,GB,TB,PB)" % unit )
Script.registerSwitch( "D:", "Dir=", "  directory to be checked" )

Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ...' %
Script.scriptName, ] ) )

Script.parseCommandLine( ignoreErrors = False )

#from DIRAC.DataManagementSystem.Client.DataIntegrityClient     import DataIntegrityClient
from DIRAC.DataManagementSystem.Client.ReplicaManager     import ReplicaManager

for switch in Script.getUnprocessedSwitches():
  if switch[0].lower() == "u" or switch[0].lower() == "unit":
    unit = switch[1]
  if switch[0] == "D" or switch[0].lower() == "dir":
    dir = switch[1]

if not dir:
  print 'One directory should be provided!'
  Script.showHelp()

scaleDict = { 'MB' : 1000 * 1000.0,
              'GB' : 1000 * 1000 * 1000.0,
              'TB' : 1000 * 1000 * 1000 * 1000.0,
              'PB' : 1000 * 1000 * 1000 * 1000 * 1000.0}
if not unit in scaleDict.keys():
  Script.showHelp()
scaleFactor = scaleDict[unit]
rm = ReplicaManager()
#dataIntegrity = DataIntegrityClient()
#res = dataIntegrity.
currentDir = dir
print 'Obtaining the catalog contents for %s directory' % currentDir
res = rm.getCatalogListDirectory( currentDir )
if not res['OK']:
  print 'ERROR: Cannot get directory content'
  DIRAC.exit( -1 )

successfulDirs = res['Value']['Successful']
failedDirs = res['Value']['Failed']
print 'Failed directories: % s' % ( failedDirs.keys() )
print 'Successful directories: % s' % ( successfulDirs.keys() )

if not successfulDirs:
  print 'No directory to analyse. Exit.'
  DIRAC.exit( 0 )

print 'Analysing directory; %s ' % currentDir
dirData = successfulDirs[currentDir]
NumOfFilesInLFC = len( dirData['Files'].keys() )
print 'Number of files registered in LFC: %d ' % NumOfFilesInLFC

allFiles = {}
dirContents = res['Value']['Successful'][currentDir]
allFiles.update( dirContents['Files'] )

zeroReplicaFiles = []
zeroSizeFiles = []
allReplicaDict = {}
allMetadataDict = {}
problematicFiles = {}
replicasPerSE = {}
for lfn, lfnDict in allFiles.items():
  print '----------------'
  print 'LFN: %s ' % lfn
  lfnReplicas = {}
  for se, replicaDict in lfnDict['Replicas'].items():
    #print 'SE: %s -- replica: %s ' % ( se, replicaDict )

    lfnReplicas[se] = replicaDict['PFN']
    if not lfnReplicas:
      zeroReplicaFiles.append( lfn )
  allReplicaDict[lfn] = lfnReplicas
  allMetadataDict[lfn] = lfnDict['MetaData']
  if lfnDict['MetaData']['Size'] == 0:
    zeroSizeFiles.append( lfn )
  print 'All replicas: %s ' % lfnReplicas
  # check each replica if it is exists on the storage
  for se in lfnReplicas:
    if se not in replicasPerSE.keys():
      replicasPerSE[ se ] = []
    pfn = lfnReplicas[ se ]
    replicasPerSE[ se ].append( pfn )
    print 'Checking on storage PFN, SE: %s %s' % ( pfn, se )
    res = rm.getStorageFileMetadata( pfn, se )
    if not res['OK']:
      print 'ERROR: could not get storage file metadata!'
      if lfn not in problematicFiles.keys():
        problematicFiles[lfn] = {}
      if 'BadReplicas' not in problematicFiles[lfn]:
         problematicFiles[lfn]['BadReplicas'] = []
      problematicFiles[lfn]['BadReplicas'].append( pfn )
      continue
    if pfn in res['Value']['Failed']:
      print 'ERROR: the PFN has some problem!'
    elif pfn in res['Value']['Successful']:
      print 'Replica is ok'

print 'Replicas per SE:'
for se in replicasPerSE.keys():
  print 'SE: %s has %d replicas:' % ( se, len( replicasPerSE[ se ] ) )
  for r in replicasPerSE[ se ]:
    print r
  print 'SE: %s has %d replicas: %s ' % ( se, len( replicasPerSE[ se ] ), replicasPerSE[ se ] )
if zeroReplicaFiles:
  print 'Files with zero replicas: %s ' % zeroReplicaFiles
if  zeroSizeFiles:
  print 'Files with zero size: %s' % zeroSizeFiles
if problematicFiles:
  print 'Found some problematic replicas: %s ' % problematicFiles
else:
  print 'All replicas in LFC have been checked on Storage. Ok!'
DIRAC.exit( 0 )
