#!/usr/bin/env python
from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/ProductionManagementSystem/scripts/dirac-production-verify-outputdata.py,v 1.3 2009/04/06 10:48:53 acsmith Exp $
########################################################################
__RCSID__   = "$Id: dirac-production-verify-outputdata.py,v 1.3 2009/04/06 10:48:53 acsmith Exp $"
__VERSION__ = "$Revision: 1.3 $"

from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.Core.Utilities.List import sortList
from DIRAC.DataManagementSystem.Client.FileCatalog import FileCatalog
from DIRAC.DataManagementSystem.Client.StorageElement import StorageElement
import sys

if len(sys.argv) < 2:
  print 'Usage: dirac-production-verify-outputdata.py prodID'
  sys.exit()
else:
  prodID = int(sys.argv[1])

#########################################################################
# Get the files present in the bookkeeping for the production
#########################################################################

client = RPCClient('Bookkeeping/BookkeepingManager')
res = client.getProductionFiles(prodID,'ALL')
if not res['OK']:
  print 'Failed to retrieve production files: %s' % res['Message']
  sys.exit()
if not res['Value']:
  print 'No output files were found for production %s' % (prodID)
  sys.exit()

incorrectlyRegisteredSize = []
incorrectlyRegisteredReplicas = []
filesWithReplicas = []
filesWithoutReplicas = []
totalBKSize = 0
fileInfo = {}
for lfn,lfnDict in res['Value'].items():
  if not lfnDict['FilesSize']:
    size = 0
    incorrectlyRegisteredSize.append(lfn)
  else:
    size = lfnDict['FilesSize']
  if not lfnDict['GotReplica']:
    incorrectlyRegisteredReplicas.append(lfn)
    hasReplica = 'No'  
  else:
    hasReplica = lfnDict['GotReplica']
  guid = lfnDict['GUID']
  if hasReplica == 'Yes':
    filesWithReplicas.append(lfn)
    totalBKSize+=size
    fileInfo[lfn] = {'Size':size,'GUID':guid}
  else:
    filesWithoutReplicas.append(lfn)
print '\n################### %s ########################\n' % 'Bookkeeping contents'.center(20)
print '%s : %s' % ('Total files'.ljust(20),str(len(filesWithoutReplicas)+len(filesWithReplicas)).ljust(20))
print '%s : %s' % ('With replicas'.ljust(20),str(len(filesWithReplicas)).ljust(20))
print '%s : %s' % ('Total size (bytes)'.ljust(20),str(totalBKSize).ljust(20))
print '%s : %s' % ('Incorrectly registered size in BK'.ljust(20),str(len(incorrectlyRegisteredSize)).ljust(20))
print '%s : %s' % ('Incorrectly registered replica in BK'.ljust(20),str(len(incorrectlyRegisteredReplicas)).ljust(20))

if incorrectlyRegisteredSize:
  print '\nThe following %s files has incorrectly registered size in the BK.\n' % len(incorrectlyRegisteredSize)
  for lfn in sortList(incorrectlyRegisteredSize):
    print lfn

if incorrectlyRegisteredReplicas:
  print '\nThe following %s files has incorrectly registered replicas in the BK.\n' % len(incorrectlyRegisteredReplicas)
  for lfn in sortList(incorrectlyRegisteredReplicas):
    print lfn

#########################################################################
# Check the files exist in the LFC
#########################################################################

client = FileCatalog('LcgFileCatalogCombined')
res = client.exists(filesWithReplicas)
if not res['OK']:
  print res['Message']
  sys.exit()

filesMissingFromLFC = []
filesPresentInLFC = []
for lfn,exists in res['Value']['Successful'].items():
  if not exists:
    filesMissingFromLFC.append(lfn)
    fileInfo.pop(lfn)
  else:
    filesPresentInLFC.append(lfn)
 
print '\n################### %s ########################\n' % 'LFC contents'.center(20)
if filesMissingFromLFC:
  print 'The following %s files were missing from the the LFC.' % len(filesMissingFromLFC)
  for lfn in sortList(filesMissingFromLFC):
    print lfn

if not filesMissingFromLFC:
  print 'Found %s files in the LFC.' % len(filesPresentInLFC)

#########################################################################
# Check the file metadata against the LFC 
#########################################################################

res = client.getFileMetadata(filesPresentInLFC) 
if not res['OK']:
  print res['Message']
  sys.exit()  

sizeMismatches = []
guidMismatches = []
for lfn,fileMetadata in res['Value']['Successful'].items():
  size = fileMetadata['Size']
  guid = fileMetadata['GUID']
  if fileInfo[lfn]['Size'] != fileMetadata['Size']:
    sizeMismatches.append(lfn)
  if fileInfo[lfn]['GUID'] != fileMetadata['GUID']:
    guidMismatches.append(lfn)

print '\n################### %s ########################\n' % 'LFC metadata'.center(20)
if sizeMismatches:
  print 'The following %s files found with LFC-BK size mismatches.' % len(sizeMismatches)
  for lfn in sortList(sizeMismatches):
    print lfn
if guidMismatches:
  print 'The following %s files found with LFC-BK guid mismatches.' % len(guidMismatches)
  for lfn in sortList(guidMismatches):
    print lfn
if not (guidMismatches or sizeMismatches):
  print 'The LFC and BK metadata matched for %s files.' % len(res['Value']['Successful'].keys())

#########################################################################
# Check the location of the file replicas
#########################################################################

res = client.getReplicas(filesPresentInLFC)
if not res['OK']:
  print res['Message']
  sys.exit()

zeroReplicaFiles = []
sePfns = {}
pfnLfns = {}
for lfn,replicaDict in res['Value']['Successful'].items():
  if not replicaDict:
    zeroReplicaFiles.append(lfn)
  else:
    for se,pfn in replicaDict.items():
      if not sePfns.has_key(se):
        sePfns[se] = []
      sePfns[se].append(pfn)
      pfnLfns[pfn] = lfn

print '\n################### %s ########################\n' % 'LFC Replicas'.center(20)
if zeroReplicaFiles:
  print 'The following %s files found with zero replicas.\n' % len(zeroReplicaFiles)
  for lfn in sortList(zeroReplicaFiles):
    print lfn

if sePfns:
  print '%s %s' % ('Site'.ljust(20), 'Files'.rjust(20))
  for site in sortList(sePfns.keys()):
    files = len(sePfns[site])
    print '%s %s' % (site.ljust(20), str(files).rjust(20))

#########################################################################
# Check the physical files exist for all the replicas
#########################################################################

missingReplicas = {}
numberOfMissingReplicas = 0
bkSESizeMismatch = {}
numberOfBkSESizeMismatch = 0
for se,pfns in sePfns.items():
  storageElement = StorageElement(se)
  res = storageElement.getFileSize(pfns)
  if not res['OK']:
    print 'Failed to get file sizes for pfns: %s' % res['Message']
  else:
    for pfn,reason in res['Value']['Failed'].items():
      if not missingReplicas.has_key(se):
       missingReplicas[se] = []
      missingReplicas[se].append(pfnLfns[pfn])
      numberOfMissingReplicas+=1
    for pfn,size in res['Value']['Successful'].items():
      if not size == fileInfo[pfnLfns[pfn]]['Size']:
        if not bkSESizeMismatch.has_key(se):
          bkSESizeMismatch[se] = []
        bkSESizeMismatch[se].append(pfnLfns[pfn])
        numberOfBkSESizeMismatch +=1

print '\n################### %s ########################\n' % 'SE physical files'.center(20)
if missingReplicas:
  print 'The following files were missing at %s SEs' % len(missingReplicas.keys())
  for se in sortList(missingReplicas.keys()):
    lfns = missingReplicas[se]
    print '%s : %s' % (se.ljust(10),str(len(lfns)).ljust(10))
    for lfn in sortList(lfns):
      print lfn

if bkSESizeMismatch:
  print 'The following files had size mis-matches at %s SEs' % len(bkSESizeMismatch.keys())
  for se in sortList(bkSESizeMismatch.keys()):
    lfns = bkSESizeMismatch[se]
    print '%s : %s' % (se.ljust(10),str(len(lfns)).ljust(10))
    for lfn in sortList(lfns):
      print lfn

if not (missingReplicas or bkSESizeMismatch):
  print 'All registered replicas existed with the correct size.'


print '\n################### %s ########################\n' % 'Summary'.center(20)

if filesMissingFromLFC:
  print 'There were %s files missing from the LFC that were present in the BK.' % len(filesMissingFromLFC)

if sizeMismatches:
  print 'There were %s files with mis-matched sizes in the BK and LFC.' % len(sizeMismatches)

if guidMismatches:
  print 'There were %s files with mis-matched guids in the BK and LFC.' % len(guidMismatches)

if zeroReplicaFiles:
  print 'There were %s files with zero replicas in the LFC.' % len(zeroReplicaFiles)

if numberOfMissingReplicas:
  print 'There were %s missing physical files.' % numberOfMissingReplicas

if numberOfBkSESizeMismatch:
  print 'There were %s physical files with mis-matched size in the BK and SE.' % numberOfBkSESizeMismatch
   
print '\n'
