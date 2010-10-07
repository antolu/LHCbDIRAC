#! /usr/bin/env python
from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

import DIRAC
from DIRAC                                                          import gLogger
from DIRAC.DataManagementSystem.Client.ReplicaManager               import ReplicaManager
from LHCbDIRAC.DataManagementSystem.Client.DataIntegrityClient      import DataIntegrityClient
from DIRAC.Core.Utilities.List                                      import sortList
import sys,os

rm = ReplicaManager()
integrity = DataIntegrityClient()

if len(sys.argv) < 2:
  print 'Usage: checkDirectoryIntegrity.py <dir|inputFileOfDirs>'
  sys.exit()
else:
  inputFileName = sys.argv[1]

if os.path.exists(inputFileName):
  inputFile = open(inputFileName,'r')
  string = inputFile.read()
  directories = string.splitlines()
  inputFile.close()
else:
  directories = [inputFileName]

######################################################
#
# This check performs Catalog->BK and Catalog->SE for possible output directories
#
res = rm.getCatalogExists(directories)
if not res['OK']:
  gLogger.error(res['Message'])
  DIRAC.exit(-2)
for directory, error in res['Value']['Failed']:
  gLogger.error('Failed to determine existance of directory','%s %s' % (directory,error))
if res['Value']['Failed']:
  DIRAC.exit(-2)
directoryExists = res['Value']['Successful']
for directory in sortList(directoryExists.keys(),True):
  if not directoryExists[directory]:
    continue
  gLogger.info("Checking the integrity of %s" % directory)
  iRes = integrity.catalogDirectoryToBK(directory)
  if not iRes['OK']:
    gLogger.error(iRes['Message'])
    continue
  catalogDirMetadata = iRes['Value']['CatalogMetadata']
  catalogDirReplicas = iRes['Value']['CatalogReplicas']
  catalogMetadata = {}
  catalogReplicas = {}
  for lfn in catalogDirMetadata.keys():
    #if not lfn in bk2catalogMetadata.keys():
      catalogMetadata[lfn] = catalogDirMetadata[lfn]
      if catalogDirReplicas.has_key(lfn):   
        catalogReplicas[lfn] = catalogDirReplicas[lfn]
  if not catalogMetadata:
    continue
  res = integrity.checkPhysicalFiles(catalogReplicas,catalogMetadata,[])
  if not res['OK']:
    gLogger.error(res['Message'])
    continue
