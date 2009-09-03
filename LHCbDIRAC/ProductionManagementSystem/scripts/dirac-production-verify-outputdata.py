#!/usr/bin/env python
from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/ProductionManagementSystem/scripts/dirac-production-verify-outputdata.py,v 1.8 2009/09/03 16:27:50 acsmith Exp $
########################################################################
__RCSID__   = "$Id: dirac-production-verify-outputdata.py,v 1.8 2009/09/03 16:27:50 acsmith Exp $"
__VERSION__ = "$Revision: 1.8 $"

from DIRAC.DataManagementSystem.Client.DataIntegrityClient import DataIntegrityClient
from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager
from DIRAC.LHCbSystem.Client.Production import Production
from DIRAC.Core.Utilities.List  import sortList
from DIRAC import gLogger,gConfig
import sys,re

if len(sys.argv) < 2:
  print 'Usage: dirac-production-verify-outputdata.py prodID'
  sys.exit()
else:
  prodID = int(sys.argv[1])

integrityClient = DataIntegrityClient()
replicaManager = ReplicaManager()
productionClient = Production()

directories = []
res = productionClient.getParameters(prodID,pname='OutputDirectories')
if res['OK']:
  directories = res['Value'].splitlines()
elif re.search('Could not retrieve parameters for production',res['Message']):
  gLogger.error("The production was not found in the production management system. Skipping SE->LFC and LFC>BK checks.")
else:
  gLogger.error(res['Message'])
  sys.exit(0)

######################################################
#
# This check performs BK->Catalog->SE
#
res = integrityClient.productionToCatalog(prodID)
if not res['OK']:
  gLogger.error(res['Message'])
  sys.exit(0)
bk2catalogMetadata = res['Value']['CatalogMetadata']
bk2catalogReplicas = res['Value']['CatalogReplicas']
res = integrityClient.checkPhysicalFiles(bk2catalogReplicas,bk2catalogMetadata)
if not res['OK']:
  gLogger.error(res['Message'])
  sys.exit(0)

######################################################
#
# This check performs Catalog->BK and Catalog->SE for possible output directories
#

if directories:
  res = replicaManager.getCatalogExists(directories)
  if not res['OK']:
    gLogger.error(res['Message'])
    sys.exit(0)
  for directory, error in res['Value']['Failed']:
    gLogger.error('Failed to determine existance of directory','%s %s' % (directory,error))
  if res['Value']['Failed']:
    sys.exit(0)
  directoryExists = res['Value']['Successful']
  for directory in sortList(directoryExists.keys()):
    if not directoryExists[directory]:
      continue
    iRes = integrityClient.catalogDirectoryToBK(directory)
    if not iRes['OK']:
      gLogger.error(iRes['Message'])
      sys.exit(0)
    catalogDirMetadata = iRes['Value']['CatalogMetadata']
    catalogDirReplicas = iRes['Value']['CatalogReplicas'] 
    catalogMetadata = {}
    catalogReplicas = {}
    for lfn in catalogDirMetadata.keys():
      if not lfn in bk2catalogMetadata.keys():
        catalogMetadata[lfn] = catalogDirMetadata[lfn]
        if catalogDirReplicas.has_key(lfn):
          catalogReplicas[lfn] = catalogDirReplicas[lfn]
    if not catalogMetadata:
      continue
    res = integrityClient.checkPhysicalFiles(catalogReplicas,catalogMetadata)
    if not res['OK']:
      gLogger.error(res['Message'])
      sys.exit(0)

###################################################### 
#
# This check performs SE->Catalog->BK for possible output directories
#
  storageElements = gConfig.getValue('Resources/StorageElementGroups/Tier1_MC_M-DST',[])
  for storageElementName in sortList(storageElements):
    res = integrityClient.storageDirectoryToCatalog(directories,storageElementName)
    if not res['OK']:
      gLogger.error(res['Message'])
      sys.exit(0)
    catalogMetadata = res['Value']['CatalogMetadata']
    storageMetadata = res['Value']['StorageMetadata']
    lfnMissing = []
    for lfn in storageMetadata.keys():
      if not lfn in bk2catalogMetadata.keys():
        lfnMissing.append(lfn)
    if lfnMissing:
      res = integrityClient.catalogFileToBK(lfnMissing)
