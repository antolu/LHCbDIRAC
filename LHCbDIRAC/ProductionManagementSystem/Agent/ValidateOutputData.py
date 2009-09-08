########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/ProductionManagementSystem/Agent/ValidateOutputData.py,v 1.4 2009/09/08 20:59:28 acsmith Exp $
########################################################################
__RCSID__   = "$Id: ValidateOutputData.py,v 1.4 2009/09/08 20:59:28 acsmith Exp $"
__VERSION__ = "$Revision: 1.4 $"

from DIRAC                                                     import S_OK, S_ERROR, gConfig, gMonitor, gLogger, rootPath
from DIRAC.Core.Base.AgentModule                               import AgentModule
from DIRAC.DataManagementSystem.Client.DataIntegrityClient     import DataIntegrityClient
from DIRAC.DataManagementSystem.Client.ReplicaManager          import ReplicaManager
from DIRAC.ProductionManagementSystem.Client.ProductionClient  import ProductionClient
from DIRAC.Core.Utilities.List                                 import sortList
from DIRAC.Core.Utilities.Shifter                              import setupShifterProxyInEnv

import re, os

AGENT_NAME = 'ProductionManagement/ValidateOutputData'

class ValidateOutputData(AgentModule):

  #############################################################################
  def initialize(self):
    """Sets defaults
    """
    self.integrityClient = DataIntegrityClient()
    self.replicaManager = ReplicaManager()
    self.productionClient = ProductionClient()
    self.am_setModuleParam("shifterProxy", "DataManager")
    self.am_setModuleParam("shifterProxyLocation","%s/runit/%s/proxy" % (rootPath,AGENT_NAME))
    return S_OK()

  #############################################################################
  def execute(self):
    """ The VerifyOutputData execution method.
    """
    self.enableFlag = self.am_getOption('EnableFlag','True')
    if not self.enableFlag == 'True':
      self.log.info('VerifyOutputData is disabled by configuration option %s/EnableFlag' %(self.section))
      return S_OK('Disabled via CS flag')

    gLogger.info("-" * 40)
    self.updateWaitingIntegrity()
    gLogger.info("-" * 40)

    res = self.productionClient.getProductionsWithStatus('ValidatingOutput')
    if not res['OK']:
      gLogger.error("Failed to get ValidatingOutput productions",res['Message'])
      return res
    prods = res['Value']
    if not prods:
      gLogger.info("No productions found in ValidatingOutput status")
      return S_OK()
    gLogger.info("Found %s productions in ValidatingOutput status" % len(prods))
    for prodID in sortList(prods):
      gLogger.info("-" * 40)
      res = self.checkProductionIntegrity(int(prodID))
      if not res['OK']:
        gLogger.error("Failed to perform full integrity check for production")
      else:
        res = self.integrityClient.getProductionProblematics(int(prodID))
        if not res['OK']:
          gLogger.error("Failed to determine whether there were problematic files",res['Message'])
          newStatus = ''       
        elif res['Value']:
          gLogger.info("%d problematic files for production %s were found" % (len(res['Value']),prodID))
          newStatus = "WaitingIntegrity"
        else:
          gLogger.info("No problematics were found for production %s" % prodID)  
          newStatus = "ValidatedOutput"
        if newStatus:
          res = self.productionClient.setProductionStatus(prodID,newStatus)
          if not res['OK']:
            gLogger.error("Failed to update status of production %s to %s" % (prodID,newStatus))
          else:
            gLogger.info("Updated status of production %s to %s" % (prodID,newStatus))
      gLogger.info("-" * 40)
    return S_OK()

  def updateWaitingIntegrity(self):
    gLogger.info("Looking for production in the WaitingIntegrity status to update")
    res = self.productionClient.getProductionsWithStatus('WaitingIntegrity')
    if not res['OK']:
      gLogger.error("Failed to get WaitingIntegrity productions",res['Message'])
      return res
    prods = res['Value']
    if not prods:
      gLogger.info("No productions found in WaitingIntegrity status")
      return S_OK()
    gLogger.info("Found %s productions in WaitingIntegrity status" % len(prods))
    for prodID in sortList(prods):
      gLogger.info("-" * 40)
      res = self.integrityClient.getProductionProblematics(int(prodID))
      if not res['OK']:
        gLogger.error("Failed to determine waiting problematics for production",res['Message'])
      elif not res['Value']:
        res = self.productionClient.setProductionStatus(prodID,'ValidatedOutput')
        if not res['OK']:
          gLogger.error("Failed to update status of production %s to ValidatedOutput" % (prodID))
        else:
          gLogger.info("Updated status of production %s to ValidatedOutput" % (prodID))
      else:
        gLogger.info("%d problematic files for production %s were found" % (len(res['Value']),prodID))
    return

  #############################################################################
  def checkProductionIntegrity(self,prodID):
    """ This method contains the real work
    """
    gLogger.info("-" * 40)
    gLogger.info("Checking the integrity of production %s" % prodID)
    gLogger.info("-" * 40)
    directories = []
    res = self.productionClient.getParameters(prodID,pname='OutputDirectories')
    if res['OK']:
      directories = res['Value'].splitlines()
    elif res['Message'].endswith('not found'):
      gLogger.warn("The production was not found in the production management system. Skipping SE->LFC and LFC>BK checks.")
    else:
      gLogger.error("Completely failed to obtain production parameters",res['Message'])
      return res

    ######################################################
    #
    # This check performs BK->Catalog->SE
    #
    res = self.integrityClient.productionToCatalog(prodID)
    if not res['OK']:
      gLogger.error(res['Message'])
      return res
    bk2catalogMetadata = res['Value']['CatalogMetadata']
    bk2catalogReplicas = res['Value']['CatalogReplicas']
    res = self.integrityClient.checkPhysicalFiles(bk2catalogReplicas,bk2catalogMetadata)
    if not res['OK']:
      gLogger.error(res['Message'])
      return res

    if not directories:
      return S_OK()
    ######################################################
    #
    # This check performs Catalog->BK and Catalog->SE for possible output directories
    #

    res = self.replicaManager.getCatalogExists(directories)
    if not res['OK']:
      gLogger.error(res['Message'])
      return res
    for directory, error in res['Value']['Failed']:
      gLogger.error('Failed to determine existance of directory','%s %s' % (directory,error))
    if res['Value']['Failed']:
      return S_ERROR("Failed to determine the existance of directories")
    directoryExists = res['Value']['Successful']
    for directory in sortList(directoryExists.keys()):
      if not directoryExists[directory]:
        continue
      iRes = self.integrityClient.catalogDirectoryToBK(directory)
      if not iRes['OK']:
        gLogger.error(iRes['Message'])
        return iRes
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
      res = self.integrityClient.checkPhysicalFiles(catalogReplicas,catalogMetadata)
      if not res['OK']:
        gLogger.error(res['Message'])
        return res

    ###################################################### 
    #
    # This check performs SE->Catalog->BK for possible output directories
    #
    storageElements = gConfig.getValue('Resources/StorageElementGroups/Tier1_MC_M-DST',[])
    for storageElementName in sortList(storageElements):
      res = self.integrityClient.storageDirectoryToCatalog(directories,storageElementName)
      if not res['OK']:
        gLogger.error(res['Message'])
        return res
      catalogMetadata = res['Value']['CatalogMetadata']
      storageMetadata = res['Value']['StorageMetadata']
      lfnMissing = []
      for lfn in storageMetadata.keys():
        if not lfn in bk2catalogMetadata.keys():
          lfnMissing.append(lfn)
      if lfnMissing:
        res = self.integrityClient.catalogFileToBK(lfnMissing)
        if not res['OK']:
          gLogger.error(res['Message'])
          return res
    gLogger.info("-" * 40)
    gLogger.info("Completed integrity check for production %s" % prodID)
    gLogger.info("-" * 40)
    return S_OK()
