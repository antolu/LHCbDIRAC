""" This is the Data Integrity Client which allows the simple reporting of problematic file and replicas to the IntegrityDB and their status correctly updated in the FileCatalog.""" 
__RCSID__ = "$Id: DataIntegrityClient.py 26492 2010-06-17 14:03:38Z acsmith $"

from DIRAC.DataManagementSystem.Client.DataIntegrityClient  import DataIntegrityClient as DIRACDataIntegrityClient
import re,os,types

class DataIntegrityClient(DIRACDataIntegrityClient):

  ##########################################################################
  #
  # This section contains the specific methods for BK->LFC checks
  #
  
  def productionToCatalog(self,productionID):
    """  This obtains the file information from the BK and checks these files are present in the LFC.
    """
    gLogger.info("-" * 40)
    gLogger.info("Performing the BK->LFC check")
    gLogger.info("-" * 40)
    res = self.__getProductionFiles(productionID)
    if not res['OK']:
      return res
    bkMetadata = res['Value']['BKMetadata']
    noReplicaFiles = res['Value']['GotReplicaNo']
    yesReplicaFiles = res['Value']['GotReplicaYes']
    # For the files marked as existing we perfom catalog check
    res = self.__getCatalogMetadata(yesReplicaFiles)
    if not res['OK']:
      return res
    catalogMetadata = res['Value']
    # Try and get the metadata for files that shouldn't exist in the catalog
    if noReplicaFiles:
      res = self.__checkCatalogForBKNoReplicas(noReplicaFiles)
      if not res['OK']:
        return res    
      catalogMetadata.update(res['Value'])
    # Get the replicas for the files found to exist in the catalog
    res = self.__getCatalogReplicas(catalogMetadata.keys())
    if not res['OK']:
      return res
    replicas = res['Value']
    resDict = {'CatalogMetadata':catalogMetadata,'CatalogReplicas':replicas}
    return S_OK(resDict)

  def __checkCatalogForBKNoReplicas(self,lfns):
    gLogger.info('Checking the catalog existence of %s files' % len(lfns))

    res = self.rm.getCatalogFileMetadata(lfns)
    if not res['OK']:
      gLogger.error('Failed to get catalog metadata',res['Message'])
      return res
    allMetadata = res['Value']['Successful']
    existingCatalogFiles = allMetadata.keys()
    if existingCatalogFiles:
      self.__reportProblematicFiles(existingCatalogFiles,'BKReplicaNo')
    gLogger.info('Checking the catalog existence of files complete')
    return S_OK(allMetadata)

  def __getProductionFiles(self,productionID):
    """ This method queries the bookkeeping and obtains the file metadata for the given production
    """
    gLogger.info("Attempting to get files for production %s" % productionID)
    bk = RPCClient('Bookkeeping/BookkeepingManager')
    res = bk.getProductionFiles(productionID,'ALL')
    if not res['OK']:
      return res
    yesReplicaFiles = []
    noReplicaFiles = []
    badReplicaFiles = []
    badBKFileSize = []
    badBKGUID = []
    allMetadata = res['Value']
    gLogger.info("Obtained at total of %s files" % len(allMetadata.keys()))
    totalSize = 0
    for lfn,bkMetadata in allMetadata.items():
      if (bkMetadata['FileType'] != 'LOG'):
        if (bkMetadata['GotReplica'] == 'Yes'):
          yesReplicaFiles.append(lfn)
          if bkMetadata['FileSize']:
            totalSize+= long(bkMetadata['FileSize'])
        elif (bkMetadata['GotReplica'] == 'No'):
          noReplicaFiles.append(lfn)
        else:
          badReplicaFiles.append(lfn)
        if not bkMetadata['FileSize']:
          badBKFileSize.append(lfn)
        if not bkMetadata['GUID']:
          badBKGUID.append(lfn)
    if badReplicaFiles:
      self.__reportProblematicFiles(badReplicaFiles,'BKReplicaBad')
    if badBKFileSize:
      self.__reportProblematicFiles(badBKFileSize,'BKSizeBad')
    if badBKGUID:
      self.__reportProblematicFiles(badBKGUID,'BKGUIDBad')
    gLogger.info("%s files marked with replicas with total size %s bytes" % (len(yesReplicaFiles),totalSize))
    gLogger.info("%s files marked without replicas" % len(noReplicaFiles))
    resDict = {'BKMetadata':allMetadata,'GotReplicaYes':yesReplicaFiles,'GotReplicaNo':noReplicaFiles}
    return S_OK(resDict)

  ##########################################################################
  #
  # This section contains the specific methods for LFC->BK checks
  #

  def catalogDirectoryToBK(self,lfnDir):
    """ This obtains the replica and metadata information from the catalog for the supplied directory and checks against the BK.
    """
    gLogger.info("-" * 40)
    gLogger.info("Performing the LFC->BK check") 
    gLogger.info("-" * 40)
    if type(lfnDir) in types.StringTypes:
      lfnDir = [lfnDir]
    res = self.__getCatalogDirectoryContents(lfnDir)
    if not res['OK']:
      return res
    replicas = res['Value']['Replicas']
    catalogMetadata = res['Value']['Metadata']
    resDict = {'CatalogMetadata':catalogMetadata,'CatalogReplicas':replicas}
    if not catalogMetadata:
      return S_ERROR('No files found in directory')
    res = self.__checkBKFiles(replicas,catalogMetadata)
    if not res['OK']:
      return res       
    return S_OK(resDict)

  def catalogFileToBK(self,lfns):
    """ This obtains the replica and metadata information from the catalog and checks against the storage elements.
    """
    gLogger.info("-" * 40)
    gLogger.info("Performing the LFC->BK check")
    gLogger.info("-" * 40)
    if type(lfns) in types.StringTypes:
      lfns = [lfns]
    res = self.__getCatalogMetadata(lfns)
    if not res['OK']:
      return res
    catalogMetadata = res['Value']
    res = self.__getCatalogReplicas(catalogMetadata.keys())
    if not res['OK']:
      return res
    replicas = res['Value']
    res = self.__checkBKFiles(replicas,catalogMetadata)
    if not res['OK']:
      return res
    resDict = {'CatalogMetadata':catalogMetadata,'CatalogReplicas':replicas}
    return S_OK(resDict)

  def __checkBKFiles(self,replicas,catalogMetadata):
    """ This takes the supplied replica and catalog metadata information and ensures the files exist in the BK with the correct metadata.
    """
    gLogger.info('Checking the bookkeeping existence of %s files' % len(catalogMetadata))

    res = self.rm.getCatalogFileMetadata(catalogMetadata.keys(),catalogs=['BookkeepingDB'])
    if not res['OK']:
      gLogger.error('Failed to get bookkeeping metadata',res['Message'])
      return res
    allMetadata = res['Value']['Successful']
    missingBKFiles = []
    sizeMismatchFiles = []
    guidMismatchFiles = []
    noBKReplicaFiles = []
    withBKReplicaFiles = []
    for lfn,error in res['Value']['Failed'].items():
      if re.search('No such file or directory',error):
        missingBKFiles.append(lfn)
    for lfn,bkMetadata in allMetadata.items():
      if not bkMetadata['FileSize'] == catalogMetadata[lfn]['Size']:
        sizeMismatchFiles.append(lfn)
      if not bkMetadata['GUID'] ==  catalogMetadata[lfn]['GUID']:
        guidMismatchFiles.append(lfn)
      gotReplica = bkMetadata['GotReplica'].lower()
      if (gotReplica == 'yes') and (not replicas.has_key(lfn)):
        withBKReplicaFiles.append(lfn)
      if (gotReplica != 'yes') and (replicas.has_key(lfn)):
        noBKReplicaFiles.append(lfn)
    if missingBKFiles:
      self.__reportProblematicFiles(missingBKFiles,'LFNBKMissing')
    if sizeMismatchFiles:
      self.__reportProblematicFiles(sizeMismatchFiles,'BKCatalogSizeMismatch')
    if guidMismatchFiles:
      self.__reportProblematicFiles(guidMismatchFiles,'BKCatalogGUIDMismatch')
    if withBKReplicaFiles:
      self.__reportProblematicFiles(withBKReplicaFiles,'BKReplicaYes')
    if noBKReplicaFiles:
      self.__reportProblematicFiles(noBKReplicaFiles,'BKReplicaNo')
    gLogger.info('Checking the bookkeeping existence of files complete')
    return S_OK(allMetadata)

  ##########################################################################
  #
  # This section contains the resolution methods for various prognoses
  #

  def resolveBKReplicaYes(self,problematicDict):
    """ This takes the problematic dictionary returned by the integrity DB and resolved the BKReplicaYes prognosis
    """
    lfn = problematicDict['LFN']
    fileID = problematicDict['FileID']

    res = self.rm.getCatalogExists(lfn,singleFile=True)
    if not res['OK']:
      return self.__returnProblematicError(fileID,res)
    removeBKFile = False
    # If the file does not exist in the catalog
    if not res['Value']:
      gLogger.info("BKReplicaYes file (%d) does not exist in the catalog. Removing..." % fileID)
      removeBKFile = True
    else:
      gLogger.info("BKReplicaYes file (%d) found to exist in the catalog" % fileID)
      # If the file has no replicas in the catalog
      res = self.rm.getCatalogReplicas(lfn,singleFile=True)
      if (not res['OK']) and (res['Message'] == 'File has zero replicas'):
        gLogger.info("BKReplicaYes file (%d) found to exist without replicas. Removing..." % fileID)
        removeBKFile = True
    if removeBKFile:
      # Remove the file from the BK because it does not exist
      res = self.rm.removeCatalogFile(lfn,singleFile=True,catalogs=['BookkeepingDB'])
      if not res['OK']:
        return self.__returnProblematicError(fileID,res)
      gLogger.info("BKReplicaYes file (%d) removed from bookkeeping" % fileID)
    return self.__updateCompletedFiles('BKReplicaYes',fileID)

  def resolveBKReplicaNo(self,problematicDict):
    """ This takes the problematic dictionary returned by the integrity DB and resolved the BKReplicaNo prognosis
    """
    lfn = problematicDict['LFN']
    fileID = problematicDict['FileID']

    res = self.rm.getCatalogExists(lfn,singleFile=True)
    if not res['OK']:
      return self.__returnProblematicError(fileID,res)
    # If the file exists in the catalog
    if not res['Value']:
      return self.__updateCompletedFiles('BKReplicaNo',fileID)
    gLogger.info("BKReplicaNo file (%d) found to exist in the catalog" % fileID)
    # and has available replicas
    res = self.rm.getCatalogReplicas(lfn,singleFile=True)
    if not res['OK']:
      return self.__returnProblematicError(fileID,res)
    if not res['Value']:
      gLogger.info("BKReplicaNo file (%d) found to have no replicas" % fileID)
      return self.changeProblematicPrognosis(fileID,'LFNZeroReplicas')
    gLogger.info("BKReplicaNo file (%d) found to have replicas" % fileID)
    res = self.rm.addCatalogFile(lfn,singleFile=True,catalogs=['BookkeepingDB'])
    if not res['OK']:
      return self.__returnProblematicError(fileID,res)
    return self.__updateCompletedFiles('BKReplicaNo',fileID)
