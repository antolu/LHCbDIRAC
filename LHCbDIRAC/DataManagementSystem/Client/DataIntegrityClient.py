"""
    This is the Data Integrity Client which allows the simple reporting of problematic
    file and replicas to the IntegrityDB and their status correctly updated in the FileCatalog.
"""


from DIRAC.DataManagementSystem.Client.DataIntegrityClient  import DataIntegrityClient as DIRACDataIntegrityClient
from DIRAC                                                  import S_OK, S_ERROR, gLogger
import re, types
from DIRAC.Resources.Catalog.FileCatalog                    import FileCatalog
from DIRAC.Resources.Storage.StorageElement                 import StorageElement
from DIRAC.Core.Utilities.ReturnValues                  import returnSingleResult

__RCSID__ = "$Id$"

class DataIntegrityClient( DIRACDataIntegrityClient ):

  ##########################################################################
  #
  # This section contains the specific methods for BK->LFC checks
  #

  def productionToCatalog( self, productionID ):
    """  This obtains the file information from the BK and checks these files are present in the LFC.
    """
    gLogger.info( "-" * 40 )
    gLogger.info( "Performing the BK->LFC check" )
    gLogger.info( "-" * 40 )
    res = self.__getProductionFiles( productionID )
    if not res['OK']:
      return res
    noReplicaFiles = res['Value']['GotReplicaNo']
    yesReplicaFiles = res['Value']['GotReplicaYes']
    # For the files marked as existing we perfom catalog check
    res = self.__getCatalogMetadata( yesReplicaFiles )
    if not res['OK']:
      return res
    catalogMetadata = res['Value']
    # Try and get the metadata for files that shouldn't exist in the catalog
    if noReplicaFiles:
      res = self.__checkCatalogForBKNoReplicas( noReplicaFiles )
      if not res['OK']:
        return res
      catalogMetadata.update( res['Value'] )
    # Get the replicas for the files found to exist in the catalog
    res = self.__getCatalogReplicas( catalogMetadata.keys() )
    if not res['OK']:
      return res
    replicas = res['Value']
    resDict = {'CatalogMetadata':catalogMetadata, 'CatalogReplicas':replicas}
    return S_OK( resDict )

  def __checkCatalogForBKNoReplicas( self, lfns ):
    ''' checks the catalog existence for given files '''
    gLogger.info( 'Checking the catalog existence of %s files' % len( lfns ) )

    res = self.fc.getFileMetadata( lfns )
    if not res['OK']:
      gLogger.error( 'Failed to get catalog metadata', res['Message'] )
      return res
    allMetadata = res['Value']['Successful']
    existingCatalogFiles = allMetadata.keys()
    if existingCatalogFiles:
      self.__reportProblematicFiles( existingCatalogFiles, 'BKReplicaNo' )
    gLogger.info( 'Checking the catalog existence of files complete' )
    return S_OK( allMetadata )

  def __getProductionFiles( self, productionID ):
    """ This method queries the bookkeeping and obtains the file metadata for the given production
    """
    from DIRAC.Core.DISET.RPCClient import RPCClient
    gLogger.info( "Attempting to get files for production %s" % productionID )
    bk = RPCClient( 'Bookkeeping/BookkeepingManager' )
    res = bk.getProductionFiles( productionID, 'ALL' )
    if not res['OK']:
      return res
    yesReplicaFiles = []
    noReplicaFiles = []
    badReplicaFiles = []
    badBKFileSize = []
    badBKGUID = []
    allMetadata = res['Value']
    gLogger.info( "Obtained at total of %s files" % len( allMetadata.keys() ) )
    totalSize = 0
    for lfn, bkMetadata in allMetadata.items():
      if ( bkMetadata['FileType'] != 'LOG' ):
        if ( bkMetadata['GotReplica'] == 'Yes' ):
          yesReplicaFiles.append( lfn )
          if bkMetadata['FileSize']:
            totalSize += long( bkMetadata['FileSize'] )
        elif ( bkMetadata['GotReplica'] == 'No' ):
          noReplicaFiles.append( lfn )
        else:
          badReplicaFiles.append( lfn )
        if not bkMetadata['FileSize']:
          badBKFileSize.append( lfn )
        if not bkMetadata['GUID']:
          badBKGUID.append( lfn )
    if badReplicaFiles:
      self.__reportProblematicFiles( badReplicaFiles, 'BKReplicaBad' )
    if badBKFileSize:
      self.__reportProblematicFiles( badBKFileSize, 'BKSizeBad' )
    if badBKGUID:
      self.__reportProblematicFiles( badBKGUID, 'BKGUIDBad' )
    gLogger.info( "%s files marked with replicas with total size %s bytes" % ( len( yesReplicaFiles ), totalSize ) )
    gLogger.info( "%s files marked without replicas" % len( noReplicaFiles ) )
    resDict = {'BKMetadata':allMetadata, 'GotReplicaYes':yesReplicaFiles, 'GotReplicaNo':noReplicaFiles}
    return S_OK( resDict )

  ##########################################################################
  #
  # This section contains the specific methods for LFC->BK checks
  #

  def catalogDirectoryToBK( self, lfnDir ):
    """ This obtains the replica and metadata information from the catalog for
      the supplied directory and checks against the BK.
    """
    gLogger.info( "-" * 40 )
    gLogger.info( "Performing the LFC->BK check" )
    gLogger.info( "-" * 40 )
    if type( lfnDir ) in types.StringTypes:
      lfnDir = [lfnDir]
    res = self.__getCatalogDirectoryContents( lfnDir )
    if not res['OK']:
      return res
    replicas = res['Value']['Replicas']
    catalogMetadata = res['Value']['Metadata']
    resDict = {'CatalogMetadata':catalogMetadata, 'CatalogReplicas':replicas}
    if not catalogMetadata:
      gLogger.warn( 'No files found in directory %s' % lfnDir )
      return S_OK( resDict )
    res = self.__checkBKFiles( replicas, catalogMetadata )
    if not res['OK']:
      return res
    return S_OK( resDict )

  def catalogFileToBK( self, lfns ):
    """ This obtains the replica and metadata information from the catalog and
      checks against the storage elements.
    """
    gLogger.info( "-" * 40 )
    gLogger.info( "Performing the LFC->BK check" )
    gLogger.info( "-" * 40 )
    if type( lfns ) in types.StringTypes:
      lfns = [lfns]
    res = self.__getCatalogMetadata( lfns )
    if not res['OK']:
      return res
    catalogMetadata = res['Value']
    res = self.__getCatalogReplicas( catalogMetadata.keys() )
    if not res['OK']:
      return res
    replicas = res['Value']
    res = self.__checkBKFiles( replicas, catalogMetadata )
    if not res['OK']:
      return res
    resDict = {'CatalogMetadata':catalogMetadata, 'CatalogReplicas':replicas}
    return S_OK( resDict )

  def __checkBKFiles( self, replicas, catalogMetadata ):
    """ This takes the supplied replica and catalog metadata information and ensures
      the files exist in the BK with the correct metadata.
    """
    gLogger.info( 'Checking the bookkeeping existence of %s files' % len( catalogMetadata ) )

    res = FileCatalog( catalogs = ['BookkeepingDB'] ).getFileMetadata( catalogMetadata.keys() )
    if not res['OK']:
      gLogger.error( 'Failed to get bookkeeping metadata', res['Message'] )
      return res
    allMetadata = res['Value']['Successful']
    missingBKFiles = []
    sizeMismatchFiles = []
    guidMismatchFiles = []
    noBKReplicaFiles = []
    withBKReplicaFiles = []
    for lfn, error in res['Value']['Failed'].items():
      if re.search( 'No such file or directory', error ):
        missingBKFiles.append( lfn )
    for lfn, bkMetadata in allMetadata.items():
      if not bkMetadata['FileSize'] == catalogMetadata[lfn]['Size']:
        sizeMismatchFiles.append( lfn )
      if not bkMetadata['GUID'] == catalogMetadata[lfn]['GUID']:
        guidMismatchFiles.append( lfn )
      gotReplica = bkMetadata['GotReplica'].lower()
      if ( gotReplica == 'yes' ) and ( not replicas.has_key( lfn ) ):
        withBKReplicaFiles.append( lfn )
      if ( gotReplica != 'yes' ) and ( replicas.has_key( lfn ) ):
        noBKReplicaFiles.append( lfn )
    if missingBKFiles:
      self.__reportProblematicFiles( missingBKFiles, 'LFNBKMissing' )
    if sizeMismatchFiles:
      self.__reportProblematicFiles( sizeMismatchFiles, 'BKCatalogSizeMismatch' )
    if guidMismatchFiles:
      self.__reportProblematicFiles( guidMismatchFiles, 'BKCatalogGUIDMismatch' )
    if withBKReplicaFiles:
      self.__reportProblematicFiles( withBKReplicaFiles, 'BKReplicaYes' )
    if noBKReplicaFiles:
      self.__reportProblematicFiles( noBKReplicaFiles, 'BKReplicaNo' )
    gLogger.info( 'Checking the bookkeeping existence of files complete' )
    return S_OK( allMetadata )

  ##########################################################################
  #
  # This section contains the resolution methods for various prognoses
  #

  def resolveBKReplicaYes( self, problematicDict ):
    """ This takes the problematic dictionary returned by the integrity DB and resolved the BKReplicaYes prognosis
    """
    lfn = problematicDict['LFN']
    fileID = problematicDict['FileID']

    res = returnSingleResult( self.fc.exists( lfn ) )
    if not res['OK']:
      return self.__returnProblematicError( fileID, res )
    removeBKFile = False
    # If the file does not exist in the catalog
    if not res['Value']:
      gLogger.info( "BKReplicaYes file (%d) does not exist in the catalog. Removing..." % fileID )
      removeBKFile = True
    else:
      gLogger.info( "BKReplicaYes file (%d) found to exist in the catalog" % fileID )
      # If the file has no replicas in the catalog
      res = returnSingleResult( self.fc.getReplicas( lfn ) )
      if ( not res['OK'] ) and ( res['Message'] == 'File has zero replicas' ):
        gLogger.info( "BKReplicaYes file (%d) found to exist without replicas. Removing..." % fileID )
        removeBKFile = True
    if removeBKFile:
      # Remove the file from the BK because it does not exist
      res = returnSingleResult( FileCatalog( catalogs = ['BookkeepingDB'] ).removeFile( lfn ) )
      if not res['OK']:
        return self.__returnProblematicError( fileID, res )
      gLogger.info( "BKReplicaYes file (%d) removed from bookkeeping" % fileID )
    return self.__updateCompletedFiles( 'BKReplicaYes', fileID )

  def resolveBKReplicaNo( self, problematicDict ):
    """ This takes the problematic dictionary returned by the integrity DB and resolved the BKReplicaNo prognosis
    """
    lfn = problematicDict['LFN']
    fileID = problematicDict['FileID']

    res = returnSingleResult( self.fc.exists( lfn ) )
    if not res['OK']:
      return self.__returnProblematicError( fileID, res )
    # If the file exists in the catalog
    if not res['Value']:
      return self.__updateCompletedFiles( 'BKReplicaNo', fileID )
    gLogger.info( "BKReplicaNo file (%d) found to exist in the catalog" % fileID )
    # and has available replicas
    res = returnSingleResult( self.fc.getCatalogReplicas( lfn ) )
    if not res['OK']:
      return self.__returnProblematicError( fileID, res )
    if not res['Value']:
      gLogger.info( "BKReplicaNo file (%d) found to have no replicas" % fileID )
      return self.changeProblematicPrognosis( fileID, 'LFNZeroReplicas' )
    gLogger.info( "BKReplicaNo file (%d) found to have replicas" % fileID )
    res = returnSingleResult( FileCatalog( catalogs = ['BookkeepingDB'] ).addFile( lfn ) )
    if not res['OK']:
      return self.__returnProblematicError( fileID, res )
    return self.__updateCompletedFiles( 'BKReplicaNo', fileID )

  def checkPhysicalFiles( self, replicas, catalogMetadata, ses = [], fixIt = False ):
    """ This obtains takes the supplied replica and metadata information obtained from the catalog and checks against the storage elements.
    """
    gLogger.info( "-" * 40 )
    gLogger.info( "Performing the LFC->SE check" )
    gLogger.info( "-" * 40 )
    return self.__checkPhysicalFiles( replicas, catalogMetadata, ses = ses, fixIt = fixIt )

  def __checkPhysicalFiles( self, replicas, catalogMetadata, ses = [], fixIt = False ):
    """ This obtains the physical file metadata and checks the metadata against the catalog entries
    """
    sePfns = {}
    pfnLfns = {}
    for lfn, replicaDict in replicas.items():
      for se, pfn in replicaDict.items():
        if ( ses ) and ( se not in ses ):
          continue
        if not sePfns.has_key( se ):
          sePfns[se] = []
        sePfns[se].append( pfn )
        pfnLfns[pfn] = lfn
    gLogger.info( '%s %s' % ( 'Storage Element'.ljust( 20 ), 'Replicas'.rjust( 20 ) ) )
    for site in sorted( sePfns ):
      files = len( sePfns[site] )
      gLogger.info( '%s %s' % ( site.ljust( 20 ), str( files ).rjust( 20 ) ) )

    for se in sorted( sePfns ):
      pfns = sePfns[se]
      pfnDict = {}
      for pfn in pfns:
        pfnDict[pfn] = pfnLfns[pfn]
      sizeMismatch = []
      checksumMismatch = []
      checksumBadInFC = []
      res = self.__checkPhysicalFileMetadata( pfnDict, se )
      if not res['OK']:
        gLogger.error( 'Failed to get physical file metadata.', res['Message'] )
        return res
      for pfn, metadata in res['Value'].items():
        if pfnLfns[pfn] in catalogMetadata:
          if ( metadata['Size'] != catalogMetadata[pfnLfns[pfn]]['Size'] ) and ( metadata['Size'] != 0 ):
            sizeMismatch.append( ( pfnLfns[pfn], pfn, se, 'CatalogPFNSizeMismatch' ) )
          if metadata['Checksum'] != catalogMetadata[pfnLfns[pfn]]['Checksum']:
            if metadata['Checksum'].replace( 'x', '0' ) == catalogMetadata[pfnLfns[pfn]]['Checksum'].replace( 'x', '0' ):
              checksumBadInFC.append( ( pfnLfns[pfn], pfn, se, "%s %s" % ( metadata['Checksum'], catalogMetadata[pfnLfns[pfn]]['Checksum'] ) ) )
            else:
              checksumMismatch.append( ( pfnLfns[pfn], pfn, se, "%s %s" % ( metadata['Checksum'], catalogMetadata[pfnLfns[pfn]]['Checksum'] ) ) )
      if sizeMismatch:
        self.__reportProblematicReplicas( sizeMismatch, se, 'CatalogPFNSizeMismatch', fixIt = fixIt )
      if checksumMismatch:
        self.__reportProblematicReplicas( checksumMismatch, se, 'CatalogChecksumMismatch', fixIt = fixIt )
      if checksumBadInFC:
        self.__reportProblematicReplicas( checksumBadInFC, se, 'CatalogChecksumToBeFixed', fixIt = fixIt )
    return S_OK()

  def __checkPhysicalFileMetadata( self, pfnLfns, se ):
    """ Check obtain the physical file metadata and check the files are available
    """
    gLogger.info( 'Checking the integrity of %s physical files at %s' % ( len( pfnLfns ), se ) )

    res = StorageElement( se ).getFileMetadata( pfnLfns.keys() )
    if not res['OK']:
      gLogger.error( 'Failed to get metadata for pfns.', res['Message'] )
      return res
    pfnMetadataDict = res['Value']['Successful']
    # If the replicas are completely missing
    missingReplicas = []
    for pfn, reason in res['Value']['Failed'].items():
      if re.search( 'File does not exist', reason ):
        missingReplicas.append( ( pfnLfns[pfn], pfn, se, 'PFNMissing' ) )
    if missingReplicas:
      self.__reportProblematicReplicas( missingReplicas, se, 'PFNMissing' )
    lostReplicas = []
    unavailableReplicas = []
    zeroSizeReplicas = []
    # If the files are not accessible
    for pfn, pfnMetadata in pfnMetadataDict.items():
      if pfnMetadata['Lost']:
        lostReplicas.append( ( pfnLfns[pfn], pfn, se, 'PFNLost' ) )
      if pfnMetadata['Unavailable']:
        unavailableReplicas.append( ( pfnLfns[pfn], pfn, se, 'PFNUnavailable' ) )
      if pfnMetadata['Size'] == 0:
        zeroSizeReplicas.append( ( pfnLfns[pfn], pfn, se, 'PFNZeroSize' ) )
    if lostReplicas:
      self.__reportProblematicReplicas( lostReplicas, se, 'PFNLost' )
    if unavailableReplicas:
      self.__reportProblematicReplicas( unavailableReplicas, se, 'PFNUnavailable' )
    if zeroSizeReplicas:
      self.__reportProblematicReplicas( zeroSizeReplicas, se, 'PFNZeroSize' )
    gLogger.info( 'Checking the integrity of physical files at %s complete' % se )
    return S_OK( pfnMetadataDict )

  def __reportProblematicReplicas( self, replicaTuple, se, reason, fixIt = False ):
    """ Simple wrapper function around setReplicaProblematic """
    gLogger.info( 'The following %s files had %s at %s' % ( len( replicaTuple ), reason, se ) )
    for lfn, pfn, se, reason1 in sorted( replicaTuple ):
      if reason1 == reason:
        reason1 = ''
      if lfn:
        gLogger.info( lfn, reason1 )
      else:
        gLogger.info( pfn, reason1 )
    if fixIt:
      res = self.setReplicaProblematic( replicaTuple, sourceComponent = 'DataIntegrityClient' )
      if not res['OK']:
        gLogger.info( 'Failed to update integrity DB with replicas', res['Message'] )
      else:
        gLogger.info( 'Successfully updated integrity DB with replicas' )

  ##########################################################################
  #
  # This section contains the specific methods for obtaining replica and metadata information from the catalog
  #

  def __getCatalogDirectoryContents( self, lfnDir ):
    """ Obtain the contents of the supplied directory
    """
    gLogger.info( 'Obtaining the catalog contents for %s directories' % len( lfnDir ) )

    activeDirs = list( lfnDir )
    allFiles = {}
    while len( activeDirs ) > 0:
      currentDir = activeDirs[0]
      res = self.fc.listDirectory( currentDir, verbose = True )
      activeDirs.remove( currentDir )
      if not res['OK']:
        gLogger.error( 'Failed to get directory contents', res['Message'] )
        return res
      elif res['Value']['Failed'].has_key( currentDir ):
        gLogger.error( 'Failed to get directory contents', '%s %s' % ( currentDir, res['Value']['Failed'][currentDir] ) )
      else:
        dirContents = res['Value']['Successful'][currentDir]
        activeDirs.extend( dirContents['SubDirs'] )
        allFiles.update( dirContents['Files'] )

    zeroReplicaFiles = []
    zeroSizeFiles = []
    allReplicaDict = {}
    allMetadataDict = {}
    for lfn, lfnDict in allFiles.items():
      lfnReplicas = {}
      for se, replicaDict in lfnDict['Replicas'].items():
        lfnReplicas[se] = replicaDict['PFN']
      if not lfnReplicas:
        zeroReplicaFiles.append( lfn )
      allReplicaDict[lfn] = lfnReplicas
      allMetadataDict[lfn] = lfnDict['MetaData']
      if lfnDict['MetaData']['Size'] == 0:
        zeroSizeFiles.append( lfn )
    if zeroReplicaFiles:
      self.__reportProblematicFiles( zeroReplicaFiles, 'LFNZeroReplicas' )
    if zeroSizeFiles:
      self.__reportProblematicFiles( zeroSizeFiles, 'LFNZeroSize' )
    gLogger.info( 'Obtained at total of %s files for the supplied directories' % len( allMetadataDict ) )
    resDict = {'Metadata':allMetadataDict, 'Replicas':allReplicaDict}
    return S_OK( resDict )
