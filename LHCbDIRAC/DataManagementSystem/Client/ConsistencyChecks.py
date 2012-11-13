""" Some utilities for BK and Catalog(s) interactions
"""

import os, copy

from DIRAC import gLogger
from DIRAC.Core.Utilities.List import breakListIntoChunks
from DIRAC.Interfaces.API.Dirac import Dirac
from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager

from LHCbDIRAC.BookkeepingSystem.Client.BKQuery import BKQuery
from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
from LHCbDIRAC.TransformationSystem.Client.TransformationClient import TransformationClient

#FIXME: this is quite dirty, what should be checked is exactly what it is done
prodsWithMerge = ( 'MCSimulation', 'DataStripping', 'DataSwimming', 'WGProduction' )

class ConsistencyChecks( object ):
  """ A class for handling some consistency check
  """

  def __init__( self, prod = 0, transClient = None, rm = None, bkClient = None ):
    """ c'tor

        One object for every production
    """

    if transClient is None:
      self.transClient = TransformationClient()
    else:
      self.transClient = transClient

    if rm is None:
      self.rm = ReplicaManager()
    else:
      self.rm = rm

    if bkClient is None:
      self.bkClient = BookkeepingClient()
    else:
      self.bkClient = bkClient

    self.dirac = Dirac()

    self.prod = prod
    self.runsList = []
    self.fileType = ''
    self.directories = []

    self.existingLFNsWithBKKReplicaNO = []
    self.nonExistingLFNsWithBKKReplicaNO = []
    self.existingLFNsWithBKKReplicaYES = []
    self.nonExistingLFNsWithBKKReplicaYES = []
    self.existingLFNsNotInBKK = []

    self.processedLFNsWithDescendants = []
    self.processedLFNsWithoutDescendants = []
    self.processedLFNsWithMultipleDescendants = []
    self.nonProcessedLFNsWithDescendants = []
    self.nonProcessedLFNsWithoutDescendants = []
    self.nonProcessedLFNsWithMultipleDescendants = []

    if self.prod:
      res = self.transClient.getTransformation( self.prod, extraParams = False )
      if not res['OK']:
        gLogger.error( "Couldn't find transformation %s: %s" % ( self.prod, res['Message'] ) )
      else:
        self.transType = res['Value']['Type']
      gLogger.info( "Production %d: %s" % ( self.prod, self.transType ) )

  ################################################################################

  def checkBKK2FC( self ):
    """ Starting from the BKK, check if the FileCatalog has consistent information (BK -> FileCatalog)
    """

    lfnsReplicaYes, lfnsReplicaNo = self._getBKKFiles()

    if self.transType not in prodsWithMerge:
      # Merging and Reconstruction
      # In principle few files without replica flag, check them in FC
      gLogger.verbose( 'Checking the File Catalog for those files with BKK ReplicaFlag = No' )
      self.existingLFNsWithBKKReplicaNO, self.nonExistingLFNsWithBKKReplicaNO = self.getReplicasPresence( lfnsReplicaNo )
      self.existingLFNsWithBKKReplicaYES, self.nonexistingLFNsWithBKKReplicaYES = self.getReplicasPresenceFromDirectoryScan( lfnsReplicaYes )

    else:
      # 'MCSimulation', 'DataStripping', 'DataSwimming', 'WGProduction'
      # In principle most files have no replica flag, start from the File Catalog files with replicas
      self.existingLFNsWithBKKReplicaNO, self.nonExistingLFNsThatAreNotInBK = self.getReplicasPresenceFromDirectoryScan( lfnsReplicaNo )
      self.existingLFNsWithBKKReplicaYES, self.nonExistingLFNsWithBKKReplicaYES = self.getReplicasPresence( lfnsReplicaYes )

    if self.existingLFNsWithBKKReplicaNO:
      gLogger.info( "For prod %s of type %s, %d files has ReplicaFlag=No,\
      %d files are in the File Catalog but are not in BK" % ( self.prod, self.transType, len( lfnsReplicaNo ),
                                                              len( self.existingLFNsWithBKKReplicaNO ) ) )

    if self.nonExistingLFNsWithBKKReplicaYES:
      gLogger.info( "For prod %s of type %s, %d files has ReplicaFlag=Yes,\
      %d files are in the File Catalog but are not in BK" % ( self.prod, self.transType, len( lfnsReplicaYes ),
                                                              len( self.nonExistingLFNsWithBKKReplicaYES ) ) )

  ################################################################################

  def _getBKKFiles( self ):
    """ Helper function - get files from BKK
    """

    bkQueryReplicaNo = BKQuery( {'Production': self.prod, 'ReplicaFlag':'No'},
                                fileTypes = 'ALL',
                                visible = ( self.transType not in prodsWithMerge ) )
    lfnsReplicaNo = bkQueryReplicaNo.getLFNs( printOutput = False )
    if not lfnsReplicaNo:
      gLogger.info( "No files found without replica flag" )
    gLogger.info( "Found %d files without replica flag" % len( lfnsReplicaNo ) )

    bkQueryReplicaYes = BKQuery( {'Production': self.prod, 'ReplicaFlag':'Yes'},
                                 fileTypes = 'ALL',
                                 visible = ( self.transType not in prodsWithMerge ) )
    lfnsReplicaYes = bkQueryReplicaYes.getLFNs( printOutput = False )
    if not lfnsReplicaYes:
      gLogger.info( "No files found with replica flag" )
    gLogger.info( "Found %d files with replica flag" % len( lfnsReplicaYes ) )

    return lfnsReplicaYes, lfnsReplicaNo

  ################################################################################

  def getReplicasPresence( self, lfns ):
    """ get the replicas using the standard ReplicaManager.getReplicas()
    """
    present = []
    notPresent = []

    for chunk in breakListIntoChunks( lfns, 1000 ):
      gLogger.verbose( len( chunk ) / 1000 * '.' )
      res = self.rm.getReplicas( chunk )
      if res['OK']:
        present += res['Value']['Successful'].keys()
        notPresent += res['Value']['Failed'].keys()

    return present, notPresent

  ################################################################################

  def getReplicasPresenceFromDirectoryScan( self, lfns ):
    """ Get replicas scanning the directories. Might be faster.
    """

    dirs = []
    present = []
    notPresent = []

    for lfn in lfns:
      dirN = os.path.dirname( lfn )
      if dirN not in dirs:
        dirs.append( dirN )
    dirs.sort()

    filesFound = self._getFilesFromDirectoryScan( dirs )

    if filesFound:
      for lfn in lfns:
        if lfn in filesFound:
          present.append( lfn )
        else:
          notPresent.append( lfn )
    else:
      notPresent = lfns

    return present, notPresent

  ################################################################################

  def _getFilesFromDirectoryScan( self, dirs ):
    """ calls rm.getFilesFromDirectory
    """

    gLogger.info( "Checking File Catalog files from %d directories" % len( dirs ) )
    res = self.rm.getFilesFromDirectory( dirs )
    if not res['OK']:
      gLogger.info( "Error getting files from directories %s:" % dirs, res['Message'] )
      return
    if res['Value']:
      filesFound = res['Value']
    else:
      filesFound = []

    return filesFound

  ################################################################################

  def checkTS2BKK( self ):
    """ Check if lfns has descendants (TransformationFiles -> BK)
    """

    processedLFNs, nonProcessedLFNs = self._getTSFiles()

    gLogger.verbose( 'Checking BKK for those files that are processed' )
    res = self.getDescendants( self.processedLFNs )
    self.processedLFNsWithDescendants = res[0]
    self.processedLFNsWithoutDescendants = res[1]
    self.processedLFNsWithMultipleDescendants = res[2]

    gLogger.verbose( 'Checking BKK for those files that are not processed' )
    res = self.getDescendants( nonProcessedLFNs )
    self.nonProcessedLFNsWithDescendants = res[0]
    self.nonProcessedLFNsWithoutDescendants = res[1]
    self.nonProcessedLFNsWithMultipleDescendants = res[2]

    if self.processedLFNsWithoutDescendants:
      gLogger.warn( "For prod %s of type %s, %d files are processed, and\
      %d of those do not have descendants" % ( self.prod, self.transType, len( processedLFNs ),
                                               len( self.processedLFNsWithoutDescendants ) ) )

    if self.processedLFNsWithMultipleDescendants:
      gLogger.warn( "For prod %s of type %s, %d files are processed, and\
      %d of those have multiple descendants: " % ( self.prod, self.transType, len( processedLFNs ),
                                                   len( self.processedLFNsWithMultipleDescendants ) ) )

    if self.nonProcessedLFNsWithDescendants:
      gLogger.warn( "For prod %s of type %s, %d files are not processed, but\
      %d of those have descendants" % ( self.prod, self.transType, len( nonProcessedLFNs ),
                                        len( self.nonProcessedLFNsWithDescendants ) ) )

    if self.nonProcessedLFNsWithMultipleDescendants:
      gLogger.warn( "For prod %s of type %s, %d files are not processed, but\
      %d of those have multiple descendants: " % ( self.prod, self.transType, len( nonProcessedLFNs ),
                                                   len( self.nonProcessedLFNsWithMultipleDescendants ) ) )

  ################################################################################

  def _getTSFiles( self ):
    """ Helper function - get files from the TS
    """

    selectDict = { 'TransformationID': self.prod}
    if self.runsList:
      selectDict['RunNumber'] = self.runsList

    selectDictProcessed = copy.deepcopy( selectDict )
    selectDictProcessed['Status'] = 'Processed'
    res = self.transClient.getTransformationFiles( selectDictProcessed )
    if not res['OK']:
      gLogger.warn( "Failed to get files for transformation %d" % self.prod )
    else:
      processedLFNs = [item['LFN'] for item in res['Value']]

    res = self.transClient.getTransformationFiles( selectDict )
    if not res['OK']:
      gLogger.warn( "Failed to get files for transformation %d" % self.prod )
    else:
      nonProcessedLFNs = list ( set( [item['LFN'] for item in res['Value']] ) - set( processedLFNs ) )

    return processedLFNs, nonProcessedLFNs

  ################################################################################

  def getDescendants( self, lfns ):
    """ get the descendants of a list of LFN (for the production)
    """

    filesWithDescendants = []
    filesWithoutDescendants = []
    filesWitMultipleDescendants = []

    lfnChunks = breakListIntoChunks( lfns, 200 )
    for lfnChunk in lfnChunks:
      resChunk = self.bkClient.getFileDescendants( lfnChunk, depth = 1, production = self.prod, checkreplica = False )
      if resChunk['OK']:
        descDict = resChunk['Value']['Successful']
        if self.fileType:
          descDict = self._selectByFileType( resChunk['Value']['Successful'] )
        for lfn in lfnChunk:
          if lfn in descDict.keys():
            filesWithDescendants.append( lfn )
            if len( descDict[lfn] ) > 1:
              filesWitMultipleDescendants.append( {lfn:descDict[lfn]} )
          else:
            filesWithoutDescendants.append( lfn )
      else:
        gLogger.error( "\nError getting descendants for %d files" % len( lfnChunk ) )
        continue

    return filesWithDescendants, filesWithoutDescendants, filesWitMultipleDescendants

  ################################################################################

  def _selectByFileType( self, lfnDict ):
    """ Select only those files from the values of lfnDict that have a certain type
    """
    ancDict = copy.deepcopy( lfnDict )
    for ancestor, descendants in lfnDict.items():
      descendantsCopy = copy.deepcopy( descendants )
      for desc in descendants:
        #quick and dirty...
        if '.'.join( os.path.basename( desc ).split( '.' )[1:] ).lower() != self.fileType.lower():
          descendantsCopy.remove( desc )
      if len( descendantsCopy ) == 0:
        ancDict.pop( ancestor )
      else:
        ancDict[ancestor] = descendantsCopy

    return ancDict

  ################################################################################

  def checkFC2BKK( self ):
    ''' check that files present in the FC are also in the BKK
    '''
    catalogFiles = self._getFilesFromDirectoryScan( self.directories )
    res = self._getBKKMetadata( catalogFiles )
    self.existingLFNsNotInBKK, self.existingLFNsWithBKKReplicaNO, self.existingLFNsWithBKKReplicaYES = res
    if self.existingLFNsWithBKKReplicaNO:
      gLogger.info( "%d files are in the FC but have replica = NO in BKK" % ( len( self.existingLFNsWithBKKReplicaNO ) ) )
    if self.existingLFNsNotInBKK:
      gLogger.info( "%d files are in the FC but not in BKK" % ( len( self.existingLFNsNotInBKK ) ) )

  ################################################################################

  def _getBKKMetadata( self, lfns ):
    ''' get metadata (i.e. replica flag) of a list of LFNs
    '''
    missingLFNs = noFlagLFNs = okLFNs = []
    res = self.bkClient.getFileMetadata( lfns )
    if not res['OK']:
      gLogger.error( "Can't get the bkk metadata: ", res['Message'] )
    else:
      metadata = res['Value']
      missingLFNs = [lfn for lfn in lfns if metadata.get( lfn, {} ).get( 'GotReplica' ) == None]
      noFlagLFNs = [lfn for lfn in lfns if metadata.get( lfn, {} ).get( 'GotReplica' ) == 'No']
      okLFNs = [lfn for lfn in lfns if metadata.get( lfn, {} ).get( 'GotReplica' ) == 'Yes']

    return missingLFNs, noFlagLFNs, okLFNs


  ################################################################################

  def compareChecksum( self, lfns ):
    '''compare the checksum of the file in the FC and the checksum of the physical replicas.
       Returns a dictionary containing 3 sub-dictionaries: one with files with missing PFN, one with
       files with all replicas corrupted, and one with files with some replicas corrupted and at least
       one good replica
    '''
    retDict = {}

    retDict['AllReplicasCorrupted'] = {}
    retDict['SomeReplicasCorrupted'] = {}

    gLogger.info( "Get lfn meta-data for files to be checked.." )
    res = self.dirac.getMetadata( lfns )
    if not res['OK']:
      gLogger.error( "error %s" % res['Message'] )
      return res

    gLogger.info( "Get all replicas.." )
    replicasRes = self.dirac.getAllReplicas( lfns )
    if not replicasRes[ 'OK' ]:
      gLogger.error( "error:  %s" % res['Message'] )
      return res

    gLogger.info( "compare checksum file by file ..." )
    csDict = {}
    checkSumMismatch = []
    pfnNotAvailable = []
    val = res['Value']
    for lfn in lfns:
    # get the lfn checksum from the LFC
      if lfn in val['Failed']:
        gLogger.info( "failed request for %s" % ( lfn, val['Failed'][lfn] ) )
        continue
      elif lfn in val['Successful']:
        if lfn not in csDict.keys():
          csDict[ lfn ] = {}
        csDict[ lfn ][ 'LFCChecksum' ] = val['Successful'][lfn][ 'Checksum']
      else:
        gLogger.error( "LFN not in return values! %s " % lfn )
        continue

      if lfn not in replicasRes['Value']['Successful'].keys():
        gLogger.error( "did not get replicas for this LFN: %s " % lfn )
        continue
      replicaDict = replicasRes['Value']['Successful'][ lfn ]
      for se in replicaDict.keys():
        surl = replicaDict[ se ]
        # get the surls metadata and compare the checksum
        surlRes = self.rm.getStorageFileMetadata( surl, se )
        if not surlRes[ 'OK' ]:
          gLogger.error( "error replicaManager.getStorageFileMetadata returns %s" % ( surlRes['Message'] ) )
          continue
        if surl not in surlRes['Value']['Successful']:
          gLogger.error( "SURL was not in the return value! %s " % surl )
          pfnNotAvailable.append( surl )
          continue
        surlChecksum = surlRes['Value']['Successful'][ surl ]['Checksum']
        csDict[ lfn ][ surl ] = {}
        csDict[ lfn ][ surl ]['PFNChecksum'] = surlChecksum
        lfcChecksum = csDict[ lfn ][ 'LFCChecksum' ]
        if lfcChecksum != surlChecksum:
          gLogger.info( "Check if the difference is just leading zeros" )
          #if lfcChecksum not in surlChecksum and surlChecksum not in lfcChecksum:
          if lfcChecksum.lstrip( '0' ) != surlChecksum.lstrip( '0' ):
            gLogger.error( "ERROR!! checksum mismatch at %s for LFN \
            %s:  LFC checksum: %s , PFN checksum : %s " % ( se, lfn, csDict[ lfn ][ 'LFCChecksum' ], surlChecksum ) )
            if lfn not in checkSumMismatch:
              checkSumMismatch.append( lfn )
          else:
            gLogger.info( "Checksums differ only for leading zeros: LFC Checksum: %s PFN Checksum %s " % ( lfcChecksum,
                                                                                                           surlChecksum ) )

    for lfn in checkSumMismatch:
      oneGoodReplica = False
      gLogger.info( "LFN: %s, LFC Checksum: %s " % ( lfn, csDict[ lfn ][ 'LFCChecksum'] ) )
      lfcChecksum = csDict[ lfn ][ 'LFCChecksum' ]
      for k in csDict[ lfn ].keys():
        if k == 'LFCChecksum':
          continue
        for kk in csDict[ lfn ][ k ].keys():
          if 'PFNChecksum' == kk:
            pfnChecksum = csDict[ lfn ][ k ]['PFNChecksum']
            pfn = k
            gLogger.info( "%s %s " % ( pfn, pfnChecksum ) )
            if pfnChecksum == lfcChecksum:
              oneGoodReplica = True
      if oneGoodReplica:
        gLogger.info( "=> At least one replica with good Checksum" )
        retDict['SomeReplicasCorrupted'][ lfn ] = csDict[ lfn ]
      else:
        gLogger.info( "=> All replicas have bad checksum" )
        retDict['AllReplicasCorrupted'][ lfn ] = csDict[ lfn ]
    retDict[ 'MissingPFN'] = pfnNotAvailable

    return retDict

