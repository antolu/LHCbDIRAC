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
    self.runsList = 0
    self.fileType = ''

    self.lfnsReplicaYes = []
    self.lfnsReplicaNo = []

    self.existingLFNsThatAreNotInBKK = []
    self.nonExistingLFNsThatAreNotInBKK = []
    self.existingLFNsThatAreInBKK = []
    self.nonExistingLFNsThatAreInBKK = []

    self.processedLFNs = []
    self.nonProcessedLFNs = []

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

  def replicaConsistencyCheck( self ):
    """ Starting from the BKK, check if the LFC has consistent information.
    """

    self._getBKKFiles()

    if self.transType not in prodsWithMerge:
      # Merging and Reconstruction
      # In principle few files without replica flag, check them in FC
      gLogger.verbose( 'Checking LFC for those files with BKK ReplicaFlag = No' )
      self.existingLFNsThatAreNotInBKK, self.nonExistingLFNsThatAreNotInBKK = self.getReplicasPresence( self.lfnsReplicaNo )
      self.existingLFNsThatAreInBKK, self.nonExistingLFNsThatAreInBKK = self.getReplicasPresenceFromDirectoryScan( self.lfnsReplicaYes )

    else:
      # 'MCSimulation', 'DataStripping', 'DataSwimming', 'WGProduction'
      # In principle most files have no replica flag, start from LFC files with replicas
      self.existingLFNsThatAreNotInBKK, self.nonExistingLFNsThatAreNotInBK = self.getReplicasPresenceFromDirectoryScan( self.lfnsReplicaNo )
      self.existingLFNsThatAreInBKK, self.nonExistingLFNsThatAreInBKK = self.getReplicasPresence( self.lfnsReplicaYes )

    if self.existingLFNsThatAreNotInBKK:
      gLogger.info( "For prod %s of type %s, %d files has ReplicaFlag=No,\
      %d files are in the LFC but are not in BK" % ( self.prod, self.transType, len( self.lfnsReplicaNo ),
                                                     len( self.existingLFNsThatAreNotInBKK ) ) )

    if self.nonExistingLFNsThatAreInBKK:
      gLogger.info( "For prod %s of type %s, %d files has ReplicaFlag=Yes,\
      %d files are in the LFC but are not in BK" % ( self.prod, self.transType, len( self.lfnsReplicaYes ),
                                                     len( self.nonExistingLFNsThatAreInBKK ) ) )

  ################################################################################

  def _getBKKFiles( self ):
    """ Helper function
    """

    bkQueryReplicaNo = BKQuery( {'Production': self.prod, 'ReplicaFlag':'No'},
                                fileTypes = 'ALL',
                                visible = ( self.transType not in prodsWithMerge ) )
    self.lfnsReplicaNo = bkQueryReplicaNo.getLFNs( printOutput = False )
    if not self.lfnsReplicaNo:
      gLogger.info( "No files found without replica flag" )
    gLogger.info( "Found %d files without replica flag" % len( self.lfnsReplicaNo ) )

    bkQueryReplicaYes = BKQuery( {'Production': self.prod, 'ReplicaFlag':'Yes'},
                                 fileTypes = 'ALL',
                                 visible = ( self.transType not in prodsWithMerge ) )
    self.lfnsReplicaYes = bkQueryReplicaYes.getLFNs( printOutput = False )
    if not self.lfnsReplicaYes:
      gLogger.info( "No files found with replica flag" )
    gLogger.info( "Found %d files with replica flag" % len( self.lfnsReplicaYes ) )

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

    filesFound = self.__getFilesFromDirectoryScan( dirs )

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

  def __getFilesFromDirectoryScan( self, dirs ):
    """ calls rm.getFilesFromDirectory
    """

    gLogger.info( "Checking LFC files from %d directories" % len( dirs ) )
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

  def descendantsConsistencyCheck( self ):
    """ Check if lfns has descendants
    """

    self._getTransformationFiles()

    gLogger.verbose( 'Checking BKK for those files that are processed' )
    res = self.getDescendants( self.processedLFNs )
    self.processedLFNsWithDescendants = res[0]
    self.processedLFNsWithoutDescendants = res[1]
    self.processedLFNsWithMultipleDescendants = res[2]

    gLogger.verbose( 'Checking BKK for those files that are not processed' )
    res = self.getDescendants( self.nonProcessedLFNs )
    self.nonProcessedLFNsWithDescendants = res[0]
    self.nonProcessedLFNsWithoutDescendants = res[1]
    self.nonProcessedLFNsWithMultipleDescendants = res[2]

    if self.processedLFNsWithoutDescendants:
      gLogger.warn( "For prod %s of type %s, %d files are processed, and\
      %d of those do not have descendants" % ( self.prod, self.transType, len( self.processedLFNs ),
                                               len( self.processedLFNsWithoutDescendants ) ) )

    if self.processedLFNsWithMultipleDescendants:
      gLogger.warn( "For prod %s of type %s, %d files are processed, and\
      %d of those have multiple descendants: " % ( self.prod, self.transType, len( self.processedLFNs ),
                                                   len( self.processedLFNsWithMultipleDescendants ) ) )

    if self.nonProcessedLFNsWithDescendants:
      gLogger.warn( "For prod %s of type %s, %d files are not processed, but\
      %d of those have descendants" % ( self.prod, self.transType, len( self.nonProcessedLFNs ),
                                        len( self.nonProcessedLFNsWithDescendants ) ) )

    if self.nonProcessedLFNsWithMultipleDescendants:
      gLogger.warn( "For prod %s of type %s, %d files are not processed, but\
      %d of those have multiple descendants: " % ( self.prod, self.transType, len( self.nonProcessedLFNs ),
                                                   len( self.nonProcessedLFNsWithMultipleDescendants ) ) )

  ################################################################################

  def _getTransformationFiles( self ):
    """ Helper function
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
      self.processedLFNs = [item['LFN'] for item in res['Value']]

    res = self.transClient.getTransformationFiles( selectDict )
    if not res['OK']:
      gLogger.warn( "Failed to get files for transformation %d" % self.prod )
    else:
      self.nonProcessedLFNs = list ( set( [item['LFN'] for item in res['Value']] ) - set( self.processedLFNs ) )

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

  def compareChecksum( self, lfns ):
    """compare the checksum of the file in the FC and the checksum of the physical replicas.
       Returns a dictionary containing 3 sub-dictionaries: one with files with missing PFN, one with
       files with all replicas corrupted, and one with files with some replicas corrupted and at least
       one good replica
    """
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
    # get the lfn checksum from LFC
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

