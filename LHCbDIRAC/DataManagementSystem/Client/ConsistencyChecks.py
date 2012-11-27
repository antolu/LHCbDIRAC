""" Main class for doing consistency checks, between files in:
    - File Catalog
    - Bookkeeping
    - TransformationSystem

    Should be extended to include the Storage (in DIRAC)
"""

import os, copy, ast, time

from DIRAC import gLogger, S_ERROR
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

  def __init__( self, transClient=None, rm=None, bkClient=None ):
    ''' c'tor

        One object for every production/BkQuery/directoriesList...
    '''

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

    #Base elements from which to start the consistency checks
    self.prod = 0
    self.directories = []
    self.bkQuery = None

    #Accessory elements
    self.lfns = []
    self.runsList = []
    self.fileType = []
    self.fileTypesExcluded = []
    self.transType = ''

    #Results of the checks
    self.existingLFNsWithBKKReplicaNO = []
    self.nonExistingLFNsWithBKKReplicaNO = []
    self.existingLFNsWithBKKReplicaYES = []
    self.nonExistingLFNsWithBKKReplicaYES = []
    self.existingLFNsNotInBKK = []
    self.nonExistingLFNsThatAreNotInBK = []
    self.nonexistingLFNsWithBKKReplicaYES = []

    self.processedLFNsWithDescendants = []
    self.processedLFNsWithoutDescendants = []
    self.processedLFNsWithMultipleDescendants = []
    self.nonProcessedLFNsWithDescendants = []
    self.nonProcessedLFNsWithoutDescendants = []
    self.nonProcessedLFNsWithMultipleDescendants = []
    self.descendantsForProcessedLFNs = []
    self.descendantsForNonProcessedLFNs = []

    self.filesInBKKNotInTS = []

  ################################################################################

  def checkBKK2FC( self ):
    ''' Starting from the BKK, check if the FileCatalog has consistent information (BK -> FileCatalog)

        Works either when the bkQuery is free, or when it is made using a transformation ID
    '''

    try:
      bkQuery = self.__getBKKQuery()
    except ValueError, e:
      return S_ERROR( e )


    lfnsReplicaYes = self._getBKKFiles( bkQuery, 'Yes' )
    lfnsReplicaNo = self._getBKKFiles( bkQuery, 'No' )

    if self.transType not in prodsWithMerge:
      # Merging and Reconstruction
      # In principle few files without replica flag, check them in FC
      gLogger.verbose( 'Checking the File Catalog for those files with BKK ReplicaFlag = No' )
      self.existingLFNsWithBKKReplicaNO, self.nonExistingLFNsWithBKKReplicaNO = self.getReplicasPresence( lfnsReplicaNo )
      gLogger.verbose( 'Checking the File Catalog for those files with BKK ReplicaFlag = Yes' )
      res = self.getReplicasPresenceFromDirectoryScan( lfnsReplicaYes )
      self.existingLFNsWithBKKReplicaYES, self.nonexistingLFNsWithBKKReplicaYES = res

    else:
      # 'MCSimulation', 'DataStripping', 'DataSwimming', 'WGProduction'
      # In principle most files have no replica flag, start from the File Catalog files with replicas
      gLogger.verbose( 'Checking the File Catalog for those files with BKK ReplicaFlag = No' )
      res = self.getReplicasPresenceFromDirectoryScan( lfnsReplicaNo )
      self.existingLFNsWithBKKReplicaNO, self.nonExistingLFNsThatAreNotInBK = res
      gLogger.verbose( 'Checking the File Catalog for those files with BKK ReplicaFlag = Yes' )
      res = self.getReplicasPresence( lfnsReplicaYes )
      self.existingLFNsWithBKKReplicaYES, self.nonExistingLFNsWithBKKReplicaYES = res

    if self.existingLFNsWithBKKReplicaNO:
      msg = "%d files have ReplicaFlag = No, but %d are in the FC" % ( len( lfnsReplicaNo ),
                                                                       len( self.existingLFNsWithBKKReplicaNO ) )
      if self.transType:
        msg = "For prod %s of type %s, " % ( self.prod, self.transType ) + msg
      gLogger.info( msg )

    if self.nonExistingLFNsWithBKKReplicaYES:
      msg = "%d files have ReplicaFlag = Yes, but %d are not in the FC" % ( len( lfnsReplicaYes ),
                                                                            len( self.nonExistingLFNsWithBKKReplicaYES ) )
      if self.transType:
        msg = "For prod %s of type %s, " % ( self.prod, self.transType ) + msg
      gLogger.info( msg )

  ################################################################################

  def _getBKKFiles( self, bkQuery, replicaFlag='Yes' ):
    ''' Helper function - get files from BKK, first constructing the bkQuery
    '''
    visibility = bkQuery.isVisible()
    if self.transType:
      visibility = 'Yes' if self.transType not in prodsWithMerge else 'All'
    bkQuery.setVisible( False )
    bkQueryRes = BKQuery( bkQuery, visible=visibility )
    bkQueryRes.setOption( 'ReplicaFlag', replicaFlag )
    lfnsRes = bkQueryRes.getLFNs( printOutput=False )
    if not lfnsRes:
      gLogger.info( "No files found with replica flag = %s" % replicaFlag )
    else:
      gLogger.info( "Found %d files with replica flag = %s" % ( len( lfnsRes ), replicaFlag ) )

    return lfnsRes

  def __getBKKQuery( self, fromTS=False ):
    ''' get the bkQuery to be used
    '''
    bkQuery = None
    if fromTS:
      res = self.transClient.getBookkeepingQueryForTransformation( self.prod )
      if not res['OK']:
        raise ValueError, res['Message']
      bkQuery = BKQuery( res['Value'] )
    else:
      if self.bkQuery:
        bkQuery = self.bkQuery
      if self.prod:
        bkQuery = BKQuery( self.bkQuery.setOption( "Production", self.prod ) )
      if not bkQuery:
        raise ValueError( "Need to specify either the bkQuery or a production id" )

    #if self.runsList:
    #  bkQuery.update( {'RunNumbers':self.runsList} )
    #  bkQuery.pop( 'StartRun', 0 )
    #  bkQuery.pop( 'EndRun', 0 )

    return bkQuery

  ################################################################################

  def getReplicasPresence( self, lfns ):
    ''' get the replicas using the standard ReplicaManager.getReplicas()
    '''
    present = []
    notPresent = []

    gLogger.info( "Checking replicas for %d files" % len( lfns ) )
    for chunk in breakListIntoChunks( lfns, 1000 ):
      gLogger.verbose( len( chunk ) / 1000 * '.' )
      res = self.rm.getReplicas( chunk )
      if res['OK']:
        present += res['Value']['Successful'].keys()
        notPresent += res['Value']['Failed'].keys()

    gLogger.info( "Found %d files with replicas and %d without" % ( len( present ), len( notPresent ) ) )
    return present, notPresent

  ################################################################################

  def getReplicasPresenceFromDirectoryScan( self, lfns ):
    ''' Get replicas scanning the directories. Might be faster.
    '''

    dirs = {}
    present = []
    notPresent = []

    for lfn in lfns:
      dirN = os.path.dirname( lfn )
      dirs.setdefault( dirN, [] ).append( lfn )

    gLogger.info( "Checking File Catalog files from %d directories" % len( dirs ) )

    for dirN in sorted( dirs ):
      startTime = time.time()
      lfnsFound = self._getFilesFromDirectoryScan( dirN )
      gLogger.verbose( "Obtained %d files in %.1f seconds" % ( len( lfnsFound ), time.time() - startTime ) )
      pr, notPr = self._compareLFNLists( dirs[dirN], lfnsFound )
      notPresent += notPr
      present += pr

    gLogger.info( "Found %d files with replicas and %d without" % ( len( present ), len( notPresent ) ) )
    return present, notPresent

  ################################################################################

  def _compareLFNLists( self, lfns, lfnsFound ):
    ''' return files in both lists and files in lfns and not in lfnsFound
    '''
    present = []
    startTime = time.time()
    gLogger.verbose( "Comparing list of %d LFNs with second list of %d" % ( len( lfns ), len( lfnsFound ) ) )
    if lfnsFound:
      lfnsFound.sort()
      for lfn in sorted( lfns ):
        while lfnsFound and lfn > lfnsFound[0]:
          lfnsFound.pop( 0 )
        if lfnsFound and lfn == lfnsFound[0]:
          present.append( lfn )
          lfnsFound.pop( 0 )
          lfns.remove( lfn )
    gLogger.verbose( "End of comparison: %.1f seconds" % ( time.time() - startTime ) )
    return present, lfns

  def _getFilesFromDirectoryScan( self, dirs ):
    ''' calls rm.getFilesFromDirectory
    '''

    res = self.rm.getFilesFromDirectory( dirs )
    if not res['OK']:
      gLogger.info( "Error getting files from directories %s:" % dirs, res['Message'] )
      return
    if res['Value']:
      lfnsFound = res['Value']
    else:
      lfnsFound = []

    return lfnsFound

  ################################################################################

  def checkTS2BKK( self ):
    ''' Check if lfns has descendants (TransformationFiles -> BK)
    '''
    if not self.prod:
      return S_ERROR( "You need a transformationID" )

    processedLFNs, nonProcessedLFNs = self._getTSFiles()
    gLogger.always( 'Found %d processed files and %d non processed files' % ( len( processedLFNs ),
                                                                              len( nonProcessedLFNs ) ) )

    gLogger.verbose( 'Checking BKK for those files that are processed' )
    res = self.getDescendants( processedLFNs )
    self.processedLFNsWithDescendants = res[0]
    self.processedLFNsWithoutDescendants = res[1]
    self.processedLFNsWithMultipleDescendants = res[2]
    self.descendantsForProcessedLFNs = res[3]

    gLogger.verbose( 'Checking BKK for those files that are not processed' )
    res = self.getDescendants( nonProcessedLFNs )
    self.nonProcessedLFNsWithDescendants = res[0]
    self.nonProcessedLFNsWithoutDescendants = res[1]
    self.nonProcessedLFNsWithMultipleDescendants = res[2]
    self.descendantsForNonProcessedLFNs = res[3]

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
    ''' Helper function - get files from the TS
    '''

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
    ''' get the descendants of a list of LFN (for the production)
    '''

    filesWithDescendants = []
    filesWithoutDescendants = []
    filesWithMultipleDescendants = []

    lfnChunks = breakListIntoChunks( lfns, 200 )
    descendants = []
    for lfnChunk in lfnChunks:
      resChunk = self.bkClient.getFileDescendants( lfnChunk, depth=1, production=self.prod, checkreplica=False )
      if resChunk['OK']:
        descDict = resChunk['Value']['Successful']
        if self.fileType:
          descDict = self._selectByFileType( resChunk['Value']['Successful'] )
          # Get the list of unique descendants
          for desc in descDict.values():
            descendants += [lfn for lfn in desc if lfn not in descendants]
        ft_count = self._getFileTypesCount( descDict )
        for lfn in lfnChunk:
          if lfn in descDict.keys():
            filesWithDescendants.append( lfn )
            for ftc in ft_count[lfn].values():
              if ftc > 1:
                filesWithMultipleDescendants.append( {lfn:descDict[lfn]} )
          else:
            filesWithoutDescendants.append( lfn )
      else:
        gLogger.error( "\nError getting descendants for %d files" % len( lfnChunk ) )
        continue

    return filesWithDescendants, filesWithoutDescendants, filesWithMultipleDescendants, descendants

  ################################################################################

  def _selectByFileType( self, lfnDict ):
    ''' Select only those files from the values of lfnDict that have a certain type
    '''
    fileTypes = list( set( self.fileType ) - set ( self.fileTypesExcluded ) )
    ancDict = copy.deepcopy( lfnDict )
    for ancestor, descendants in lfnDict.items():
      descendantsCopy = copy.deepcopy( descendants )
      for desc in descendants:
        #quick and dirty...
        if '.'.join( os.path.basename( desc ).split( '.' )[1:] ).lower() not in fileTypes:
          descendantsCopy.remove( desc )
      if len( descendantsCopy ) == 0:
        ancDict.pop( ancestor )
      else:
        ancDict[ancestor] = descendantsCopy

    return ancDict

  @classmethod
  def _getFileTypesCount( self, lfnDict ):
    ''' return file types count
    '''
    ft_dict = {}
    for ancestor, descendants in lfnDict.items():
      t_dict = {}
      for desc in descendants:
        fType = '.'.join( os.path.basename( desc ).split( '.' )[1:] ).lower()
        t_dict[fType] = t_dict.setdefault( fType, 0 ) + 1
      ft_dict[ancestor] = t_dict

    return ft_dict

  ################################################################################

  def checkFC2BKK( self ):
    ''' check that files present in the FC are also in the BKK
    '''
    if not self.lfns:
      try:
        directories = self.__getDirectories()
      except RuntimeError, e:
        return S_ERROR( e )
      self.lfns = self._getFilesFromDirectoryScan( directories )

    res = self._getBKKMetadata( self.lfns )
    self.existingLFNsNotInBKK, self.existingLFNsWithBKKReplicaNO, self.existingLFNsWithBKKReplicaYES = res
    msg = ''
    if self.transType:
      msg = "For prod %s of type %s, " % ( self.prod, self.transType )
    if self.existingLFNsWithBKKReplicaNO:
      gLogger.warn( "%s %d files are in the FC but have replica = NO in BKK" % ( msg,
                                                                           len( self.existingLFNsWithBKKReplicaNO ) ) )
    if self.existingLFNsNotInBKK:
      gLogger.warn( "%s %d files are in the FC but not in BKK" % ( msg, len( self.existingLFNsNotInBKK ) ) )

  ################################################################################

  def __getDirectories( self ):
    ''' get the directories where to look into (they are either given, or taken from the transformation ID
    '''
    if self.directories:
      return self.directories
    elif self.prod:
      res = self.transClient.getTransformationParameters( self.prod, ['OutputDirectories'] )
      if not res['OK']:
        raise RuntimeError, res['Message']
      else:
        return res['Value'].split( '\n' )
    else:
      raise RuntimeError( "Need to specify either the directories or a production id" )


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

  def checkBKK2TS( self ):
    ''' check that files present in the BKK are also in the FC (re-check of BKKWatchAgent)
    '''
    try:
      bkQuery = self.__getBKKQuery( fromTS=True )
    except ValueError, e:
      return S_ERROR( e )
    lfnsReplicaYes = self._getBKKFiles( bkQuery )
    proc, nonProc = self._getTSFiles()
    self.filesInBKKNotInTS = list( set( lfnsReplicaYes ) - set( proc, nonProc ) )
    if self.filesInBKKNotInTS:
      gLogger.warn( "There are %d files in BKK that are not in TS: %s" % ( len( self.filesInBKKNotInTS ),
                                                                           str( self.filesInBKKNotInTS ) ) )

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
        gLogger.info( "Failed request for LFN %s: %s" % ( lfn, val['Failed'][lfn] ) )
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

  ################################################################################
  # properties

  def set_prod( self, value ):
    if value:
      value = int( value )
      res = self.transClient.getTransformation( value, extraParams=False )
      if not res['OK']:
        gLogger.error( "Couldn't find transformation %d: %s" % ( value, res['Message'] ) )
      else:
        self.transType = res['Value']['Type']
      gLogger.info( "Production %d has type %s" % ( value, self.transType ) )
    else:
      value = 0
    self._prod = value
  def get_prod( self ):
    return self._prod
  prod = property( get_prod, set_prod )

  def set_fileType( self, value ):
    fts = [ft.lower() for ft in value]
    self._fileType = fts
  def get_fileType( self ):
    return self._fileType
  fileType = property( get_fileType, set_fileType )

  def set_fileTypesExcluded( self, value ):
    fts = [ft.lower() for ft in value]
    self._fileTypesExcluded = fts
  def get_fileTypesExcluded( self ):
    return self._fileTypesExcluded
  fileTypesExcluded = property( get_fileTypesExcluded, set_fileTypesExcluded )

  def set_bkQuery( self, value ):
    if type( value ) == type( "" ):
      self._bkQuery = ast.literal_eval( value )
    else:
      self._bkQuery = value
  def get_bkQuery( self ):
    return self._bkQuery
  bkQuery = property( get_bkQuery, set_bkQuery )

  def set_lfns( self, value ):
    if type( value ) == type( "" ):
      value = [value]
    value = [v.replace( ' ', '' ).replace( '//', '/' ) for v in value]
    self._lfns = value
  def get_lfns( self ):
    return self._lfns
  lfns = property( get_lfns, set_lfns )

