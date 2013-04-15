""" Main class for doing consistency checks, between files in:
    - File Catalog
    - Bookkeeping
    - TransformationSystem

    Should be extended to include the Storage (in DIRAC)
"""

import os, copy, ast, time, sys

from DIRAC import gLogger, S_ERROR
from DIRAC.Core.Utilities.List import breakListIntoChunks
from DIRAC.Interfaces.API.Dirac import Dirac
from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager

from LHCbDIRAC.BookkeepingSystem.Client.BKQuery import BKQuery
from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
from LHCbDIRAC.TransformationSystem.Client.TransformationClient import TransformationClient
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations

#FIXME: this is quite dirty, what should be checked is exactly what it is done
prodsWithMerge = ( 'MCSimulation', 'DataStripping', 'MCStripping', 'DataSwimming', 'WGProduction' )

def getFileDescendants( transID, lfns, transClient = None, rm = None, bkClient = None ):
  cc = ConsistencyChecks( interactive = False, transClient = transClient, rm = rm, bkClient = bkClient )
  cc.prod = transID
  cc.fileType = []
  cc.fileTypesExcluded = Operations().getValue( 'DataConsistency/IgnoreDescendantsOfType', [] )

  return cc.getDescendants( lfns )[0]

class ConsistencyChecks( object ):
  """ A class for handling some consistency check
  """

  def __init__( self, interactive = True, transClient = None, rm = None, bkClient = None ):
    ''' c'tor

        One object for every production/BkQuery/directoriesList...
    '''
    self.interactive = interactive
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
    self.runStatus = None
    self.fromProd = None
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
    self.filesInBKKNotInTS = []

    self.processedLFNsWithDescendants = []
    self.processedLFNsWithoutDescendants = []
    self.processedLFNsWithMultipleDescendants = []
    self.nonProcessedLFNsWithDescendants = []
    self.nonProcessedLFNsWithoutDescendants = []
    self.nonProcessedLFNsWithMultipleDescendants = []
    self.descendantsForProcessedLFNs = []
    self.descendantsForNonProcessedLFNs = []

  ################################################################################

  def checkBKK2FC( self, checkAll ):
    ''' Starting from the BKK, check if the FileCatalog has consistent information (BK -> FileCatalog)

        Works either when the bkQuery is free, or when it is made using a transformation ID
    '''

    if self.lfns:
      lfnsNotInBK, lfnsReplicaNo, lfnsReplicaYes = self._getBKKMetadata( self.lfns )
      lfnsReplicaNo += lfnsNotInBK
    else:
      try:
        bkQuery = self.__getBKKQuery()
      except ValueError, e:
        return S_ERROR( e )
      if checkAll:
        lfnsReplicaNo = self._getBKKFiles( bkQuery, 'No' )
      lfnsReplicaYes = self._getBKKFiles( bkQuery, 'Yes' )

    if self.lfns:
      gLogger.verbose( 'Checking the File Catalog for those files with BKK ReplicaFlag = No' )
      self.existingLFNsWithBKKReplicaNO, self.nonExistingLFNsWithBKKReplicaNO = self.getReplicasPresence( lfnsReplicaNo )
      gLogger.verbose( 'Checking the File Catalog for those files with BKK ReplicaFlag = Yes' )
      self.existingLFNsWithBKKReplicaYES, self.nonExistingLFNsWithBKKReplicaYES = self.getReplicasPresence( lfnsReplicaYes )
    elif self.transType not in prodsWithMerge:
      # Merging and Reconstruction
      # In principle few files without replica flag, check them in FC
      if checkAll:
        gLogger.verbose( 'Checking the File Catalog for those files with BKK ReplicaFlag = No' )
        self.existingLFNsWithBKKReplicaNO, self.nonExistingLFNsWithBKKReplicaNO = self.getReplicasPresence( lfnsReplicaNo )
      gLogger.verbose( 'Checking the File Catalog for those files with BKK ReplicaFlag = Yes' )
      self.existingLFNsWithBKKReplicaYES, self.nonExistingLFNsWithBKKReplicaYES = self.getReplicasPresenceFromDirectoryScan( lfnsReplicaYes )

    else:
      # prodsWithMerge
      # In principle most files have no replica flag, start from the File Catalog files with replicas
      if checkAll:
        gLogger.verbose( 'Checking the File Catalog for those files with BKK ReplicaFlag = No' )
        self.existingLFNsWithBKKReplicaNO, self.nonExistingLFNsThatAreNotInBK = self.getReplicasPresenceFromDirectoryScan( lfnsReplicaNo )
      gLogger.verbose( 'Checking the File Catalog for those files with BKK ReplicaFlag = Yes' )
      self.existingLFNsWithBKKReplicaYES, self.nonExistingLFNsWithBKKReplicaYES = self.getReplicasPresence( lfnsReplicaYes )

    if checkAll and self.existingLFNsWithBKKReplicaNO:
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

  def _getBKKFiles( self, bkQuery, replicaFlag = 'Yes' ):
    ''' Helper function - get files from BKK, first constructing the bkQuery
    '''
    visibility = bkQuery.isVisible()
    if self.transType:
      visibility = 'Yes' if self.transType not in prodsWithMerge else 'All'
    bkQuery.setVisible( False )
    bkQueryRes = BKQuery( bkQuery, visible = visibility )
    bkQueryRes.setOption( 'ReplicaFlag', replicaFlag )
    lfnsRes = bkQueryRes.getLFNs( printOutput = False )
    if not lfnsRes:
      gLogger.info( "No files found with replica flag = %s" % replicaFlag )
    else:
      gLogger.info( "Found %d files with replica flag = %s" % ( len( lfnsRes ), replicaFlag ) )

    return lfnsRes

  def __getBKKQuery( self, fromTS = False ):
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

    chunkSize = 1000
    printProgress = ( len( lfns ) > 5 * chunkSize )
    if printProgress:
      self.__write( "Checking replicas for %d files (chunks of %d) " % ( len( lfns ), chunkSize ) )
    for chunk in breakListIntoChunks( lfns, chunkSize ):
      if printProgress:
        self.__write( '.' )
      res = self.rm.getReplicas( chunk )
      if res['OK']:
        present += res['Value']['Successful'].keys()
        notPresent += res['Value']['Failed'].keys()
    if printProgress:
      self.__write( '\n' )

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
      pr, notPr = self.__compareLFNLists( dirs[dirN], lfnsFound )
      notPresent += notPr
      present += pr

    gLogger.info( "Found %d files with replicas and %d without" % ( len( present ), len( notPresent ) ) )
    return present, notPresent

  ################################################################################

  @staticmethod
  def __compareLFNLists( lfns, lfnsFound ):
    ''' return files in both lists and files in lfns and not in lfnsFound
    '''
    present = []
    startTime = time.time()
    gLogger.verbose( "Comparing list of %d LFNs with second list of %d" % ( len( lfns ), len( lfnsFound ) ) )
    if lfnsFound:
      #print sorted( lfns )
      #print sorted( lfnsFound )
      setLfns = set( lfns )
      setLfnsFound = set( lfnsFound )
      present = list( setLfns & setLfnsFound )
      lfns = list( setLfns - setLfnsFound )
      #print sorted( present )
      #print sorted( lfns )
    gLogger.verbose( "End of comparison: %.1f seconds" % ( time.time() - startTime ) )
    return present, lfns

  def _getFilesFromDirectoryScan( self, dirs ):
    ''' calls rm.getFilesFromDirectory
    '''

    gLogger.setLevel( 'ERROR' )
    res = self.rm.getFilesFromDirectory( dirs )
    gLogger.setLevel( 'INFO' )
    if not res['OK']:
      gLogger.error( "Error getting files from directories %s:" % dirs, res['Message'] )
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
    if self.runStatus and self.fromProd:
      res = self.transClient.getTransformationRuns( {'TransformationID': self.fromProd, 'Status':self.runStatus} )
      if not res['OK']:
        gLogger.error( "Failed to get runs for transformation %d" % self.prod )
      else:
        self.runsList.extend( [run['RunNumber'] for run in res['Value']] )
        gLogger.always( "%d runs selected" % len( res['Value'] ) )
    if self.runsList:
      selectDict['RunNumber'] = self.runsList

    selectDictProcessed = copy.deepcopy( selectDict )
    selectDictProcessed['Status'] = 'Processed'
    res = self.transClient.getTransformationFiles( selectDictProcessed )
    if not res['OK']:
      gLogger.error( "Failed to get files for transformation %d" % self.prod )
    else:
      processedLFNs = [item['LFN'] for item in res['Value']]

    res = self.transClient.getTransformationFiles( selectDict )
    if not res['OK']:
      gLogger.error( "Failed to get files for transformation %d" % self.prod )
    else:
      nonProcessedLFNs = list ( set( [item['LFN'] for item in res['Value']] ) - set( processedLFNs ) )

    return processedLFNs, nonProcessedLFNs

  ################################################################################

  def __write( self, text ):
    if self.interactive:
      sys.stdout.write( text )
      sys.stdout.flush()

  def getDescendants( self, lfns ):
    ''' get the descendants of a list of LFN (for the production)
    '''
    filesWithDescendants = {}
    filesWithoutDescendants = {}
    filesWithMultipleDescendants = {}
    fileTypesExcluded = Operations().getValue( 'DataConsistency/IgnoreDescendantsOfType', [] )
    descendants = []
    if not lfns:
      return filesWithDescendants, filesWithoutDescendants, filesWithMultipleDescendants, descendants

    chunkSize = 500
    self.__write( "Now checking daughters for %d mothers (chunks of %d) " % ( len( lfns ), chunkSize ) )
    startTime = time.time()
    for lfnChunk in breakListIntoChunks( lfns, chunkSize ):
      self.__write( '.' )
      while True:
        resChunk = self.bkClient.getFileDescendants( lfnChunk, depth = 1, production = self.prod, checkreplica = False )
        if resChunk['OK']:
          descDict = self._selectByFileType( resChunk['Value']['WithMetadata'] )
          # Get the list of unique descendants
          for desc in descDict.values():
            descendants += [lfn for lfn in desc if lfn not in descendants]
          ft_count = self._getFileTypesCount( descDict )
          for lfn in lfnChunk:
            if lfn in descDict:
              filesWithDescendants.update( {lfn:descDict[lfn]} )
              for ftc in ft_count[lfn].values():
                if ftc > 1:
                  filesWithMultipleDescendants.update( {lfn:descDict[lfn]} )
            else:
              filesWithoutDescendants.update( {lfn:None} )
          break
        else:
          gLogger.warn( "\nError getting descendants for %d files, retry" % len( lfnChunk ), resChunk['Message'] )
    self.__write( ' (%.1f seconds)\n' % ( time.time() - startTime ) )
    if filesWithDescendants:
      # Now check whether these descendants files have replicas or have descendants that have replicas
      present = []
      notPresent = []
      descendants = list( set( descendants ) )
      self.__write( "Now checking presence of %d daughters (chunks of %d) " % ( len( descendants ), chunkSize ) )
      startTime = time.time()
      for lfnChunk in breakListIntoChunks( descendants, chunkSize ):
        pr, notPr = self.getReplicasPresence( lfnChunk )
        self.__write( '.' )
        present += pr
        notPresent += notPr
      self.__write( ' (%.1f seconds)\n' % ( time.time() - startTime ) )
      # Now check whether the files without replica have a descendant
      if notPresent:
        descWithDescendants = {}
        self.__write( "Now checking descendants from %d daughters without replicas (chunks of %d) " % ( len( notPresent ), 
                                                                                                        chunkSize ) )
        startTime = time.time()
        for lfnChunk in breakListIntoChunks( notPresent, chunkSize ):
          self.__write( '.' )
          while True:
            res = self.bkClient.getFileDescendants( lfnChunk, checkreplica = True )
            if res['OK']:
              descWithDescendants.update( res['Value']['WithMetadata'] )
              break
            else:
              gLogger.warn( "\nError getting descendants for %d files, retry" % len( lfnChunk ), res['Message'] )
        self.__write( ' (%.1f seconds)\n' % ( time.time() - startTime ) )

        self.__write( "Now establishing final list of relevant descendants for %d mothers (chunks of %d)" % ( len( filesWithDescendants ), chunkSize ) )
        startTime = time.time()
        i = 0
        setDescWithDescendants = set( descWithDescendants )
        for lfn, desc in filesWithDescendants.items():
          if i % chunkSize == 0:
            self.__write( '.' )
          i += 1
          # Only interested in descendants without replica
          if not [pr for pr in desc if pr in notPresent]:
            continue
          realDescendants = [pr for pr in desc if pr in present]
          lfnDict = dict( [( pr, descWithDescendants[pr] ) for pr in set( desc ).intersection( setDescWithDescendants )] )
          descToCheck = self._selectByFileType( lfnDict, fileTypesExcluded = fileTypesExcluded )
          for lfn1 in descToCheck :
            # This daughter had descendants, therefore consider it
            realDescendants.append( lfn1 )
            if lfn1 in notPresent:
              notPresent.remove( lfn1 )
          if len( realDescendants ) == 0:
            gLogger.verbose( '%s has no real descendants' % lfn )
            filesWithMultipleDescendants.pop( lfn , None )
            filesWithDescendants.pop( lfn, None )
            filesWithoutDescendants.update( {lfn:None} )
          elif len( realDescendants ) == 1:
            filesWithMultipleDescendants.pop( lfn, None )
          else:
            # still multiple descendants!
            pass
        for lfn1 in notPresent:
          descendants.remove( lfn1 )
        self.__write( ' (%.1f seconds)\n' % ( time.time() - startTime ) )

    return filesWithDescendants, filesWithoutDescendants, filesWithMultipleDescendants, descendants

  ################################################################################

  def _selectByFileType( self, lfnDict, fileTypes = [], fileTypesExcluded = [] ):
    ''' Select only those files from the values of lfnDict that have a certain type
    '''
    if not lfnDict:
      return {}
    if fileTypes == [] and fileTypesExcluded == []:
      fileTypes = list( set( self.fileType ) - set ( self.fileTypesExcluded ) )
    fileTypesExcluded += self.fileTypesExcluded
    ancDict = copy.deepcopy( lfnDict )
    metadata = {}
    if type( ancDict.values()[0] ) == type( ':' ):
      for lfnChunk in breakListIntoChunks( [lfn for desc in ancDict.values() for lfn in desc], 1000 ):
        res = self.bkClient.getFileMetadata( lfnChunk )
        if res['OK']:
          metadata.update( res['Value']['Successful'] )
        else:
          gLogger.error( "Error getting %d files metadata" % len( lfnChunk ), res['Message'] )
    else:
      for descDict in ancDict.values():
        metadata.update( descDict )
    for ancestor, descendants in ancDict.items():
      descendants = list( descendants )
      for desc in list( descendants ):
        if metadata[desc]['FileType'] in fileTypesExcluded or ( fileTypes and metadata[desc]['FileType'] not in fileTypes ):
          descendants.remove( desc )
      if len( descendants ) == 0:
        ancDict.pop( ancestor )
      else:
        ancDict[ancestor] = descendants

    return ancDict

  @staticmethod
  def _getFileTypesCount( lfnDict ):
    ''' return file types count
    '''
    ft_dict = {}
    for ancestor, descendants in lfnDict.items():
      t_dict = {}
      for desc in descendants:
        fType = '.'.join( os.path.basename( desc ).split( '.' )[1:] ).upper()
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
      present = self._getFilesFromDirectoryScan( directories )
      prStr = ' are in the FC but'
    else:
      present, notPresent = self.getReplicasPresence( self.lfns )
      gLogger.always( 'Out of %d files, %d are in the FC, %d are not' % ( len( self.lfns ), present, notPresent ) )
      if not present:
        gLogger.always( 'No files are in the FC, no check in the BK. Use dirac-dms-check-bkk2fc instead' )
        return
      prStr = ''

    res = self._getBKKMetadata( present )
    self.existingLFNsNotInBKK, self.existingLFNsWithBKKReplicaNO, self.existingLFNsWithBKKReplicaYES = res
    msg = ''
    if self.transType:
      msg = "For prod %s of type %s, " % ( self.prod, self.transType )
    if self.existingLFNsWithBKKReplicaNO:
      gLogger.warn( "%s %d files%s have replica = NO in BKK" % ( msg, len( self.existingLFNsWithBKKReplicaNO ),
                                                                 prStr ) )
    if self.existingLFNsNotInBKK:
      gLogger.warn( "%s %d files%s not in BKK" % ( msg, len( self.existingLFNsNotInBKK ), prStr ) )

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
      metadata = res['Value']['Successful']
      missingLFNs = [lfn for lfn in lfns if metadata.get( lfn, {} ).get( 'GotReplica' ) == None]
      noFlagLFNs = [lfn for lfn in lfns if metadata.get( lfn, {} ).get( 'GotReplica' ) == 'No']
      okLFNs = [lfn for lfn in lfns if metadata.get( lfn, {} ).get( 'GotReplica' ) == 'Yes']

    return missingLFNs, noFlagLFNs, okLFNs

  ################################################################################

  def checkBKK2TS( self ):
    ''' check that files present in the BKK are also in the FC (re-check of BKKWatchAgent)
    '''
    try:
      bkQuery = self.__getBKKQuery( fromTS = True )
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
      res = self.transClient.getTransformation( value, extraParams = False )
      if not res['OK']:
        gLogger.error( "Couldn't find transformation %d: %s" % ( value, res['Message'] ) )
      else:
        self.transType = res['Value']['Type']
      if self.interactive:
        gLogger.info( "Production %d has type %s" % ( value, self.transType ) )
    else:
      value = 0
    self._prod = value
  def get_prod( self ):
    return self._prod
  prod = property( get_prod, set_prod )

  def set_fileType( self, value ):
    fts = [ft.upper() for ft in value]
    self._fileType = fts
  def get_fileType( self ):
    return self._fileType
  fileType = property( get_fileType, set_fileType )

  def set_fileTypesExcluded( self, value ):
    fts = [ft.upper() for ft in value]
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

