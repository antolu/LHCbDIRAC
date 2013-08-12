""" Main class for doing consistency checks, between files in:
    - File Catalog
    - Bookkeeping
    - TransformationSystem

    Should be extended to include the Storage (in DIRAC)
"""

import os, copy, ast, time, sys

import DIRAC
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
    self.inBKNotInFC = []
    self.inFCNotInBK = []
    self.removedFiles = []

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
      gLogger.always( 'Getting files for BK query %s...' % str( bkQuery ) )
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
    bkQuery.setVisible( 'All' )
    bkQueryRes = BKQuery( bkQuery, visible = visibility )
    bkQueryRes.setOption( 'ReplicaFlag', replicaFlag )
    startTime = time.time()
    lfnsRes = bkQueryRes.getLFNs( printOutput = False )
    if not lfnsRes:
      gLogger.info( "No files found with replica flag = %s" % replicaFlag )
    else:
      gLogger.info( "Found %d files with replica flag = %s (%.1f seconds)" % ( len( lfnsRes ), replicaFlag, time.time() - startTime ) )

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
    printProgress = ( len( lfns ) > chunkSize )
    startTime = time.time()
    self.__write( "Checking replicas for %d files%s" % ( len( lfns ), ( ' (chunks of %d)' % chunkSize ) if printProgress else '... ' ) )
    for chunk in breakListIntoChunks( lfns, chunkSize ):
      if printProgress:
        self.__write( '.' )
      while True:
        res = self.rm.getReplicas( chunk )
        if res['OK']:
          present += res['Value']['Successful'].keys()
          notPresent += res['Value']['Failed'].keys()
          break
        else:
          gLogger.error( "\nError getting replicas from FC, retry", res['Message'] )
    self.__write( ' (%.1f seconds)\n' % ( time.time() - startTime ) )

    gLogger.info( "Found %d files with replicas and %d without" % ( len( present ), len( notPresent ) ) )
    return present, notPresent

  ################################################################################

  def getReplicasPresenceFromDirectoryScan( self, lfns ):
    ''' Get replicas scanning the directories. Might be faster.
    '''

    dirs = {}
    present = []
    notPresent = []
    compare = True

    for lfn in lfns:
      dirN = os.path.dirname( lfn )
      if lfn == dirN + '/':
        compare = False
      dirs.setdefault( dirN, [] ).append( lfn )

    if compare:
      self.__write( "Checking File Catalog for %d files from %d directories " % ( len( lfns ), len( dirs ) ) )
    else:
      self.__write( "Getting files from %d directories " % len( dirs ) )
    startTime = time.time()

    for dirN in sorted( dirs ):
      startTime1 = time.time()
      self.__write( '.' )
      lfnsFound = self._getFilesFromDirectoryScan( dirN )
      gLogger.verbose( "Obtained %d files in %.1f seconds" % ( len( lfnsFound ), time.time() - startTime1 ) )
      if compare:
        pr, notPr = self.__compareLFNLists( dirs[dirN], lfnsFound )
        notPresent += notPr
        present += pr
      else:
        present += lfnsFound

    self.__write( ' (%.1f seconds)\n' % ( time.time() - startTime ) )
    gLogger.info( "Found %d files with replicas and %d without" % ( len( present ), len( notPresent ) ) )
    return present, notPresent

  ################################################################################

  @staticmethod
  def __compareLFNLists( lfns, lfnsFound ):
    ''' return files in both lists and files in lfns and not in lfnsFound
    '''
    present = []
    notPresent = lfns
    startTime = time.time()
    gLogger.verbose( "Comparing list of %d LFNs with second list of %d" % ( len( lfns ), len( lfnsFound ) ) )
    if lfnsFound:
      #print sorted( lfns )
      #print sorted( lfnsFound )
      setLfns = set( lfns )
      setLfnsFound = set( lfnsFound )
      present = list( setLfns & setLfnsFound )
      notPresent = list( setLfns - setLfnsFound )
      #print sorted( present )
      #print sorted( notPresent )
    gLogger.verbose( "End of comparison: %.1f seconds" % ( time.time() - startTime ) )
    return present, notPresent

  def _getFilesFromDirectoryScan( self, dirs ):
    ''' calls rm.getFilesFromDirectory
    '''

    level = gLogger.getLevel()
    gLogger.setLevel( 'FATAL' )
    res = self.rm.getFilesFromDirectory( dirs )
    gLogger.setLevel( level )
    if not res['OK']:
      if 'No such file or directory' not in res['Message']:
        gLogger.error( "Error getting files from directories %s:" % dirs, res['Message'] )
      return []
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

    gLogger.always( 'Getting files from the TransformationSystem...' )
    startTime = time.time()
    processedLFNs, nonProcessedLFNs, statuses = self._getTSFiles()
    gLogger.always( 'Found %d processed files and %d non processed%s files (%.1f seconds)' % ( len( processedLFNs ),
                                                                              len( nonProcessedLFNs ),
                                                                              ' (%s)' % ','.join( statuses ) if statuses else '',
                                                                              ( time.time() - startTime ) ) )

    res = self.getDescendants( processedLFNs, status = 'processed' )
    self.processedLFNsWithDescendants = res[0]
    self.processedLFNsWithoutDescendants = res[1]
    self.processedLFNsWithMultipleDescendants = res[2]
    self.descendantsForProcessedLFNs = res[3]
    self.inFCNotInBK = res[4]
    self.inBKNotInFC = res[5]
    self.removedFiles = res[6]

    res = self.getDescendants( nonProcessedLFNs, status = 'non-processed' )
    self.nonProcessedLFNsWithDescendants = res[0]
    self.nonProcessedLFNsWithoutDescendants = res[1]
    self.nonProcessedLFNsWithMultipleDescendants = res[2]
    self.descendantsForNonProcessedLFNs = res[3]

    if self.processedLFNsWithoutDescendants:
      gLogger.verbose( "For prod %s of type %s, %d files are processed, and %d of those do not have descendants" \
                     % ( self.prod, self.transType, len( processedLFNs ), len( self.processedLFNsWithoutDescendants ) ) )

    if self.processedLFNsWithMultipleDescendants:
      gLogger.verbose( "For prod %s of type %s, %d files are processed, and %d of those have multiple descendants: " \
                     % ( self.prod, self.transType, len( processedLFNs ), len( self.processedLFNsWithMultipleDescendants ) ) )

    if self.nonProcessedLFNsWithDescendants:
      gLogger.verbose( "For prod %s of type %s, %d files are not processed, but %d of those have descendants" % \
                     ( self.prod, self.transType, len( nonProcessedLFNs ), len( self.nonProcessedLFNsWithDescendants ) ) )

    if self.nonProcessedLFNsWithMultipleDescendants:
      gLogger.verbose( "For prod %s of type %s, %d files are not processed, but %d of those have multiple descendants: " % \
                     ( self.prod, self.transType, len( nonProcessedLFNs ), len( self.nonProcessedLFNsWithMultipleDescendants ) ) )

  ################################################################################

  def _getTSFiles( self ):
    ''' Helper function - get files from the TS
    '''

    selectDict = { 'TransformationID': self.prod}
    if self._lfns:
      selectDict['LFN'] = self._lfns
    elif self.runStatus and self.fromProd:
      res = self.transClient.getTransformationRuns( {'TransformationID': self.fromProd, 'Status':self.runStatus} )
      if not res['OK']:
        gLogger.error( "Failed to get runs for transformation %d" % self.prod )
      else:
        if res['Value']:
          self.runsList.extend( [run['RunNumber'] for run in res['Value'] if run['RunNumber'] not in self.runsList] )
          gLogger.always( "%d runs selected" % len( res['Value'] ) )
        elif not self.runsList:
          gLogger.always( "No runs selected, check completed" )
          DIRAC.exit( 0 )
    if not self._lfns and self.runsList:
      selectDict['RunNumber'] = self.runsList

    res = self.transClient.getTransformationFiles( selectDict )
    if not res['OK']:
      gLogger.error( "Failed to get files for transformation %d" % self.prod, res['Message'] )
      return [], [], []
    else:
      processedLFNs = [item['LFN'] for item in res['Value'] if item['Status'] == 'Processed']
      nonProcessedLFNs = [item['LFN'] for item in res['Value'] if item['Status'] != 'Processed']
      nonProcessedStatuses = list( set( [item['Status'] for item in res['Value'] if item['Status'] != 'Processed'] ) )

    return processedLFNs, nonProcessedLFNs, nonProcessedStatuses

  ################################################################################

  def __write( self, text ):
    if self.interactive:
      sys.stdout.write( text )
      sys.stdout.flush()

  def getDescendants( self, lfns, status = '' ):
    ''' get the descendants of a list of LFN (for the production)
    '''
    if type( lfns ) == type( '' ):
      lfns = [lfns]
    elif type( lfns ) == type( {} ):
      lfns = lfns.keys()
    filesWithDescendants = {}
    filesWithoutDescendants = {}
    filesWithMultipleDescendants = {}
    fileTypesExcluded = Operations().getValue( 'DataConsistency/IgnoreDescendantsOfType', [] )
    daughtersBKInfo = {}
    inFCNotInBK = []
    inBKNotInFC = []
    allDaughters = []
    removedFiles = []
    if not lfns:
      return filesWithDescendants, filesWithoutDescendants, filesWithMultipleDescendants, \
        allDaughters, inFCNotInBK, inBKNotInFC, removedFiles

    chunkSize = 100 if self.transType == 'DataStripping' and len( self.fileType ) > 1 else 500
    self.__write( "Now getting daughters for %d %s mothers in production %d (chunks of %d) "
                  % ( len( lfns ), status, self.prod, chunkSize ) )
    startTime = time.time()
    for lfnChunk in breakListIntoChunks( lfns, chunkSize ):
      self.__write( '.' )
      while True:
        resChunk = self.bkClient.getFileDescendants( lfnChunk, depth = 1,
                                                     production = self.prod, checkreplica = False )
        if resChunk['OK']:
          # Key is ancestor, value is metadata dictionary of daughters
          descDict = self._selectByFileType( resChunk['Value']['WithMetadata'] )
          # Do the daughters have a replica flag in BK? Store file type as well... Key is daughter
          daughtersBKInfo.update( dict( ( lfn, ( desc[lfn]['GotReplica'] == 'Yes', desc[lfn]['FileType'] ) )
                                      for desc in descDict.values() for lfn in desc ) )
          # Count the daughters per file type (key is ancestor)
          ft_count = self._getFileTypesCount( descDict )
          for lfn in lfnChunk:
            # Check if file has a daughter and how many per file type
            if lfn in descDict:
              # Assign the daughters list to the initial LFN
              filesWithDescendants[lfn] = descDict[lfn].keys()
              # Is there a file type with more than one daughter of a given file type?
              multi = dict( [( ft, ftc ) for ft, ftc in ft_count[lfn].items() if ftc > 1] )
              if multi:
                filesWithMultipleDescendants[lfn] = multi
            else:
              # No daughter, easy case!
              filesWithoutDescendants[lfn] = None
          break
        else:
          gLogger.error( "\nError getting daughters for %d files, retry"
                        % len( lfnChunk ), resChunk['Message'] )
    self.__write( ' (%.1f seconds)\n' % ( time.time() - startTime ) )

    # This is the list of all daughters, sets will contain unique entries
    setAllDaughters = set( daughtersBKInfo )
    allDaughters = list( setAllDaughters )
    inBK = set( [lfn for lfn in setAllDaughters if daughtersBKInfo[lfn][0]] )
    setRealDaughters = set()
    # Now check whether these daughters files have replicas or have descendants that have replicas
    if filesWithDescendants:
      # First check in LFC the presence of daughters
      if len( allDaughters ) > 10 * chunkSize and len( inBK ) < len( allDaughters ) / 2:
        present, notPresent = self.getReplicasPresenceFromDirectoryScan( allDaughters )
      else:
        present, notPresent = self.getReplicasPresence( allDaughters )

      setPresent = set( present )
      setRealDaughters = setPresent
      setNotPresent = set( notPresent )
      # Get list of unique daughters
      notPresent = list( setNotPresent )
      # Now check consistency between BK and FC for daughters
      inBKNotInFC = list( inBK & setNotPresent )
      inFCNotInBK = list( setPresent - inBK )

      # Now check whether the daughters without replica have a descendant
      if notPresent:
        chunkSize = 500
        startTime = time.time()
        self.__write( "Now checking descendants from %d daughters without replicas (chunks of %d) "
                      % ( len( notPresent ), chunkSize ) )
        # Get existing descendants of notPresent daughters
        setDaughtersWithDesc = set()
        for lfnChunk in breakListIntoChunks( notPresent, chunkSize ):
          self.__write( '.' )
          while True:
            res = self.bkClient.getFileDescendants( lfnChunk, depth = 99, checkreplica = True )
            if res['OK']:
              # Exclude ignored file types, but select any other file type, key is daughters
              setDaughtersWithDesc.update( self._selectByFileType( res['Value']['WithMetadata'], fileTypes = [''],
                                                                   fileTypesExcluded = fileTypesExcluded ) )
              break
            else:
              gLogger.error( "\nError getting descendants for %d files, retry"
                             % len( lfnChunk ), res['Message'] )
        self.__write( ' (%.1f seconds)\n' % ( time.time() - startTime ) )
        #print "%d not Present daughters, %d have a descendant" % ( len( notPresent ), len( setDaughtersWithDesc ) )

        startTime = time.time()
        chunkSize = 500
        self.__write( "Now establishing final list of existing descendants for %d mothers (chunks of %d)"
                      % ( len( filesWithDescendants ), chunkSize ) )
        i = -1
        for lfn in set( filesWithDescendants ):
          verbose = False
          setDaughters = set( filesWithDescendants[lfn] )
          i += 1
          if i % chunkSize == 0:
            self.__write( '.' )
          # If all daughters are present, all is easy...
          daughtersNotPresent = setDaughters & setNotPresent
          if not daughtersNotPresent:
            continue
          if verbose:
            print '\n\nLFN', lfn
            print 'Daughters', sorted( filesWithDescendants[lfn] )
            print 'Not present daughters', sorted( list( daughtersNotPresent ) )
            #print 'Multiple descendants', filesWithMultipleDescendants.get( lfn )
          # Only interested in daughters without replica, so if all have one, skip

          #Some daughters may have a replica though, take them into account
          daughtersWithReplica = setDaughters & setPresent
          # and add those without a replica but that have  a descendant with replica
          realDaughters = list( daughtersWithReplica.union( daughtersNotPresent & setDaughtersWithDesc ) )
          if verbose:
            print 'realDaughters', realDaughters
          # descToCheck: dictionary with key = daughter and value = dictionary of file type counts
          daughtersDict = dict( [( daughter, {daughter:{'FileType':daughtersBKInfo[daughter][1]}} ) for daughter in realDaughters] )
          if verbose:
            print 'daughtersDict', daughtersDict
          descToCheck = self._getFileTypesCount ( daughtersDict )
          if verbose:
            print 'descToCheck', descToCheck

          # Update the result dictionaries according to the final set of descendants
          if len( descToCheck ) == 0:
            # Mother has no descendant
            gLogger.verbose( '%s has no real descendants' % lfn )
            filesWithMultipleDescendants.pop( lfn, None )
            filesWithDescendants.pop( lfn, None )
            filesWithoutDescendants[lfn] = None
          else:
            filesWithDescendants[lfn] = realDaughters
            setRealDaughters.update( realDaughters )
            # Count the descendants by file type
            ft_count = {}
            for counts in descToCheck.values():
              for ft in counts:
                ft_count[ft] = ft_count.setdefault( ft, 0 ) + counts.get( ft, 0 )
            if verbose:
              print 'ft_count', ft_count
            multi = dict( [( ft, ftc ) for ft, ftc in ft_count.items() if ftc > 1] )
            if verbose:
              print 'Multi', multi
            # Mother has at least one real descendant
            # Now check whether there are more than one descendant of the same file type
            if not multi:
              filesWithMultipleDescendants.pop( lfn, None )
              prStr = 'single'
            else:
              filesWithMultipleDescendants[lfn] = multi
              prStr = 'multiple'
            gLogger.verbose( '%s has %s descendants: %s' % ( lfn, prStr, sorted( descToCheck ) ) )
        self.__write( ' (%.1f seconds)\n' % ( time.time() - startTime ) )
        startTime = time.time()
        gLogger.verbose( "Reduced list of descendants in %.1f seconds" % ( time.time() - startTime ) )
    #print 'Final multiple descendants', filesWithMultipleDescendants

    # File files without descendants don't exist, not important
    if filesWithoutDescendants:
      present, removedFiles = self.getReplicasPresence( filesWithoutDescendants.keys() )
      filesWithoutDescendants = dict.fromkeys( present )
    else:
      removedFiles = []

    # Remove files with multiple descedants from files with descendants
    for lfn in filesWithMultipleDescendants:
      filesWithDescendants.pop( lfn, None )
    # For files in FC and not in BK, ignore if they are not active
    if inFCNotInBK:
      inFCNotInBK = self.getReplicasPresence( inFCNotInBK )[0]
    return filesWithDescendants, filesWithoutDescendants, filesWithMultipleDescendants, \
      list( setRealDaughters ), inFCNotInBK, inBKNotInFC, removedFiles

  ################################################################################

  def _selectByFileType( self, lfnDict, fileTypes = None, fileTypesExcluded = None ):
    ''' Select only those files from the values of lfnDict that have a certain type
    '''
    if not lfnDict:
      return {}
    if not fileTypes:
      fileTypes = self.fileType
    if not fileTypesExcluded:
      fileTypesExcluded = self.fileTypesExcluded
    else:
      fileTypesExcluded += [ft for ft in self.fileTypesExcluded if ft not in fileTypesExcluded]
    # lfnDict is a dictionary of dictionaries including the metadata, create a deep copy to get modified
    ancDict = copy.deepcopy( lfnDict )
    if fileTypes == ['']:
      fileTypes = []
    # and loop on the original dictionaries
    for ancestor in lfnDict:
      for desc in lfnDict[ancestor]:
        ft = lfnDict[ancestor][desc]['FileType']
        if ft in fileTypesExcluded or ( fileTypes and ft not in fileTypes ):
          ancDict[ancestor].pop( desc )
      if len( ancDict[ancestor] ) == 0:
        ancDict.pop( ancestor )
    return ancDict

  @staticmethod
  def _getFileTypesCount( lfnDict ):
    ''' return file types count
    '''
    ft_dict = {}
    for ancestor in lfnDict:
      t_dict = {}
      for desc in lfnDict[ancestor]:
        ft = lfnDict[ancestor][desc]['FileType']
        t_dict[ft] = t_dict.setdefault( ft, 0 ) + 1
      ft_dict[ancestor] = t_dict

    return ft_dict

  ################################################################################

  def checkFC2BKK( self ):
    ''' check that files present in the FC are also in the BKK
    '''
    if not self.lfns:
      try:
        directories = []
        for dirName in self.__getDirectories():
          if not dirName.endswith( '/' ):
            dirName += '/'
          directories.append( dirName )
      except RuntimeError, e:
        return S_ERROR( e )
      present, notPresent = self.getReplicasPresenceFromDirectoryScan( directories )
      gLogger.always( '%d files found in the FC' % len( present ) )
      prStr = ' are in the FC but'
    else:
      present, notPresent = self.getReplicasPresence( self.lfns )
      gLogger.always( 'Out of %d files, %d are in the FC, %d are not' \
                      % ( len( self.lfns ), len( present ), len( notPresent ) ) )
      if not present:
        gLogger.always( 'No files are in the FC, no check in the BK. Use dirac-dms-check-bkk2fc instead' )
        return
      prStr = ''

    res = self._getBKKMetadata( present )
    self.existingLFNsNotInBKK = res[0]
    self.existingLFNsWithBKKReplicaNO = res[1]
    self.existingLFNsWithBKKReplicaYES = res[2]
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
        directories = []
        dirList = res['Value'].split( '\n' )
        for dirName in dirList:
          # There is a shortcut when multiple streams are used, only the stream name is repeated!
          if ';' in dirName:
            items = dirName.split( ';' )
            baseDir = os.path.dirname( items[0] )
            items[0] = os.path.basename( items[0] )
            lastItems = items[-1].split( '/' )
            items[-1] = lastItems[0]
            if len( lastItems ) > 1:
              suffix = '/'.join( lastItems[1:] )
            else:
              suffix = ''
            for it in items:
              directories.append( os.path.join( baseDir, it, suffix ) )
          else:
            directories.append( dirName )
        return directories
    else:
      raise RuntimeError( "Need to specify either the directories or a production id" )


  ################################################################################

  def _getBKKMetadata( self, lfns ):
    ''' get metadata (i.e. replica flag) of a list of LFNs
    '''
    missingLFNs = []
    noFlagLFNs = {}
    okLFNs = []
    chunkSize = 1000
    startTime = time.time()
    self.__write( 'Getting %d files metadata from BK (chunks of %d) ' % ( len( lfns ), chunkSize ) )
    for lfnChunk in breakListIntoChunks( lfns, chunkSize ):
      self.__write( '.' )
      while True:
        res = self.bkClient.getFileMetadata( lfnChunk )
        if not res['OK']:
          gLogger.error( "\nCan't get the bkk metadata, retry: ", res['Message'] )
        else:
          metadata = res['Value']['Successful']
          missingLFNs += [lfn for lfn in lfnChunk if lfn not in metadata]
          noFlagLFNs.update( dict( [( lfn, metadata[lfn]['RunNumber'] )
                                    for lfn in metadata if metadata[lfn]['GotReplica'] == 'No'] ) )
          okLFNs += [lfn for lfn in metadata if metadata[lfn]['GotReplica'] == 'Yes']
          break
    self.__write( ' (%.1f seconds)\n' % ( time.time() - startTime ) )
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
    proc, nonProc, _statuses = self._getTSFiles()
    self.filesInBKKNotInTS = list( set( lfnsReplicaYes ) - set( proc + nonProc ) )
    if self.filesInBKKNotInTS:
      gLogger.error( "There are %d files in BKK that are not in TS: %s" % ( len( self.filesInBKKNotInTS ),
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
        raise RuntimeError, "Couldn't find transformation %d: %s" % ( value, res['Message'] )
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

