"""
    LHCb class for doing consistency checks, between files in:
    - Bookkeeping
"""

import time
import ast
import os

import DIRAC

from DIRAC import gLogger

from DIRAC.DataManagementSystem.Client.ConsistencyInspector import ConsistencyInspector as DiracConsistencyChecks
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.Core.Utilities.List import breakListIntoChunks
from DIRAC.Resources.Storage.StorageElement import StorageElement
from DIRAC.Core.Utilities.Adler import compareAdler

from LHCbDIRAC.BookkeepingSystem.Client.BKQuery import BKQuery
from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
from LHCbDIRAC.TransformationSystem.Client.TransformationClient import TransformationClient

prodsWithMerge = ( 'MCSimulation', 'DataStripping', 'MCStripping', 'DataSwimming', 'WGProduction' )


def getFileDescendants( transID, lfns, transClient = None, dm = None, bkClient = None ):
  cc = ConsistencyChecks( interactive = False, transClient = transClient, dm = dm, bkClient = bkClient )
  cc.prod = transID
  cc.fileType = []
  cc.fileTypesExcluded = Operations().getValue( 'DataConsistency/IgnoreDescendantsOfType', [] )
  savedLevel = gLogger.getLevel()
  gLogger.setLevel( 'FATAL' )
  descendants = cc.getDescendants( lfns )[0]
  gLogger.setLevel( savedLevel )
  return descendants

# FIXME: this is quite dirty, what should be checked is exactly what it is done
prodsWithMerge = ( 'MCSimulation', 'DataStripping', 'MCStripping', 'DataSwimming', 'WGProduction' )

class ConsistencyChecks( DiracConsistencyChecks ):
  """ LHCb extension to ConsistencyChecks
  """

  def __init__( self, interactive = True, transClient = None, dm = None, bkClient = None, fc = None ):
    """ c'tor
    """
    super( ConsistencyChecks, self ).__init__( interactive, transClient, dm, fc )

    self.bkClient = BookkeepingClient() if bkClient is None else bkClient
    self.transClient = TransformationClient() if transClient is None else transClient

    # Results of the checks
    self.existLFNsBKRepNo = {}
    self.absentLFNsBKRepNo = []
    self.existLFNsBKRepYes = []
    self.absentLFNsBKRepYes = []
    self.existLFNsNotInBK = []
    self.absentLFNsNotInBK = []
    self.filesInBKNotInTS = []

    self.inBKNotInFC = []
    self.inFCNotInBK = []

  ################################################################################

  def __getLFNsFromBK( self, checkAll = False ):
    lfnsReplicaNo, lfnsReplicaYes = ( 0, 0 )
    if self.lfns:
      lfnsNotInBK, lfnsReplicaNo, lfnsReplicaYes = self._getBKMetadata( self.lfns )
      lfnsReplicaNo = lfnsReplicaNo.keys() + lfnsNotInBK
    else:
      bkQuery = self.__getBKQuery()
      gLogger.always( 'Getting files for BK query %s...' % str( bkQuery ) )
      if checkAll:
        lfnsReplicaNo = self._getBKFiles( bkQuery, 'No' )
      lfnsReplicaYes = self._getBKFiles( bkQuery, 'Yes' )
    return lfnsReplicaNo, lfnsReplicaYes

  def checkBK2FC( self, checkAll ):
    """ Starting from the BK, check if the FileCatalog has consistent information (BK -> FileCatalog)

        Works either when the bkQuery is free, or when it is made using a transformation ID
    """
    lfnsReplicaNo, lfnsReplicaYes = self.__getLFNsFromBK( checkAll )

    if self.lfns:
      gLogger.verbose( 'Checking the File Catalog for those files with BK ReplicaFlag = No' )
      self.existLFNsBKRepNo, self.absentLFNsBKRepNo = self.getReplicasPresence( lfnsReplicaNo )
      gLogger.verbose( 'Checking the File Catalog for those files with BK ReplicaFlag = Yes' )
      self.existLFNsBKRepYes, self.absentLFNsBKRepYes = self.getReplicasPresence( lfnsReplicaYes )
    elif self.transType not in prodsWithMerge:
      # Merging and Reconstruction
      # In principle few files without replica flag, check them in FC
      if checkAll:
        gLogger.verbose( 'Checking the File Catalog for those files with BK ReplicaFlag = No' )
        self.existLFNsBKRepNo, self.absentLFNsBKRepNo = self.getReplicasPresence( lfnsReplicaNo )
      gLogger.verbose( 'Checking the File Catalog for those files with BK ReplicaFlag = Yes' )
      self.existLFNsBKRepYes, self.absentLFNsBKRepYes = self.getReplicasPresenceFromDirectoryScan( lfnsReplicaYes )

    else:
      # prodsWithMerge
      # In principle most files have no replica flag, start from the File Catalog files with replicas
      if checkAll:
        gLogger.verbose( 'Checking the File Catalog for those files with BK ReplicaFlag = No' )
        self.existLFNsBKRepNo, self.absentLFNsNotInBK = self.getReplicasPresenceFromDirectoryScan( lfnsReplicaNo )
      gLogger.verbose( 'Checking the File Catalog for those files with BK ReplicaFlag = Yes' )
      self.existLFNsBKRepYes, self.absentLFNsBKRepYes = self.getReplicasPresence( lfnsReplicaYes )

    if checkAll and self.existLFNsBKRepNo:
      msg = "%d files have ReplicaFlag = No, but %d are in the FC" % ( len( lfnsReplicaNo ),
                                                                       len( self.existLFNsBKRepNo ) )
      if self.transType:
        msg = "For prod %s of type %s, " % ( self.prod, self.transType ) + msg
      gLogger.info( msg )

    if self.absentLFNsBKRepYes:
      msg = "%d files have ReplicaFlag = Yes, but %d are not in the FC" % ( len( lfnsReplicaYes ),
                                                                            len( self.absentLFNsBKRepYes ) )
      if self.transType:
        msg = "For prod %s of type %s, " % ( self.prod, self.transType ) + msg
      gLogger.info( msg )

    ################################################################################

  def _getBKFiles( self, bkQuery, replicaFlag = 'Yes' ):
    """ Helper function - get files from BK, first constructing the bkQuery
    """
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
      gLogger.info( "Found %d files with replica flag = %s (%.1f seconds)" %
                    ( len( lfnsRes ), replicaFlag, time.time() - startTime ) )

    return lfnsRes

  def __getBKQuery( self, fromTS = False ):
    """ get the bkQuery to be used
    """
    bkQuery = None
    if fromTS:
      res = self.transClient.getBookkeepingQuery( self.prod )
      if not res['OK']:
        raise ValueError( res['Message'] )
      bkQuery = BKQuery( res['Value'] )
    else:
      if self.bkQuery:
        bkQuery = self.bkQuery
      if self.prod:
        if not self.bkQuery:
          bkQuery = BKQuery( prods = self.prod, fileTypes = self.fileType )
        else:
          bkQuery = BKQuery( self.bkQuery.setOption( "Production", self.prod ) )
      if not bkQuery:
        raise ValueError( "Need to specify either the bkQuery or a production id" )

    return bkQuery

  ################################################################################

  def checkTS2BK( self ):
    """ Check if lfns has descendants (TransformationFiles -> BK)
    """
    if not self.prod:
      raise ValueError( "You need a transformationID" )

    gLogger.always( 'Getting files from the TransformationSystem...' )
    startTime = time.time()
    processedLFNs, nonProcessedLFNs, statuses = self._getTSFiles()
    gLogger.always( 'Found %d processed files and %d non processed%s files (%.1f seconds)' %
                    ( len( processedLFNs ),
                      len( nonProcessedLFNs ),
                      ' (%s)' % ','.join( statuses ) if statuses else '',
                      ( time.time() - startTime ) ) )

    res = self.getDescendants( processedLFNs, status = 'processed' )
    self.prcdWithDesc = res[0]
    self.prcdWithoutDesc = res[1]
    self.prcdWithMultDesc = res[2]
    self.descForPrcdLFNs = res[3]
    self.inFCNotInBK = res[4]
    self.inBKNotInFC = res[5]
    self.removedFiles = res[6]

    res = self.getDescendants( nonProcessedLFNs, status = 'non-processed' )
    self.nonPrcdWithDesc = res[0]
    self.nonPrcdWithoutDesc = res[1]
    self.nonPrcdWithMultDesc = res[2]
    self.descForNonPrcdLFNs = res[3]

    if self.prcdWithoutDesc:
      gLogger.verbose( "For prod %s of type %s, %d files are processed, and %d of those do not have descendants" %
                       ( self.prod, self.transType, len( processedLFNs ), len( self.prcdWithoutDesc ) ) )

    if self.prcdWithMultDesc:
      gLogger.verbose( "For prod %s of type %s, %d files are processed, and %d of those have multiple descendants: " %
                       ( self.prod, self.transType, len( processedLFNs ), len( self.prcdWithMultDesc ) ) )

    if self.nonPrcdWithDesc:
      gLogger.verbose( "For prod %s of type %s, %d files are not processed, but %d of those have descendants" %
                       ( self.prod, self.transType, len( nonProcessedLFNs ), len( self.nonPrcdWithDesc ) ) )

    if self.nonPrcdWithMultDesc:
      gLogger.verbose( "For prod %s of type %s, %d files are not processed, but %d of those have multiple descendants: " %
                       ( self.prod, self.transType, len( nonProcessedLFNs ), len( self.nonPrcdWithMultDesc ) ) )

  ################################################################################

  def checkAncestors( self ):
    """ Check if a set of files don't share a common ancestor
    """
    if self.lfns:
      files = self.lfns
      bkQuery = None
      fileType = []
    else:
      bkQuery = self.__getBKQuery()
      gLogger.always( "Getting files for BK query: %s" % bkQuery )
      fileType = bkQuery.getFileTypeList()
      files = self._getBKFiles( bkQuery )

    if len( fileType ) == 1:
      fileTypes = { fileType[0]: set( files )}
      getFileType = False
    else:
      fileTypes = {}
      getFileType = True

    chunkSize = 100
    ancestors = {}
    listAncestors = []
    self.__write( 'Getting ancestors for %d files (chunks of %d)' % ( len( files ), chunkSize ) )
    for lfnChunk in breakListIntoChunks( files, chunkSize ):
      self.__write( '.' )
      if getFileType:
        res = self.bkClient.getFileMetadata( lfnChunk )
        if not res['OK']:
          gLogger.fatal( 'Error getting files metadata', res['Message'] )
          DIRAC.exit( 2 )
        for lfn, metadata in res['Value']['Successful'].items():
          fileType = metadata['FileType']
          fileTypes.setdefault( fileType, set() ).add( lfn )
      res = self.bkClient.getFileAncestors( lfnChunk, depth = 10 )
      if not res['OK']:
        gLogger.fatal( 'Error getting file ancestors', res['Message'] )
        DIRAC.exit( 2 )
      for lfn, anc in res['Value']['Successful'].items():
        ancestors[lfn] = sorted( [ancDict['FileName'] for ancDict in anc] )
        if not getFileType:
          listAncestors += ancestors[lfn]
    self.__write( '\n' )

    self.ancestors = ancestors.copy()
    self.commonAncestors = {}
    self.multipleDescendants = {}
    if not getFileType and len( listAncestors ) == len( set( listAncestors ) ):
      gLogger.info( 'Found %d ancestors, no common one' % len( listAncestors ) )
      return

    gLogger.info( 'Found files with %d file types' % len( fileTypes ) )
    for fileType in fileTypes:
      lfns = fileTypes[fileType] & set( ancestors )
      gLogger.info( 'File type %s, checking %d files' % ( fileType, len( lfns ) ) )
      listAncestors = []
      for lfn in lfns:
        listAncestors += ancestors[lfn]
      setAncestors = set( listAncestors )
      if len( listAncestors ) == len( setAncestors ):
        gLogger.info( 'Found %d ancestors for file type %s, no common one' % ( len( listAncestors ), fileType ) )
        continue
      # There are common ancestors
      descendants = {}
      # Reverse the list of ancestors
      for lfn in lfns:
        for anc in ancestors[lfn]:
          descendants.setdefault( anc, [] ).append( lfn )
      # Check if ancestor has more than one descendant
      for anc in sorted( descendants ):
        if len( descendants[anc] ) > 1:
          desc = sorted( descendants[anc] )
          gLogger.info( 'For ancestor %s, found %d descendants: %s' % ( anc, len( desc ), desc ) )
          self.multipleDescendants[anc] = desc
          self.commonAncestors.setdefault( ','.join( sorted( desc ) ), [] ).append( anc )

  ################################################################################

  def __getDaughtersInfo( self, lfns, status, filesWithDescendants, filesWithoutDescendants, filesWithMultipleDescendants ):
    """ Get BK information about daughers of lfns """
    chunkSize = 100 if self.transType == 'DataStripping' and len( self.fileType ) > 1 else 500
    self.__write( "Now getting daughters for %d %s mothers in production %d (chunks of %d) "
                  % ( len( lfns ), status, self.prod, chunkSize ) )
    startTime = time.time()
    daughtersBKInfo = {}
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
    return daughtersBKInfo

  def getDescendants( self, lfns, status = '' ):
    """ get the descendants of a list of LFN (for the production)
    """
    if type( lfns ) == type( '' ):
      lfns = [lfns]
    elif type( lfns ) == type( {} ):
      lfns = lfns.keys()
    filesWithDescendants = {}
    filesWithoutDescendants = {}
    filesWithMultipleDescendants = {}
    fileTypesExcluded = Operations().getValue( 'DataConsistency/IgnoreDescendantsOfType', [] )
    inFCNotInBK = []
    inBKNotInFC = []
    allDaughters = []
    removedFiles = []
    if not lfns:
      return filesWithDescendants, filesWithoutDescendants, filesWithMultipleDescendants, \
        allDaughters, inFCNotInBK, inBKNotInFC, removedFiles

    daughtersBKInfo = self.__getDaughtersInfo( lfns, status, filesWithDescendants, filesWithoutDescendants, filesWithMultipleDescendants )

    # This is the list of all daughters, sets will contain unique entries
    setAllDaughters = set( daughtersBKInfo )
    allDaughters = list( setAllDaughters )
    inBK = set( [lfn for lfn in setAllDaughters if daughtersBKInfo[lfn][0]] )
    setRealDaughters = set()
    # Now check whether these daughters files have replicas or have descendants that have replicas
    chunkSize = 100 if self.transType == 'DataStripping' and len( self.fileType ) > 1 else 500
    if filesWithDescendants:
      # First check in LFC the presence of daughters
      if not self.noLFC:
        present, notPresent = self.getReplicasPresenceFromDirectoryScan( allDaughters ) \
                                if len( allDaughters ) > 10 * chunkSize and \
                                   len( inBK ) < len( allDaughters ) / 2 else \
                              self.getReplicasPresence( allDaughters )
        setPresent = set( present )
        setNotPresent = set( notPresent )
      else:
        setPresent = inBK
        setNotPresent = setAllDaughters - inBK

      setRealDaughters = setPresent
      # Now check consistency between BK and FC for daughters
      inBKNotInFC = list( inBK & setNotPresent )
      inFCNotInBK = list( setPresent - inBK )

      # Now check whether the daughters without replica have a descendant
      if setNotPresent:
        chunkSize = 500
        startTime = time.time()
        self.__write( "Now checking descendants from %d daughters without replicas (chunks of %d) "
                      % ( len( setNotPresent ), chunkSize ) )
        # Get existing descendants of notPresent daughters
        setDaughtersWithDesc = set()
        for lfnChunk in breakListIntoChunks( list( setNotPresent ), chunkSize ):
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
        # print "%d not Present daughters, %d have a descendant" % ( len( notPresent ), len( setDaughtersWithDesc ) )

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
            # print 'Multiple descendants', filesWithMultipleDescendants.get( lfn )
          # Only interested in daughters without replica, so if all have one, skip

          # Some daughters may have a replica though, take them into account
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
    # print 'Final multiple descendants', filesWithMultipleDescendants

    # File files without descendants don't exist, not important
    if filesWithoutDescendants:
      present, removedFiles = self.getReplicasPresence( filesWithoutDescendants.keys() )
      filesWithoutDescendants = dict.fromkeys( present )
    else:
      removedFiles = []

    # Remove files with multiple descendants from files with descendants
    for lfn in filesWithMultipleDescendants:
      filesWithDescendants.pop( lfn, None )
    # For files in FC and not in BK, ignore if they are not active
    if inFCNotInBK:
      inFCNotInBK = self.getReplicasPresence( inFCNotInBK )[0]
    return filesWithDescendants, filesWithoutDescendants, filesWithMultipleDescendants, \
      list( setRealDaughters ), inFCNotInBK, inBKNotInFC, removedFiles

    ########################################################################


  def checkFC2BK( self, bkCheck = True ):
    """ check that files present in the FC are also in the BK
    """
    present, _notPresent = self.__getLFNsFromFC()
    if not self.lfns:
      prStr = ' are in the FC but'
    else:
      if not present:
        if bkCheck:
          gLogger.always( 'No files are in the FC, no check in the BK. Use dirac-dms-check-bkk2fc instead' )
        return
      prStr = ''

    if bkCheck:
      res = self._getBKMetadata( present )
      self.existLFNsNotInBK = res[0]
      self.existLFNsBKRepNo = res[1]
      self.existLFNsBKRepYes = res[2]
      msg = ''
      if self.transType:
        msg = "For prod %s of type %s, " % ( self.prod, self.transType )
      if self.existLFNsBKRepNo:
        gLogger.warn( "%s %d files%s have replica = NO in BK" % ( msg, len( self.existLFNsBKRepNo ),
                                                                 prStr ) )
      if self.existLFNsNotInBK:
        gLogger.warn( "%s %d files%s not in BK" % ( msg, len( self.existLFNsNotInBK ), prStr ) )
    else:
      self.existLFNsBKRepYes = present

    ########################################################################

  def __getDirectories( self ):
    """ get the directories where to look into (they are either given, or taken from the transformation ID
    """
#     if self.directories:
#       directories = []
#       printout = False
#       for directory in self.directories:
#         if not directory.endswith( '...' ):
#           directories.append( directory )
#         else:
#           printout = True
#           topDir = os.path.dirname( directory )
#           res = self.dm.getCatalogListDirectory( topDir )
#           if not res['OK']:
#             raise RuntimeError( res['Message'] )
#           else:
#             matchDir = directory.split( '...' )[0]
#             directories += [d for d in res['Value']['Successful'].get( topDir, {} ).get( 'SubDirs', [] ) if d.startswith( matchDir )]
#       if printout:
#         gLogger.always( 'Expanded list of %d directories:\n%s' % ( len( directories ), '\n'.join( directories ) ) )
#       return directories
    try:
      bkQuery = self.__getBKQuery()
    except ValueError, _e:
      pass
    if bkQuery and set( bkQuery.getQueryDict() ) - set( ['Visible', 'Production', 'FileType'] ):
      return bkQuery.getDirs()
    if self.prod:
      if bkQuery:
        fileType = bkQuery.getFileTypeList()
      else:
        fileType = []
      res = self.transClient.getTransformationParameters( self.prod, ['OutputDirectories'] )
      if not res['OK']:
        raise RuntimeError( res['Message'] )
      else:
        directories = []
        dirList = res['Value']
        if type( dirList ) == type( '' ) and dirList[0] == '[' and dirList[-1] == ']':
          dirList = ast.literal_eval( dirList )
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
            if dirName.endswith( '/0' ):
              dirName = dirName.replace( '/0', '/%08d' % int( self.prod ) )
            ftOK = True if not fileType else False
            for ft in fileType:
              if ft in dirName:
                ftOK = True
                break
            if ftOK:
              directories.append( dirName )
        return directories
    else:
      raise RuntimeError( "Need to specify either the directories or a production id" )

    ########################################################################

  def _getBKMetadata( self, lfns ):
    """ get metadata (i.e. replica flag) of a list of LFNs
    """
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
          gLogger.error( "\nCan't get the BK metadata, retry: ", res['Message'] )
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

  def checkBK2TS( self ):
    """ check that files present in the BK are also in the FC (re-check of BKWatchAgent)
    """
    bkQuery = self.__getBKQuery( fromTS = True )
    lfnsReplicaYes = self._getBKFiles( bkQuery )
    proc, nonProc, _statuses = self._getTSFiles()
    self.filesInBKNotInTS = list( set( lfnsReplicaYes ) - set( proc + nonProc ) )
    if self.filesInBKNotInTS:
      gLogger.error( "There are %d files in BK that are not in TS: %s" % ( len( self.filesInBKNotInTS ),
                                                                           str( self.filesInBKNotInTS ) ) )

  ################################################################################


  def checkFC2SE( self, bkCheck = True ):
    self.checkFC2BK( bkCheck = bkCheck )
    if self.existLFNsBKRepYes or self.existLFNsBKRepNo:
      repDict = self.compareChecksum( self.existLFNsBKRepYes + self.existLFNsBKRepNo.keys() )
      self.existLFNsNoSE = repDict['MissingPFN']
      self.existLFNsBadReplicas = repDict['SomeReplicasCorrupted']
      self.existLFNsBadFiles = repDict['AllReplicasCorrupted']

  def checkSE( self, seList ):
    lfnsReplicaNo, lfnsReplicaYes = self.__getLFNsFromBK()
    if not lfnsReplicaNo and not lfnsReplicaYes:
      lfns, notPresent = self.__getLFNsFromFC()
    else:
      lfns = lfnsReplicaYes
      notPresent = []
    gLogger.always( "Checking presence of %d files at %s" % ( len( lfns ), ', '.join( seList ) ) )
    replicaRes = self.dm.getReplicas( lfns )
    if not replicaRes['OK']:
      gLogger.error( 'Error getting replicas', replicaRes['Message'] )
      return
    seSet = set( seList )
    success = replicaRes['Value']['Successful']
    self.absentLFNsInFC = sorted( set( notPresent ) | set( replicaRes['Value']['Failed'] ) )
    self.existLFNsNoSE = [lfn for lfn in success if not seSet & set( success[lfn] ) ]

  def compareChecksum( self, lfns ):
    """compare the checksum of the file in the FC and the checksum of the physical replicas.
       Returns a dictionary containing 3 sub-dictionaries: one with files with missing PFN, one with
       files with all replicas corrupted, and one with files with some replicas corrupted and at least
       one good replica
    """
    retDict = {'AllReplicasCorrupted' : {}, 'SomeReplicasCorrupted': {}, 'MissingPFN':{}, 'NoReplicas':{}}

    chunkSize = 1000
    replicas = {}
    setLfns = set( lfns )
    cachedLfns = setLfns & set( self.cachedReplicas )
    for lfn in cachedLfns:
      replicas[lfn] = self.cachedReplicas[lfn]
    lfnsLeft = list( setLfns - cachedLfns )
    startTime = time.time()
    if lfnsLeft:
      self.__write( "Get replicas for %d files (chunks of %d): " % ( len( lfnsLeft ), chunkSize ) )
      for lfnChunk in breakListIntoChunks( lfnsLeft, chunkSize ):
        self.__write( '.' )
        replicasRes = self.dm.getReplicas( lfnChunk )
        if not replicasRes['OK']:
          gLogger.error( "error:  %s" % replicasRes['Message'] )
          raise RuntimeError( "error:  %s" % replicasRes['Message'] )
        replicasRes = replicasRes['Value']
        if replicasRes['Failed']:
          retDict['NoReplicas'].update( replicasRes['Failed'] )
        replicas.update( replicasRes['Successful'] )
      self.__write( ' (%.1f seconds)\n' % ( time.time() - startTime ) )

    self.__write( "Get FC metadata for %d files to be checked: " % len( lfns ) )
    metadata = {}
    for lfnChunk in breakListIntoChunks( replicas.keys(), chunkSize ):
      self.__write( '.' )
      res = self.fc.getFileMetadata( lfnChunk )
      if not res['OK']:
        raise RuntimeError( "error %s" % res['Message'] )
      metadata.update( res['Value']['Successful'] )
    self.__write( ' (%.1f seconds)\n' % ( time.time() - startTime ) )

    gLogger.always( "Check existence and compare checksum file by file..." )
    csDict = {}
    seFiles = {}
    startTime = time.time()
    # Reverse the LFN->SE dictionary
    for lfn in replicas:
      csDict.setdefault( lfn, {} )[ 'LFCChecksum' ] = metadata.get( lfn, {} ).get( 'Checksum' )
      for se in replicas[ lfn ]:
        seFiles.setdefault( se, [] ).append( lfn )

    checkSum = {}
    self.__write( 'Getting checksum of %d replicas in %d SEs (chunks of %d): ' % ( len( surlLfn ), len( seFiles ), chunkSize ) )
    lfnNotAvailable = {}
    logLevel = gLogger.getLevel()
    gLogger.setLevel( 'FATAL' )
    for num, se in enumerate( sorted( seFiles ) ):
      self.__write( '\n%d. At %s (%d files): ' % ( num, se, len( seFiles[se] ) ) )
      oSe = StorageElement( se )
      for lfnChunk in breakListIntoChunks( seFiles[se], chunkSize ):
        self.__write( '.' )
        metadata = oSe.getFileMetadata( lfnChunk )
        if not metadata['OK']:
          gLogger.error( "error StorageElement.getFileMetadata returns %s" % ( metadata['Message'] ) )
          raise RuntimeError( "error StorageElement.getFileMetadata returns %s" % ( metadata['Message'] ) )
        metadata = metadata['Value']
        for lfn in metadata['Failed']:
          gLogger.info( "LFN was not found at %s! %s " % ( se, lfn ) )
          lfnNotAvailable.setdefault( lfn, [] ).append( se )
        for lfn in metadata['Successful']:
          checkSum.setdefault( lfn, {} )[se] = metadata['Successful'][ lfn ]['Checksum']
    self.__write( ' (%.1f seconds)\n' % ( time.time() - startTime ) )
    gLogger.setLevel( logLevel )
    retDict[ 'MissingPFN'] = {}

    startTime = time.time()
    self.__write( 'Verifying checksum of %d files (chunks of %d) ' % ( len( replicas ), chunkSize ) )
    for num, lfn in enumerate( replicas ):
      # get the lfn checksum from the LFC
      if num % chunkSize == 0:
        self.__write( '.' )

      replicaDict = replicas[ lfn ]
      oneGoodReplica = False
      allGoodReplicas = True
      lfcChecksum = csDict[ lfn ].pop( 'LFCChecksum' )
      for se in replicaDict:
        # If replica doesn't exist skip check
        if se in lfnNotAvailable.get( lfn, [] ):
          allGoodReplicas = False
          continue
        surl = replicaDict[ se ]
        # get the surls metadata and compare the checksum
        surlChecksum = checkSum.get( lfn, {} ).get( se, '' )
        if not surlChecksum or not compareAdler( lfcChecksum , surlChecksum ):
          # if lfcChecksum does not match surlChecksum
          csDict[ lfn ][ se ] = {'SURL':surl, 'PFNChecksum': surlChecksum}
          gLogger.info( "ERROR!! checksum mismatch at %s for LFN %s:  LFC checksum: %s , PFN checksum : %s "
                        % ( se, lfn, lfcChecksum, surlChecksum ) )
          allGoodReplicas = False
        else:
          oneGoodReplica = True
      if not oneGoodReplica:
        if lfn in lfnNotAvailable:
          gLogger.info( "=> All replicas are missing" )
          retDict['MissingPFN'][ lfn] = 'All'
        else:
          gLogger.info( "=> All replicas have bad checksum" )
          retDict['AllReplicasCorrupted'][ lfn ] = csDict[ lfn ]
      elif not allGoodReplicas:
        if lfn in lfnNotAvailable:
          gLogger.info( "=> At least one replica missing" )
          retDict['MissingPFN'][lfn] = lfnNotAvailable[lfn]
        else:
          gLogger.info( "=> At least one replica with good Checksum" )
          retDict['SomeReplicasCorrupted'][ lfn ] = csDict[ lfn ]

    self.__write( ' (%.1f seconds)\n' % ( time.time() - startTime ) )
    return retDict

  ################################################################################
  # properties

  def set_prod( self, value ):
    """ Setter """
    if value:
      value = int( value )
      res = self.transClient.getTransformation( value, extraParams = False )
      if not res['OK']:
        raise RuntimeError( "Couldn't find transformation %d: %s" % ( value, res['Message'] ) )
      else:
        self.transType = res['Value']['Type']
      if self.interactive:
        gLogger.info( "Production %d has type %s" % ( value, self.transType ) )
    else:
      value = 0
    self._prod = value
  def get_prod( self ):
    """ Getter """
    return self._prod
  prod = property( get_prod, set_prod )

  def set_fileType( self, value ):
    """ Setter """
    fts = [ft.upper() for ft in value]
    self._fileType = fts
  def get_fileType( self ):
    """ Getter """
    return self._fileType
  fileType = property( get_fileType, set_fileType )

  def set_fileTypesExcluded( self, value ):
    """ Setter """
    fts = [ft.upper() for ft in value]
    self._fileTypesExcluded = fts
  def get_fileTypesExcluded( self ):
    """ Getter """
    return self._fileTypesExcluded
  fileTypesExcluded = property( get_fileTypesExcluded, set_fileTypesExcluded )

  def set_bkQuery( self, value ):
    """ Setter """
    if type( value ) == type( "" ):
      self._bkQuery = ast.literal_eval( value )
    else:
      self._bkQuery = value
  def get_bkQuery( self ):
    """ Getter """
    return self._bkQuery
  bkQuery = property( get_bkQuery, set_bkQuery )

  def set_lfns( self, value ):
    """ Setter """
    if type( value ) == type( "" ):
      value = [value]
    value = [v.replace( ' ', '' ).replace( '//', '/' ) for v in value]
    self._lfns = value
  def get_lfns( self ):
    """ Getter """
    return self._lfns
  lfns = property( get_lfns, set_lfns )
