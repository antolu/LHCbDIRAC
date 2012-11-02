""" Some utilities for BK and Catalog(s) interactions
"""

import os

from DIRAC import gLogger
from DIRAC.Core.Utilities.List import breakListIntoChunks
from DIRAC.Interfaces.API.Dirac import Dirac
from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager

from LHCbDIRAC.BookkeepingSystem.Client.BKQuery import BKQuery
from LHCbDIRAC.TransformationSystem.Client.TransformationClient import TransformationClient

#FIXME: this is quite dirty, what should be checked is exactly what it is done
prodsWithMerge = ( 'MCSimulation', 'DataStripping', 'DataSwimming', 'WGProduction' )

class ConsistencyChecks( object ):
  """ A class for handling some consistency check
  """

  def __init__( self, prod = 0, transClient = None, rm = None ):
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

    self.dirac = Dirac()

    self.prod = prod

    self.lfnsReplicaYes = []
    self.lfnsReplicaNo = []

    self.existingLFNsThatAreNotInBKK = []
    self.nonExistingLFNsThatAreNotInBKK = []
    self.existingLFNsThatAreInBKK = []
    self.nonExistingLFNsThatAreInBKK = []

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
    gLogger.info( "Checking LFC files from %d directories" % len( dirs ) )
    gLogger.setLevel( 'FATAL' )
    res = self.rm.getFilesFromDirectory( dirs )
    gLogger.setLevel( 'VERBOSE' )
    if not res['OK']:
      gLogger.info( "Error getting files from directories %s:" % dirs, res['Message'] )
      return
    if res['Value']:
      filesFound = res['Value']
    else:
      filesFound = []

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

