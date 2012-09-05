""" Some utilities for BK and Catalog(s) interactions
"""

import os
from DIRAC import gLogger
from DIRAC.Core.Utilities.List import breakListIntoChunks

from LHCbDIRAC.BookkeepingSystem.Client.BKQuery import BKQuery

#FIXME: this is quite dirty, what should be checked is exactly what it is done
prodsWithMerge = ( 'MCSimulation', 'DataStripping', 'DataSwimming', 'WGProduction' )

class consistencyChecks( object ):
  """ A class for handling some consistency check
  """

  def __init__( self, prod = 0, transClient = None, rm = None ):
    """ c'tor

        One object for every production
    """

    if transClient is None:
      from LHCbDIRAC.TransformationSystem.Client.TransformationClient import TransformationClient
      self.transClient = TransformationClient()
    else:
      self.transClient = transClient

    if rm is None:
      from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager
      self.rm = ReplicaManager()
    else:
      self.rm = rm

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
