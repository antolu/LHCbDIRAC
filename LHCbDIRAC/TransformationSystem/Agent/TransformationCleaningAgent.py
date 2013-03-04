""" :mod: TransformationCleaningAgent
    =================================

    .. module: TransformationCleaningAgent
    :synopsis: clean up of finalised transformations
"""

__RCSID__ = "$Id$"

# # from DIRAC
from DIRAC import S_OK, S_ERROR, gConfig
from DIRAC.Core.Utilities.List import sortList
from DIRAC.TransformationSystem.Agent.TransformationCleaningAgent import TransformationCleaningAgent as DiracTCAgent
# # from LHCbDIRAC
from LHCbDIRAC.TransformationSystem.Client.TransformationClient import TransformationClient
from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
from LHCbDIRAC.DataManagementSystem.Client.StorageUsageClient import StorageUsageClient

# # agent's name
AGENT_NAME = 'Transformation/TransformationCleaningAgent'

class TransformationCleaningAgent( DiracTCAgent ):
  """
  .. class:: TransformationCleaningAgent

  """

  def __init__( self, *args, **kwargs ):
    """ c'tor
    """
    DiracTCAgent.__init__( self, *args, **kwargs )
    self.bkClient = BookkeepingClient()
    self.transClient = TransformationClient()
    self.storageUsageClient = StorageUsageClient()

    self.directoryLocations = sortList( self.am_getOption( 'DirectoryLocations', [ 'TransformationDB',
                                                                                   'StorageUsage' ] ) )
    self.archiveAfter = self.am_getOption( 'ArchiveAfter', 7 )  # days

  #############################################################################

  def initialize( self ):
    """ initialize

    :param self: self reference
    """
    # # shifter proxy
    self.am_setOption( 'shifterProxy', 'DataManager' )
    self.log.info( "Will consider the following transformation types: %s" % str( self.transformationTypes ) )

    self.log.info( "Will search for directories in the following locations: %s" % str( self.directoryLocations ) )
    self.log.info( "Will archive Completed transformations after %d days" % self.archiveAfter )
    storageElements = gConfig.getValue( '/Resources/StorageElementGroups/Tier1_MC_M-DST', [] )
    storageElements += ['CNAF_MC-DST', 'CNAF-RAW']
    # # what about RSS???
    self.activeStorages = sortList( self.am_getOption( 'ActiveSEs', storageElements ) )
    self.log.info( "Will check the following storage elements: %s" % str( self.activeStorages ) )
    self.logSE = self.am_getOption( 'TransformationLogSE', 'LogSE' )
    self.log.info( "Will remove logs found on storage element: %s" % self.logSE )
    return S_OK()

  def cleanMetadataCatalogFiles( self, transID ):
    ''' clean the metadata using BKK and Replica Manager. Replace the one from base class
    '''
    res = self.bkClient.getProductionFiles( transID, 'ALL', 'Yes' )
    if not res['OK']:
      return res
    bkMetadata = res['Value']
    fileToRemove = []
    yesReplica = []
    self.log.info( "Found a total of %d files in the BK for transformation %d" % ( len( bkMetadata ), transID ) )
    for lfn, metadata in bkMetadata.items():
      if metadata['FileType'] != 'LOG':
        fileToRemove.append( lfn )
        if metadata['GotReplica'] == 'Yes':
          yesReplica.append( lfn )
    self.log.info( "Attempting to remove %d possible remnants from the catalog and storage" % len( fileToRemove ) )
    res = self.replicaManager.removeFile( fileToRemove )
    if not res['OK']:
      return res
    for lfn, reason in res['Value']['Failed'].items():
      self.log.error( "Failed to remove file found in BK", "%s %s" % ( lfn, reason ) )
    if res['Value']['Failed']:
      return S_ERROR( "Failed to remove all files found in the BK" )
    if yesReplica:
      self.log.info( "Ensuring that %d files are removed from the BK" % ( len( yesReplica ) ) )
      res = self.replicaManager.removeCatalogFile( yesReplica, catalogs = ['BookkeepingDB'] )
      if not res['OK']:
        return res
      for lfn, reason in res['Value']['Failed'].items():
        self.log.error( "Failed to remove file from BK", "%s %s" % ( lfn, reason ) )
      if res['Value']['Failed']:
        return S_ERROR( "Failed to remove all files from the BK" )
    self.log.info( "Successfully removed all files found in the BK" )
    return S_OK()

  def getTransformationDirectories( self, transID ):
    """ get the directories for the supplied transformation from the transformation system

    :param self: self reference
    :param int transID: transformation ID
    """

    res = DiracTCAgent.getTransformationDirectories( self, transID )

    if res['OK']:
      directories = res['Value']
    else:
      return res

    if 'StorageUsage' in self.directoryLocations:
      res = self.storageUsageClient.getStorageDirectories( '', '', transID, [] )
      if not res['OK']:
        self.log.error( "Failed to obtain storage usage directories", res['Message'] )
        return res
      transDirectories = res['Value']
      directories = self._addDirs( transID, transDirectories, directories )

    if not directories:
      self.log.info( "No output directories found" )
    directories = sortList( directories )
    return S_OK( directories )
