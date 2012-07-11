########################################################################
# $HeadURL$
########################################################################

""" :mod: TransformationCleaningAgent 
    =================================
 
    .. module: TransformationCleaningAgent
    :synopsis: clean up of finalised transformations
"""

__RCSID__ = "$Id$"

## from DIRAC
from DIRAC import S_OK, S_ERROR, gConfig
from DIRAC.Core.Utilities.List import sortList
from DIRAC.TransformationSystem.Agent.TransformationCleaningAgent import TransformationCleaningAgent as DiracTCAgent
## from LHCbDIRAC
from LHCbDIRAC.TransformationSystem.Client.TransformationClient import TransformationClient
from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient

## agent's name
AGENT_NAME = 'Transformation/TransformationCleaningAgent'

class TransformationCleaningAgent( DiracTCAgent ):
  """
  .. class:: TransformationCleaningAgent

  """

  def __init__( self, agentName, baseAgentName = False, 	properties = dict() ):
    """ c'tor

    :param self: self reference
    :param str agentName: name of agent
    :param bool baseAgentName: whatever  
    :param dict properties: whatever else
    """
    DiracTCAgent.__init__( self, agentName, baseAgentName, properties )
    ## LHCb bookkeeping client
    self.bkClient = BookkeepingClient()
    ## LHCb transformation client 
    self.transClient = TransformationClient()

  #############################################################################
  def initialize( self ):
    """ initialize 

    :param self: self reference
    """
    ## shifter proxy
    self.am_setOption( "shifterProxy", "DataManager" )
    self.transformationTypes = sortList( self.am_getOption( 'TransformationTypes', [ 'MCSimulation',
                                                                                     'DataReconstruction',
                                                                                     'DataStripping',
                                                                                     'MCStripping',
                                                                                     'Merge',
                                                                                     'Replication'] ) )
    self.log.info( "Will consider the following transformation types: %s" % str( self.transformationTypes ) )
    self.directoryLocations = sortList( self.am_getOption( 'DirectoryLocations', [ 'TransformationDB',
                                                                                   'StorageUsage' ] ) )
    self.log.info( "Will search for directories in the following locations: %s" % str( self.directoryLocations ) )
    self.archiveAfter = self.am_getOption( 'ArchiveAfter', 7 ) # days
    self.log.info( "Will archive Completed transformations after %d days" % self.archiveAfter )
    storageElements = gConfig.getValue( '/Resources/StorageElementGroups/Tier1_MC_M-DST', [] )
    storageElements += ['CNAF_MC-DST', 'CNAF-RAW']
    ## what about RSS???
    self.activeStorages = sortList( self.am_getOption( 'ActiveSEs', storageElements ) )
    self.log.info( "Will check the following storage elements: %s" % str( self.activeStorages ) )
    self.logSE = self.am_getOption( 'TransformationLogSE', 'LogSE' )
    self.log.info( "Will remove logs found on storage element: %s" % self.logSE )
    return S_OK()

  def cleanMetadataCatalogFiles( self, transID, directories ):
    bkFlag = 'Yes'
    if not directories:
      bkFlag = ''
    res = self.bkClient.getProductionFiles( transID, 'ALL', bkFlag )
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
