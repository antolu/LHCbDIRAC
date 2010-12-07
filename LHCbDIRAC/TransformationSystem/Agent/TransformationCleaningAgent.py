########################################################################
# $HeadURL$
########################################################################
__RCSID__ = "$Id$"

from DIRAC                                                          import S_OK, S_ERROR, gConfig, gMonitor, gLogger, rootPath
from DIRAC.Core.Utilities.List                                      import sortList, breakListIntoChunks
from DIRAC.DataManagementSystem.Client.ReplicaManager               import ReplicaManager
from DIRAC.RequestManagementSystem.Client.RequestClient             import RequestClient
from DIRAC.Resources.Catalog.FileCatalogClient                      import FileCatalogClient
from DIRAC.TransformationSystem.Agent.TransformationCleaningAgent   import TransformationCleaningAgent as DIRACTransformationCleaningAgent
from DIRAC.WorkloadManagementSystem.Client.WMSClient                import WMSClient
from LHCbDIRAC.TransformationSystem.Client.TransformationClient     import TransformationClient
from LHCbDIRAC.NewBookkeepingSystem.Client.BookkeepingClient           import BookkeepingClient
from datetime                                                       import datetime, timedelta
import re, os

AGENT_NAME = 'Transformation/TransformationCleaningAgent'

class TransformationCleaningAgent( DIRACTransformationCleaningAgent ):

  #############################################################################
  def initialize( self ):
    """Sets defaults """
    self.storageUsageClient = StorageUsageClient()
    self.replicaManager = ReplicaManager()
    self.transClient = TransformationClient()
    self.wmsClient = WMSClient()
    self.requestClient = RequestClient()
    self.bkClient = BookkeepingClient()

    # This sets the Default Proxy to used as that defined under 
    # /Operations/Shifter/DataManager
    # the shifterProxy option in the Configuration can be used to change this default.
    self.am_setOption( 'shifterProxy', 'DataManager' )

    self.transformationTypes = sortList( self.am_getOption( 'TransformationTypes', ['MCSimulation', 'DataReconstruction', 'DataStripping', 'MCStripping', 'Merge', 'Replication'] ) )
    gLogger.info( "Will consider the following transformation types: %s" % str( self.transformationTypes ) )
    self.directoryLocations = sortList( self.am_getOption( 'DirectoryLocations', ['TransformationDB', 'StorageUsage'] ) )
    gLogger.info( "Will search for directories in the following locations: %s" % str( self.directoryLocations ) )
    self.archiveAfter = self.am_getOption( 'ArchiveAfter', 7 ) # days
    gLogger.info( "Will archive Completed transformations after %d days" % self.archiveAfter )
    storageElements = gConfig.getValue( '/Resources/StorageElementGroups/Tier1_MC_M-DST', [] )
    storageElements.extend( ['CNAF_MC-DST', 'CNAF-RAW'] )
    self.activeStorages = sortList( self.am_getOption( 'ActiveSEs', storageElements ) )
    gLogger.info( "Will check the following storage elements: %s" % str( self.activeStorages ) )
    self.logSE = self.am_getOption( 'TransformationLogSE', 'LogSE' )
    gLogger.info( "Will remove logs found on storage element: %s" % self.logSE )
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
    gLogger.info( "Found a total of %d files in the BK for transformation %d" % ( len( bkMetadata ), transID ) )
    for lfn, metadata in bkMetadata.items():
      if metadata['FileType'] != 'LOG':
        fileToRemove.append( lfn )
        if metadata['GotReplica'] == 'Yes':
          yesReplica.append( lfn )
    gLogger.info( "Attempting to remove %d possible remnants from the catalog and storage" % len( fileToRemove ) )
    res = self.replicaManager.removeFile( fileToRemove )
    if not res['OK']:
      return res
    for lfn, reason in res['Value']['Failed'].items():
      gLogger.error( "Failed to remove file found in BK", "%s %s" % ( lfn, reason ) )
    if res['Value']['Failed']:
      return S_ERROR( "Failed to remove all files found in the BK" )
    if yesReplica:
      gLogger.info( "Ensuring that %d files are removed from the BK" % ( len( yesReplica ) ) )
      res = self.replicaManager.removeCatalogFile( yesReplica, catalogs = ['BookkeepingDB'] )
      if not res['OK']:
        return res
      for lfn, reason in res['Value']['Failed'].items():
        gLogger.error( "Failed to remove file from BK", "%s %s" % ( lfn, reason ) )
      if res['Value']['Failed']:
        return S_ERROR( "Failed to remove all files from the BK" )
    gLogger.info( "Successfully removed all files found in the BK" )
    return S_OK()
