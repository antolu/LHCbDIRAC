########################################################################
# $HeadURL$
########################################################################
__RCSID__ = "$Id$"

from DIRAC                                                          import S_OK, S_ERROR, gLogger
from DIRAC.Core.Utilities.List                                      import sortList
from DIRAC.DataManagementSystem.Client.ReplicaManager               import ReplicaManager
from DIRAC.DataManagementSystem.Client.StorageUsageClient           import StorageUsageClient
from DIRAC.TransformationSystem.Agent.ValidateOutputDataAgent       import ValidateOutputDataAgent as DIRACValidateOutputDataAgent
from LHCbDIRAC.DataManagementSystem.Client.DataIntegrityClient      import DataIntegrityClient
from LHCbDIRAC.TransformationSystem.Client.TransformationClient     import TransformationClient

AGENT_NAME = 'Transformation/ValidateOutputDataAgent'

class ValidateOutputDataAgent( DIRACValidateOutputDataAgent ):

  #############################################################################

  def initialize( self ):
    """Sets defaults
    """
    self.integrityClient = DataIntegrityClient()
    self.replicaManager = ReplicaManager()
    self.transClient = TransformationClient()
    self.storageUsageClient = StorageUsageClient()

    # This sets the Default Proxy
    # the shifterProxy option in the Configuration can be used to change this default.
    self.am_setOption( 'shifterProxy', 'DataManager' )

    self.transformationTypes = sortList( self.am_getOption( 'TransformationTypes', ['MCSimulation', 'DataReconstruction', 'DataStripping', 'MCStripping', 'Merge'] ) )
    gLogger.info( "Will treat the following transformation types: %s" % str( self.transformationTypes ) )
    self.directoryLocations = sortList( self.am_getOption( 'DirectoryLocations', ['TransformationDB', 'StorageUsage'] ) )
    gLogger.info( "Will search for directories in the following locations: %s" % str( self.directoryLocations ) )
    return S_OK()

  #############################################################################

  def checkTransformationIntegrity( self, prodID ):
    """ This method contains the real work
    """
    gLogger.info( "-" * 40 )
    gLogger.info( "Checking the integrity of production %s" % prodID )
    gLogger.info( "-" * 40 )

    res = self.getTransformationDirectories( prodID )
    if not res['OK']:
      return res
    directories = res['Value']

    ######################################################
    #
    # This check performs BK->Catalog->SE
    #
    res = self.integrityClient.productionToCatalog( prodID )
    if not res['OK']:
      gLogger.error( res['Message'] )
      return res
    bk2catalogMetadata = res['Value']['CatalogMetadata']
    bk2catalogReplicas = res['Value']['CatalogReplicas']
    res = self.integrityClient.checkPhysicalFiles( bk2catalogReplicas, bk2catalogMetadata )
    if not res['OK']:
      gLogger.error( res['Message'] )
      return res

    if not directories:
      return S_OK()

    ######################################################
    #
    # This check performs Catalog->BK and Catalog->SE for possible output directories
    #
    res = self.replicaManager.getCatalogExists( directories )
    if not res['OK']:
      gLogger.error( res['Message'] )
      return res
    for directory, error in res['Value']['Failed']:
      gLogger.error( 'Failed to determine existance of directory', '%s %s' % ( directory, error ) )
    if res['Value']['Failed']:
      return S_ERROR( "Failed to determine the existance of directories" )
    directoryExists = res['Value']['Successful']
    for directory in sortList( directoryExists.keys() ):
      if not directoryExists[directory]:
        continue
      iRes = self.integrityClient.catalogDirectoryToBK( directory )
      if not iRes['OK']:
        gLogger.error( iRes['Message'] )
        return iRes
      catalogDirMetadata = iRes['Value']['CatalogMetadata']
      catalogDirReplicas = iRes['Value']['CatalogReplicas']
      catalogMetadata = {}
      catalogReplicas = {}
      for lfn in catalogDirMetadata.keys():
        if not lfn in bk2catalogMetadata.keys():
          catalogMetadata[lfn] = catalogDirMetadata[lfn]
          if catalogDirReplicas.has_key( lfn ):
            catalogReplicas[lfn] = catalogDirReplicas[lfn]
      if not catalogMetadata:
        continue
      res = self.integrityClient.checkPhysicalFiles( catalogReplicas, catalogMetadata )
      if not res['OK']:
        gLogger.error( res['Message'] )
        return res

    return S_OK()
