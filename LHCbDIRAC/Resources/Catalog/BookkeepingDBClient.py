""" Client for BookkeepingDB file catalog
"""

__RCSID__ = "$Id$"

from DIRAC                                                          import gLogger, S_OK
from DIRAC.Core.DISET.RPCClient                                     import RPCClient
from DIRAC.Core.Utilities.List                                      import breakListIntoChunks
from DIRAC.Resources.Catalog.FileCatalogClientBase                  import FileCatalogClientBase
from DIRAC.Resources.Catalog.Utilities                              import checkCatalogArguments

import types





class BookkeepingDBClient( FileCatalogClientBase ):
  """ File catalog client for bookkeeping DB
  """

  READ_METHODS = FileCatalogClientBase.READ_METHODS + ['isDirectory', 'isLink', 'getFileSize', 'getFileMetadata' ]
  WRITE_METHODS = FileCatalogClientBase.WRITE_METHODS + [ 'addFile', 'addReplica', 'removeFile', 'removeReplica', 'setReplicaStatus',
                  'setReplicaProblematic', 'setReplicaHost', 'removeDirectory', 'createDirectory',
                  'removeLink', 'createLink' ]

  def __init__( self, url = False, **kwargs ):
    """ Constructor of the Bookkeeping catalogue client
    """
    self.splitSize = 1000
    self.name = 'BookkeepingDB'
    self.valid = True
    self.server = None
    self.url = None
    try:
      self.server = self.__getServer( url = url )
      gLogger.verbose( "BK catalog URLs: %s" % self.url )
    except Exception, exceptionMessage:
      gLogger.exception( 'BookkeepingDBClient.__init__: Exception while obtaining Bookkeeping service URL.', '', exceptionMessage )
      self.valid = False

  def __getServer( self, url = False ):
    if not self.server:
      if url:
        self.url = url
      if not self.url:
        self.url = 'Bookkeeping/BookkeepingManager'
      self.server = RPCClient( self.url, timeout = 120 )
    return self.server

  def isOK( self ):
    ''' Returns valid
    '''
    return self.valid

  @checkCatalogArguments
  def addFile( self, lfn ):
    """ Set the replica flag
    """
    return self.__setHasReplicaFlag( lfn )

  @checkCatalogArguments
  def addReplica( self, lfn ):
    """ Same as addFile
    """
    return self.addFile( lfn )

  @checkCatalogArguments
  def removeFile( self, path ):
    """ Remove the replica flag
    """
    return self.__unsetHasReplicaFlag( path )

  @checkCatalogArguments
  def isFile( self, lfn ):
    """ Returns a dictionary True/False
    """
    return self.__exists( lfn )

  @checkCatalogArguments
  def isDirectory( self, lfn ):
    """ Return Successful dict: True if lfn is a directory, False if a file - Failed dict if not existing
    """
    res = self.isFile( lfn )
    if not res['OK']:
      successful = {}
      failed = dict.fromkeys( lfn, res['Message'] )
    else:
      successful = dict.fromkeys( [lfn for lfn, val in res['Value']['Successful'].items() if val], False )
      failed = res['Value']['Failed']
      toCheck = [lfn for lfn, val in res['Value']['Successful'].items() if not val]
      if toCheck:
        # Can't use service directly as
        res = self.__getServer().getDirectoryMetadata_new( toCheck )
        if not res['OK']:
          failed.update( dict.fromkeys( toCheck, res['Message'] ) )
        else:
          failed.update( dict.fromkeys( [lfn for lfn in res['Value']['Failed']], 'Not a file or directory' ) )
          successful.update( dict.fromkeys( [lfn for lfn in res['Value']['Successful']], True ) )
    return S_OK( {'Successful':successful, 'Failed':failed} )


  def isLink( self, lfn ):
    return self.__returnSuccess( lfn, val = False )

  @checkCatalogArguments
  def __returnSuccess( self, lfn, val = True ):
    """ Generic method returning success for all input files"""
    return S_OK( {'Failed':{}, 'Successful':dict.fromkeys( lfn, val )} )

  def removeReplica( self, lfn ):
    return self.__returnSuccess( lfn )

  def setReplicaStatus( self, lfn ):
    return self.__returnSuccess( lfn )

  def setReplicaProblematic( self, lfn, revert = False ):
    return self.__returnSuccess( lfn )

  def setReplicaHost( self, lfn ):
    return self.__returnSuccess( lfn )

  def removeDirectory( self, lfn, recursive = False ):
    return self.__returnSuccess( lfn )

  def createDirectory( self, lfn ):
    return self.__returnSuccess( lfn )

  def removeLink( self, lfn ):
    return self.__returnSuccess( lfn )

  def createLink( self, lfn ):
    return self.__returnSuccess( lfn )

  @checkCatalogArguments
  def exists( self, path ):
    """ Returns a dictionary of True/False on file existence
    """
    return self.__exists( path )

  @checkCatalogArguments
  def getFileMetadata( self, path ):
    """ Return the metadata dictionary
    """
    return self.__getFileMetadata( path )

  @checkCatalogArguments
  def getFileSize( self, path ):
    """ Return just the file size
    """

    res = self.__getFileMetadata( path )
    # Always returns OK
    successful = dict( [( lfn, metadata['FileSize'] ) for lfn, metadata in res['Value']['Successful'].items()] )
    return S_OK( {'Successful':successful, 'Failed':res['Value']['Failed']} )

  ################################################################
  #
  # These are the internal methods used for actual interaction with the BK service
  #


  def __toggleReplicaFlag( self, lfns, setflag = True ):
    successful = {}
    # Poor man's way to not return an error for user files
    for lfn in  [lfn for lfn in lfns if lfn.startswith( '/lhcb/user' )]:
      lfns.remove( lfn )
      successful[lfn] = True
    failed = {}
    for lfnList in breakListIntoChunks( lfns, self.splitSize ):
      res = {True: self.__getServer().addFiles, False:self.__getServer().removeFiles}[setflag]( lfnList )
      if not res['OK']:
        failed.update( dict.fromkeys( lfnList, res['Message'] ) )
      else:
        # It is a dirty, but ...
        failed.update( dict.fromkeys( [lfn for lfn in res['Value']['Failed']], 'File does not exist' ) )
        successful.update( dict.fromkeys( [lfn for lfn in res['Value']['Successful']], True ) )
    return S_OK( {'Successful':successful, 'Failed':failed} )

  def __setHasReplicaFlag( self, lfns ):
    '''
      Set replica flags on BKK
    '''
    return self.__toggleReplicaFlag( lfns, setflag = True )

  def __unsetHasReplicaFlag( self, lfns ):
    '''
      Removes replica flags on BKK
    '''
    return self.__toggleReplicaFlag( lfns, setflag = False )

  def __exists( self, lfns ):
    '''
      Checks if lfns exist
    '''
    successful = {}
    failed = {}
    for lfnList in breakListIntoChunks( lfns, self.splitSize ):
      res = self.__getServer().exists( lfnList )
      if not res['OK']:
        failed.update( dict.fromkeys( lfnList, res['Message'] ) )
      else:
        successful.update( res['Value'] )
    return S_OK( {'Successful':successful, 'Failed':failed} )

  @checkCatalogArguments
  def __getFileMetadata( self, lfns ):
    '''
      Returns lfns metadata
    '''
    successful = {}
    failed = {}
    for lfnList in breakListIntoChunks( lfns, self.splitSize ):
      res = self.__getServer().getFileMetadata( lfnList )
      if not res['OK']:
        failed.update( dict.fromkeys( lfnList, res['Message'] ) )
      else:
        success = res['Value'].get( 'Successful', res['Value'] )
        failed.update( dict.fromkeys( [lfn for lfn in lfnList if lfn not in success], 'File does not exist' ) )
        failed.update( dict( [( lfn, val ) for lfn, val in success.items() if type( val ) in types.StringTypes ] ) )
        successful.update( dict( [( lfn, val ) for lfn, val in success.items() if type( val ) not in types.StringTypes ] ) )
    return S_OK( {'Successful':successful, 'Failed':failed} )
################################################################################
# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
