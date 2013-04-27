""" Client for BookkeepingDB file catalog
"""

__RCSID__ = "$Id$"

from DIRAC                                                          import gLogger, S_OK, S_ERROR
from DIRAC.Core.DISET.RPCClient                                     import RPCClient
from DIRAC.Core.Utilities.List                                      import breakListIntoChunks
from DIRAC.Resources.Catalog.FileCatalogueBase                      import FileCatalogueBase
from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient           import BookkeepingClient
import types

class BookkeepingDBClient( FileCatalogueBase ):
  """ File catalog client for bookkeeping DB
  """
  def __init__( self, url = False ):
    """ Constructor of the Bookkeeping catalogue client
    """
    self.splitSize = 1000
    self.name = 'BookkeepingDB'
    self.valid = True
    try:
      if url:
        self.url = url
      else:
        self.url = 'Bookkeeping/BookkeepingManager'
      gLogger.verbose( "BK catalog URLs: %s" % self.url )
      server = RPCClient( self.url, timeout = 120 )
      self.bkClient = BookkeepingClient( server )
    except Exception, exceptionMessage:
      gLogger.exception( 'BookkeepingDBClient.__init__: Exception while obtaining Bookkeeping service URL.', '', exceptionMessage )
      self.valid = False

  def isOK( self ):
    '''
      Returns valid
    '''
    return self.valid

  def addFile( self, lfn ):
    """ Set the replica flag
    """
    res = self.__checkArgumentFormat( lfn )
    if not res['OK']:
      return res
    return self.__setHasReplicaFlag( res['Value'] )

  def addReplica( self, lfn ):
    """ Same as addFile
    """
    return self.addFile( lfn )

  def removeFile( self, path ):
    """ Remove teh replica flag
    """
    res = self.__checkArgumentFormat( path )
    if not res['OK']:
      return res
    return self.__unsetHasReplicaFlag( res['Value'] )

  def isFile( self, lfn ):
    return self.exists( lfn )

  def isDirectory( self, lfn ):
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
        res = self.bkClient.getDirectoryMetadata( toCheck )
        failed.update( dict.fromkeys( [lfn for lfn in res['Value']['Failed']], 'Not a file or directory' ) )
        successful.update( dict.fromkeys( [lfn for lfn in res['Value']['Successful']], True ) )
    return S_OK( {'Successful':successful, 'Failed':failed} )

  def isLink( self, lfn ):
    return self.__returnSuccess( lfn, val = False )

  def __returnSuccess( self, lfn, val = True ):
    """ Generic method returning success for all input files"""
    res = self.__checkArgumentFormat( lfn )
    if not res['OK']:
      return res
    return S_OK( {'Failed':{}, 'Successful':dict.fromkeys( res['Value'], val )} )

  def removeReplica( self, lfn ):
    return self.__returnSuccess( lfn )

  def setReplicaStatus( self, lfn ):
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

  def exists( self, path ):
    """ Returns a dictionary of True/False on file existence
    """
    res = self.__checkArgumentFormat( path )
    if not res['OK']:
      return res
    return self.__exists( res['Value'] )

  def getFileMetadata( self, path ):
    """ Return the metadata dictionary
    """
    res = self.__checkArgumentFormat( path )
    if not res['OK']:
      return res
    return self.__getFileMetadata( res['Value'] )

  def getFileSize( self, path ):
    """ Return just the file size
    """
    res = self.__checkArgumentFormat( path )
    if not res['OK']:
      return res
    res = self.__getFileMetadata( res['Value'] )
    # Always returns OK
    successful = dict( [( lfn, metadata['FileSize'] ) for lfn, metadata in res['Value']['Successful'].items()] )
    return S_OK( {'Successful':successful, 'Failed':res['Value']['Failed']} )

  ################################################################
  #
  # These are the internal methods used for actual interaction with the BK service
  #

  @staticmethod
  def __checkArgumentFormat( path ):
    '''
      Returns a list, either from a string or keys of a dict
    '''
    if type( path ) in types.StringTypes:
      return S_OK( [path] )
    elif type( path ) == types.ListType:
      return S_OK( path )
    elif type( path ) == types.DictType:
      return S_OK( path.keys() )
    else:
      errStr = "BookkeepingDBClient.__checkArgumentFormat: Supplied path is not of the correct format."
      gLogger.error( errStr )
      return S_ERROR( errStr )

  def __toggleReplicaFlag( self, lfns, setflag = True ):

    gLogger.verbose( "**** Set replica flag on %s" % self.url )
    successful = {}
    failed = {}
    for lfnList in breakListIntoChunks( lfns, self.splitSize ):
      res = {True: self.bkClient.addFiles, False:self.bkClient.removeFiles}[setflag]( lfnList )
      if not res['OK']:
        failed.update( dict.fromkeys( lfnList, res['Message'] ) )
      else:
        #It is a dirty, but ...
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
      res = self.bkClient.exists( lfnList )
      if not res['OK']:
        failed.update( dict.fromkeys( lfnList, res['Message'] ) )
      else:
        successful.update( res['Value'] )
    return S_OK( {'Successful':successful, 'Failed':failed} )

  def __getFileMetadata( self, lfns ):
    '''
      Returns lfns metadata
    '''

    successful = {}
    failed = {}
    for lfnList in breakListIntoChunks( lfns, self.splitSize ):
      res = self.bkClient.getFileMetadata( lfnList )
      if not res['OK']:
        failed.update( dict.fromkeys( lfnList, res['Message'] ) )
      else:
        success = res['Value'].get( 'Successful', res['Value'] )
        failed.update( dict.fromkeys( [lfn for lfn in lfnList if lfn not in success], 'File does not exist' ) )
        failed.update( dict( [( lfn, val ) for lfn, val in success.items() if type( val ) in types.StringTypes ] ) )
        successful.update( dict( [( lfn, val ) for lfn, val in success.items() if type( val ) not in types.StringTypes ] ) )
    return S_OK( {'Successful':successful, 'Failed':failed} )
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
