""" Client for BookkeepingDB file catalog
"""

__RCSID__ = "$Id$"

from DIRAC                                                          import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.ConfigurationSystem.Client                               import PathFinder
from DIRAC.Core.DISET.RPCClient                                     import RPCClient
from DIRAC.Core.Utilities.List                                      import breakListIntoChunks, randomize
from DIRAC.Resources.Catalog.FileCatalogueBase                      import FileCatalogueBase
import types, os

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
      if not url:
        self.url = 'Bookkeeping/BookkeepingManager'
      else:
        self.url = url
      gLogger.verbose( "BK catalog URLs: %s" % self.url )
    except Exception, exceptionMessage:
      gLogger.exception( 'BookkeepingDBClient.__init__: Exception while obtaining Bookkeeping service URL.', '', exceptionMessage )
      self.valid = False

  def isOK( self ):
    '''
      Returns valid
    '''
    return self.valid

  def addFile( self, lfn ):
    res = self.__checkArgumentFormat( lfn )
    if not res['OK']:
      return res
    fileList = []
    for lfn, _info in res['Value'].items():
      fileList.append( lfn )
    return self.__setHasReplicaFlag( fileList )

  def addReplica( self, lfn ):
    res = self.__checkArgumentFormat( lfn )
    if not res['OK']:
      return res
    fileList = []
    for lfn, _info in res['Value'].items():
      fileList.append( lfn )
    return self.__setHasReplicaFlag( fileList )

  def removeFile( self, path ):
    #FIXME: who can be such short method soooo messy ???
    res = self.__checkArgumentFormat( path )
    if not res['OK']:
      return res
    lfns = res['Value'].keys()
    res = self.__exists( lfns )
    if not res['OK']:
      return res
    lfnsToRemove = []
    successful = {}
    for lfn, exists in res['Value']['Successful'].items():
      if exists:
        lfnsToRemove.append( lfn )
      else:
        successful[lfn] = True
    res = self.__unsetHasReplicaFlag( lfnsToRemove )
    failed = res['Value']['Failed']
    successful.update( res['Value']['Successful'] )
    resDict = {'Failed':failed, 'Successful':successful}
    return S_OK( resDict )

  def removeReplica( self, lfn ):
    res = self.__checkArgumentFormat( lfn )
    if not res['OK']:
      return res
    successful = {}
    for lfn, _info in res['Value'].items():
      successful[lfn] = True
    resDict = {'Failed':{}, 'Successful':successful}
    return S_OK( resDict )

  def setReplicaStatus( self, lfn ):
    #FIXME: the same method as removeReplica, setReplicaHost, removeDirectory
    #createDirectory, removeLink... ??
    res = self.__checkArgumentFormat( lfn )
    if not res['OK']:
      return res
    successful = {}
    for lfn, _info in res['Value'].items():
      successful[lfn] = True
    resDict = {'Failed':{}, 'Successful':successful}
    return S_OK( resDict )

  def setReplicaHost( self, lfn ):
    res = self.__checkArgumentFormat( lfn )
    if not res['OK']:
      return res
    successful = {}
    for lfn, _info in res['Value'].items():
      successful[lfn] = True
    resDict = {'Failed':{}, 'Successful':successful}
    return S_OK( resDict )

  def removeDirectory( self, lfn, recursive = False ):
    res = self.__checkArgumentFormat( lfn )
    if not res['OK']:
      return res
    successful = {}
    for lfn, _info in res['Value'].items():
      successful[lfn] = True
    resDict = {'Failed':{}, 'Successful':successful}
    return S_OK( resDict )

  def createDirectory( self, lfn ):
    res = self.__checkArgumentFormat( lfn )
    if not res['OK']:
      return res
    successful = {}
    for lfn, _info in res['Value'].items():
      successful[lfn] = True
    resDict = {'Failed':{}, 'Successful':successful}
    return S_OK( resDict )

  def removeLink( self, lfn ):
    res = self.__checkArgumentFormat( lfn )
    if not res['OK']:
      return res
    successful = {}
    for lfn, _info in res['Value'].items():
      successful[lfn] = True
    resDict = {'Failed':{}, 'Successful':successful}
    return S_OK( resDict )

  def createLink( self, lfn ):
    res = self.__checkArgumentFormat( lfn )
    if not res['OK']:
      return res
    successful = {}
    for lfn, _info in res['Value'].items():
      successful[lfn] = True
    resDict = {'Failed':{}, 'Successful':successful}
    return S_OK( resDict )

  def exists( self, path ):
    res = self.__checkArgumentFormat( path )
    if not res['OK']:
      return res
    lfns = res['Value'].keys()
    return self.__exists( lfns )

  def getFileMetadata( self, path ):
    res = self.__checkArgumentFormat( path )
    if not res['OK']:
      return res
    lfns = res['Value'].keys()
    return self.__getFileMetadata( lfns )

  def getFileSize( self, path ):
    res = self.__checkArgumentFormat( path )
    if not res['OK']:
      return res
    lfns = res['Value'].keys()
    res = self.__getFileMetadata( lfns )
    #FIXME: this is an unreachable line !
    if not res['OK']:
      return res
    failed = res['Value']['Failed']
    successful = {}
    for lfn, metadata in res['Value']['Successful'].items():
      successful[lfn] = metadata['FileSize']
    resDict = {'Successful':successful, 'Failed':failed}
    return S_OK( resDict )

  ################################################################
  #
  # These are the internal methods used for actual interaction with the BK service
  #

  @staticmethod
  def __checkArgumentFormat( path ):
    '''
      If argument given is a string, returns false. If it is a list, all them
      are converted into dictionary keys with false value. If a dictionary,
      it returns it.
    '''
    #FIXME: why value is False ? Just a default ?
    if type( path ) in types.StringTypes:
      urls = {path:False}
    elif type( path ) == types.ListType:
      urls = {}
      for url in path:
        urls[url] = False
    elif type( path ) == types.DictType:
      urls = path
    else:
      errStr = "BookkeepingDBClient.__checkArgumentFormat: Supplied path is not of the correct format."
      gLogger.error( errStr )
      return S_ERROR( errStr )
    return S_OK( urls )

  def __setHasReplicaFlag( self, lfns ):
    '''
      Set replica flags on BKK
    '''
    server = RPCClient( self.url, timeout = 120 )
    gLogger.verbose( "**** Set replica flag on %s" % self.url )
    successful = {}
    failed = {}
    for lfnList in breakListIntoChunks( lfns, self.splitSize ):
      res = server.addFiles( lfnList )
      if not res['OK']:
        for lfn in lfnList:
          failed[lfn] = res['Message']
      else:
        for lfn in lfnList:
          if res['Value'].has_key( lfn ):
            failed[lfn] = res['Value'][lfn]
          else:
            successful[lfn] = True
    resDict = {'Successful':successful, 'Failed':failed}
    return S_OK( resDict )

  def __unsetHasReplicaFlag( self, lfns ):
    '''
      Removes replica flags on BKK
    '''
    #FIXME: this method is VERY VERY similar to setHasReplicaFlag.. why not refactor
    # a little bit ?
    server = RPCClient( self.url, timeout = 120 )
    successful = {}
    failed = {}
    for lfnList in breakListIntoChunks( lfns, self.splitSize ):
      res = server.removeFiles( lfnList )
      if not res['OK']:
        for lfn in lfnList:
          failed[lfn] = res['Message']
      else:
        for lfn in lfnList:
          if res['Value'].has_key( lfn ):
            failed[lfn] = res['Value'][lfn]
          else:
            successful[lfn] = True
    resDict = {'Successful':successful, 'Failed':failed}
    return S_OK( resDict )

  def __exists( self, lfns ):
    '''
      Checks if lfns exist
    '''
    #FIXME: what if the lfns returned by the service are different than the ones
    # we are asking for.. the output will be totally messed up.
    server = RPCClient( self.url, timeout = 120 )
    successful = {}
    failed = {}
    for lfnList in breakListIntoChunks( lfns, self.splitSize ):
      res = server.exists( lfnList )
      if not res['OK']:
        for lfn in lfnList:
          #FIXME: W(TF)hat happens with failed after all ?
          failed[lfn] = res['Message']
      else:
        for lfn, exists in res['Value'].items():
          successful[lfn] = exists
    resDict = {'Successful':successful, 'Failed':{}}
    return S_OK( resDict )

  def __getFileMetadata( self, lfns ):
    '''
      Returns lfns metadata
    '''
    server = RPCClient( self.url, timeout = 120 )
    successful = {}
    failed = {}
    for lfnList in breakListIntoChunks( lfns, self.splitSize ):
      res = server.getFileMetadata( lfnList )
      if not res['OK']:
        for lfn in lfnList:
          failed[lfn] = res['Message']
      else:
        for lfn in lfnList:
          if not lfn in res['Value']['Successful'].keys():
            failed[lfn] = 'File does not exist'
          #FIXME: Should not it be isinstance( res['Value'][lfn], str )
          elif res['Value']['Successful'][lfn] in types.StringTypes:
            failed[lfn] = res['Value']['Successful'][lfn]
          else:
            successful[lfn] = res['Value']['Successful'][lfn]
    resDict = {'Successful':successful, 'Failed':failed}
    return S_OK( resDict )
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
