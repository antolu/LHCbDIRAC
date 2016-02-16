""" This is the LHCb Online storage """

import types
import xmlrpclib
import copy

from DIRAC                                      import gLogger, S_OK, S_ERROR
from DIRAC.Resources.Storage.StorageBase        import StorageBase
#from stat                                       import *

__RCSID__ = "$Id: LHCbOnlineStorage.py 85389 2015-08-27 16:45:51Z chaen $"

class LHCbOnlineStorage( StorageBase ):

  def __init__( self, storageName, parameterDict ):
    self.isok = True

    super( LHCbOnlineStorage, self ).__init__( storageName, parameterDict )
    self.pluginName = 'LHCbOnline'
    self.name = storageName
    self.timeout = 100
    
    serverString = "%s://%s:%s" % ( self.protocolParameters['Protocol'],
                                    self.protocolParameters['Host'],
                                    self.protocolParameters['Port'] )
    self.server  = xmlrpclib.Server( serverString )


  def getProtocolPfn( self, pfnDict, withPort ):
    #FIXME: What the hell is this method doing ??
    """ From the pfn dict construct the SURL to be used
    """
#    pfnDict['Path'] = ''
#    res = pfnunparse(pfnDict)
#    pfn = res['Value'].replace('/','')
    return S_OK( pfnDict['FileName'] )

  def getFileSize( self, path ):
    #FIXME: What the hell is this method doing ??
    """ Get a fake file size
    """
    res = self.__checkArgumentFormat( path )
    if not res['OK']:
      return res
    urls = res['Value']
    if not len( path ) > 0:
      return S_ERROR( "LHCbOnline.getFileSize: No surls supplied." )
    successful = {}
    failed = {}
    for pfn in urls:
      successful[pfn] = 0
    resDict = {'Failed':failed, 'Successful':successful}
    return S_OK( resDict )

  def retransferOnlineFile( self, path ):
    """ Tell the Online system that the migration failed and we want to get the request again
    """
    res = self.__checkArgumentFormat( path )
    if not res['OK']:
      return res
    urls = res['Value']
    if not len( urls ) > 0:
      return S_ERROR( "LHCbOnline.requestRetransfer: No surls supplied." )
    successful = {}
    failed = {}
    for pfn in urls:
      try:
        success, error = self.server.errorMigratingFile( pfn )
        if success:
          successful[pfn] = True
          gLogger.info( "LHCbOnline.requestRetransfer: Successfully requested file from RunDB." )
        else:
          errStr = "LHCbOnline.requestRetransfer: Failed to request file from RunDB: %s" % error
          failed[pfn] = errStr
          gLogger.error( errStr, pfn )
      except Exception, x:
        errStr = "LHCbOnline.requestRetransfer: Exception while requesting file from RunDB."
        gLogger.exception( errStr, lException = x )
        failed[pfn] = errStr
    resDict = {'Failed':failed, 'Successful':successful}
    return S_OK( resDict )

  def removeFile( self, path ):
    """Remove physically the file specified by its path
    """
    res = self.__checkArgumentFormat( path )
    if not res['OK']:
      return res
    urls = res['Value']
    if not len( urls ) > 0:
      return S_ERROR( "LHCbOnline.removeFile: No surls supplied." )
    successful = {}
    failed = {}
    for pfn in urls:
      try:
        success, error = self.server.endMigratingFile( pfn )
        if success:
          successful[pfn] = True
          gLogger.info( "LHCbOnline.getFile: Successfully issued removal to RunDB." )
        else:
          errStr = "LHCbOnline.getFile: Failed to issue removal to RunDB: %s" % error
          failed[pfn] = errStr
          gLogger.error( errStr, pfn )
      except Exception, x:
        errStr = "LHCbOnline.getFile: Exception while issuing removal to RunDB."
        gLogger.exception( errStr, lException = x )
        failed[pfn] = errStr
        #FIXME: this should return S_ERROR !! 
    resDict = {'Failed':failed, 'Successful':successful}
    return S_OK( resDict )

  def __checkArgumentFormat( self, path ):
    if type( path ) in types.StringTypes:
      urls = [path]
    elif type( path ) == types.ListType:
      urls = path
    elif type( path ) == types.DictType:
      urls = path.keys()
    else:
      return S_ERROR( "LHCbOnline.__checkArgumentFormat: Supplied path is not of the correct format." )
    return S_OK( urls )
