""" Client plug-in for the RAWIntegrity catalog.
    This exposes a single method to add files to the RAW IntegrityDB.

    USED at OnLine
"""

from DIRAC import S_OK
from DIRAC.Resources.Catalog.Utilities                              import checkCatalogArguments
from DIRAC.Resources.Catalog.FileCatalogClientBase import FileCatalogClientBase

__RCSID__ = "$Id$"

__RCSID__ = '$Id$'

READ_METHODS = []

WRITE_METHODS = [ 'addFile' ]

NO_LFN_METHODS = []


class RAWIntegrityClient( FileCatalogClientBase ):

  def __init__( self, url = '', **kwargs ):

    self.serverURL = 'DataManagement/RAWIntegrity' if not url else url
    super( RAWIntegrityClient, self ).__init__( self.serverURL, **kwargs )
    self.rawIntegritySrv = self._getRPC()

      
  @staticmethod
  def getInterfaceMethods():
    """ Get the methods implemented by the File Catalog client

    :return tuple: ( read_methods_list, write_methods_list, nolfn_methods_list )
    """
    return ( READ_METHODS, WRITE_METHODS, NO_LFN_METHODS )

  @staticmethod
  def hasCatalogMethod( methodName ):
    """ Check of a method with the given name is implemented
    :param str methodName: the name of the method to check
    :return: boolean Flag
    """
    return methodName in ( READ_METHODS + WRITE_METHODS + NO_LFN_METHODS )


  def isOK( self ):
    """
      Returns valid
    """
    return self.valid


  @checkCatalogArguments
  def exists( self, lfns ):
    """ LFN may be a string or list of strings
    """
    return S_OK( {'Failed' : {}, 'Successful' : dict.fromkeys( lfns, False )} )

  @checkCatalogArguments
  def addFile( self, lfns ):
    failed = {}
    successful = {}
    for lfn, info in lfns.items():
      pfn = str( info['PFN'] )
      size = int( info['Size'] )
      se = str( info['SE'] )
      guid = str( info['GUID'] )
      checksum = str( info['Checksum'] )
      res = self.rawIntegritySrv.addFile( lfn, pfn, size, se, guid, checksum )
#       rpc = self._getRPC()
#       rpc.addFile( lfn, pfn, size, se, guid, checksum )
      if not res['OK']:
        failed[lfn] = res['Message']
      else:
        successful[lfn] = True

    resDict = {'Failed': failed,
               'Successful': successful}
    return S_OK( resDict )

  @staticmethod
  @checkCatalogArguments
  def getPathPermissions( paths ):
    """ Determine the VOMs based ACL information for a supplied path
    """
    return S_OK( {'Failed' : {}, 'Successful' : dict.fromkeys( paths, {'Write':True} )} )

  @staticmethod
  @checkCatalogArguments
  def hasAccess( _opType, paths ):
    """ Returns True for all path and all actions"""
    return S_OK( {'Failed' : {}, 'Successful' : dict.fromkeys( paths, True )} )

################################################################################
# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
