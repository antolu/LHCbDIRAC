""" Client plug-in for the RAWIntegrity catalog.
    This exposes a single method to add files to the RAW IntegrityDB.

    USED at OnLine
"""

from DIRAC import S_OK
from DIRAC.Resources.Catalog.Utilities                              import checkCatalogArguments
from DIRAC.Resources.Catalog.FileCatalogClientBase import FileCatalogClientBase

__RCSID__ = "$Id$"


class RAWIntegrityClient( FileCatalogClientBase ):

  # List of common File Catalog methods implemented by this client
  WRITE_METHODS = FileCatalogClientBase.WRITE_METHODS + [ 'addFile' ]

  def __init__( self, url = '', **kwargs ):

    self.serverURL = 'DataManagement/RAWIntegrity' if not url else url
    super( RAWIntegrityClient, self ).__init__( self.serverURL, **kwargs )
    self.rawIntegritySrv = self._getRPC()


  def isOK( self ):
    """
      Returns valid
    """
    return self.valid


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

################################################################################
# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
