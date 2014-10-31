""" Client plug-in for the RAWIntegrity catalogue.
    This exposes a single method to add files to the RAW IntegrityDB.

    USED at OnLine
"""

from DIRAC                                     import S_OK, gLogger
from DIRAC.ConfigurationSystem.Client          import PathFinder
from DIRAC.Resources.Utilities.Utils           import checkArgumentFormat
from DIRAC.Core.Base.Client import Client

__RCSID__ = '$Id$'

class RAWIntegrityClient( Client ):

  def __init__( self, url = '' ):
    Client.__init__( self )
    try:
      if url:
        self.url = url
      else:
        self.url = PathFinder.getServiceURL( 'DataManagement/RAWIntegrity' )
      self.setServer( url )
      self.valid = True
      self.rawIntegritySrv = self._getRPC()
    except Exception, x:
      errStr = "RAWIntegrityClient.__init__: Exception while generating server url."
      gLogger.exception( errStr, lException = x )
      self.valid = False

  def isOK( self ):
    """
      Returns valid
    """
    return self.valid

  def exists( self, lfn ):
    """ LFN may be a string or list of strings
    """
    res = checkArgumentFormat( lfn )
    if not res['OK']:
      return res
    lfns = res['Value']
    successful = {}
    failed = {}
    for lfn in lfns.keys():
      successful[lfn] = False
    resDict = {'Failed'     : failed,
               'Successful' : successful}
    return S_OK( resDict )

  def addFile( self, lfn ):
    res = checkArgumentFormat( lfn )
    if not res['OK']:
      return res
    failed = {}
    successful = {}
    for lfn, info in res['Value'].items():
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

    resDict = {
               'Failed'     : failed,
               'Successful' : successful
               }
    return S_OK( resDict )

  @staticmethod
  def getPathPermissions( path ):
    """ Determine the VOMs based ACL information for a supplied path
    """
    res = checkArgumentFormat( path )
    if not res['OK']:
      return res
    lfns = res['Value']
    failed = {}
    successful = {}
    for lfn in lfns.keys():
      successful[lfn] = {'Write':True}
    resDict = {'Failed':failed, 'Successful':successful}
    return S_OK( resDict )

################################################################################
# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
