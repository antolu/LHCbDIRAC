""" Client plug-in for the RAWIntegrity catalogue.
    This exposes a single method to add files to the RAW IntegrityDB.

    USED at OnLine
"""

from DIRAC                                     import S_OK, gLogger
from DIRAC.ConfigurationSystem.Client          import PathFinder
from DIRAC.Resources.Catalog.FileCatalogueBase import FileCatalogueBase
from DIRAC.Resources.Utilities.Utils           import checkArgumentFormat

__RCSID__ = '$Id$'

class RAWIntegrityClient( FileCatalogueBase ):

  def __init__( self ):
    try:
      self.url = PathFinder.getServiceURL( 'DataManagement/RAWIntegrity' )
      self.valid = True
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
    resDict = {
               'Failed'     : failed,
               'Successful' : successful
               }
    return S_OK( resDict )

  def addFile( self, lfn ):
    res = checkArgumentFormat( lfn )
    if not res['OK']:
      return res
    failed = {}
    successful = {}
    for lfn, _info in res['Value'].items():
      pass
      # FIXME
#      server = RPCClient( self.url, timeout = 120 )
#      pfn = str( info['PFN'] )
#      size = int( info['Size'] )
#      se = str( info['SE'] )
#      guid = str( info['GUID'] )
#      checksum = str( info['Checksum'] )
#      res = server.addFile( lfn, pfn, size, se, guid, checksum )
#      if not res['OK']:
#        failed[lfn] = res['Message']
#      else:
#        successful[lfn] = True
    resDict = {
               'Failed'     : failed,
               'Successful' : successful
               }
    return S_OK( resDict )

  def getPathPermissions( self, path ):
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
