""" The input data resolution module is a VO-specific plugin that
    allows to define VO input data policy in a simple way using existing
    utilities in DIRAC or extension code supplied by the VO.

    The arguments dictionary from the Job Wrapper includes the file catalogue
    result and in principle has all necessary information to resolve input data
    for applications.

"""

__RCSID__ = "$Id$"

import DIRAC
from DIRAC                                                          import S_OK, S_ERROR, gLogger
from DIRAC.ConfigurationSystem.Client.Helpers.Operations            import Operations
from DIRAC.Core.Utilities.ModuleFactory                             import ModuleFactory
from DIRAC.WorkloadManagementSystem.Client.PoolXMLSlice             import PoolXMLSlice
from DIRAC.WorkloadManagementSystem.Client.InputDataResolution      import InputDataResolution as DIRACInputDataResolution

import types

COMPONENT_NAME = 'LHCbInputDataResolution'
CREATE_CATALOG = False

class InputDataResolution ( DIRACInputDataResolution ):
  """ Define the Input Data Policy
  """

  def __init__( self, argumentsDict, bkkClient = None ):
    """ Standard constructor
    """
    self.arguments = argumentsDict
    self.arguments.setdefault( 'Configuration', {} )['AllReplicas'] = True
    self.name = COMPONENT_NAME
    self.log = gLogger.getSubLogger( self.name )

    if not bkkClient:
      from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
      self.bkkClient = BookkeepingClient()
    else:
      self.bkkClient = bkkClient

  #############################################################################

  def execute( self ):
    """Given the arguments from the Job Wrapper, this function calls existing
       utilities in DIRAC to resolve input data according to LHCb VO policy.
    """
    result = DIRACInputDataResolution.execute( self )
    if not result['OK'] or not result['Value'].get( 'Successful', {} ):
      return result
    resolvedData = result['Value']['Successful']

    resolvedData = self._addPfnType( resolvedData )
    if not resolvedData['OK']:
      return resolvedData
    resolvedData = resolvedData['Value']

    # TODO: Below is temporary behaviour to prepend mdf: to resolved TURL(s) for case when not a ROOT file
    # This instructs the Gaudi applications to use root to access different file types e.g. for MDF.
    # In the longer term this should be derived from file catalog metadata information.
    # 24/08/2010 - updated hack to use "mdf:" after udpates from Markus
    for lfn, mdataList in resolvedData.items():
      if type( mdataList ) != types.ListType:
        mdataList = [mdataList]
      for mdata in mdataList:
        if mdata['pfntype'] == 'MDF':
          mdata['turl'] = 'mdf:' + mdata['turl']
          self.log.info( 'Prepending mdf: to TURL for %s' % lfn )

    catalogName = self.arguments['Configuration'].get( 'CatalogName', 'pool_xml_catalog.xml' )

    self.log.verbose( 'Catalog name will be: %s' % catalogName )
    appCatalog = PoolXMLSlice( catalogName )
    check = appCatalog.execute( resolvedData )
    if not check['OK']:
      return check
    return result

  #############################################################################

  def _addPfnType( self, resolvedData ):
    """ Add the pfn type to the lfn list in input
    """

    typeVersions = self.bkkClient.getFileTypeVersion( resolvedData.keys() )
    if not typeVersions['OK']:
      return typeVersions
    typeVersions = typeVersions['Value']
    print resolvedData

    for lfn, mdataList in resolvedData.items():
      print type( mdataList )
      if type( mdataList ) != types.ListType:
        mdataList = [mdataList]
      if lfn not in typeVersions:
        self.log.warn( 'The file %s do not exist in the BKK, assuming ROOT, unless it is a RAW (MDF)' % lfn )
        if lfn.split( '.' )[-1].lower() == 'raw':
          lfnType = 'MDF'
        else:
          lfnType = 'ROOT'
      else:
        self.log.verbose( 'Adding PFN file type %s for %s' % ( typeVersions[lfn], lfn ) )
        lfnType = typeVersions[lfn]
      print mdataList
      for mdata in mdataList:
        mdata['pfntype'] = lfnType

    return S_OK( resolvedData )

  #############################################################################

# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
