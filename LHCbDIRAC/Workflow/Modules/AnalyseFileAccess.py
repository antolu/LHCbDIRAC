""" Analyse XMLSummary module and PoolCatalog in order to monitor the files access
    We send data to the accounting (Site -> SE : fail/success)
"""

__RCSID__ = "$Id$"

from DIRAC import S_OK, gLogger

from DIRAC.FrameworkSystem.Client.NotificationClient import NotificationClient
from DIRAC.Resources.Catalog.PoolXMLCatalog import PoolXMLCatalog
from DIRAC.AccountingSystem.Client.Types.DataOperation import DataOperation
from DIRAC.AccountingSystem.Client.DataStoreClient import gDataStoreClient

from LHCbDIRAC.Workflow.Modules.ModuleBase import ModuleBase
from LHCbDIRAC.Core.Utilities.XMLSummaries import XMLSummary

class AnalyseFileAccess( ModuleBase ):
  """ Analyzing the access with xroot
  """

  def __init__( self, bkClient = None, dm = None ):
    """Module initialization.
    """

    self.log = gLogger.getSubLogger( 'AnalyseFileAccess' )
    super( AnalyseFileAccess, self ).__init__( self.log, bkClientIn = bkClient, dm = dm )

    self.version = __RCSID__
    self.nc = NotificationClient()
    self.XMLSummary = ''
    self.XMLSummary_o = None
    self.poolXMLCatName = ''
    self.poolXMLCatName_o = None

    # Used to cache the information PFN -> SE from the catalog
    self.pfn_se = {}
    # Used to cache the information LFN -> PFN from the catalog
    self.lfn_pfn = {}

    # Holds a cache of the mapping SE -> Site
    self.seSiteCache = {}

  def _resolveInputVariables( self ):
    """ By convention any workflow parameters are resolved here.
    """

    super( AnalyseFileAccess, self )._resolveInputVariables()
    super( AnalyseFileAccess, self )._resolveInputStep()

    self.XMLSummary_o = XMLSummary( self.XMLSummary, log = self.log )
    self.poolXMLCatName_o = PoolXMLCatalog( xmlfile = self.poolXMLCatName )

  def execute( self, production_id = None, prod_job_id = None, wms_job_id = None,
               workflowStatus = None, stepStatus = None,
               wf_commons = None, step_commons = None,
               step_number = None, step_id = None ):
    """ Main execution method.

        Here we analyse what is written in the XML summary and the pool XML, and send accounting
    """

    try:
      super( AnalyseFileAccess, self ).execute( self.version,
                                                production_id, prod_job_id, wms_job_id,
                                                workflowStatus, stepStatus,
                                                wf_commons, step_commons,
                                                step_number, step_id )

      self._resolveInputVariables()

      self.log.info( "Analyzing root access from %s and %s" % ( self.XMLSummary, self.poolXMLCatName ) )

      pfn_lfn = {}
      lfn_guid = {}

      lfn_pfn_fail = {}
      successful_lfn = set()

      for guid in self.poolXMLCatName_o.files:
        pFile = self.poolXMLCatName_o.files[guid]
        lfn = pFile.lfns[0] # there can be only one
        lfn_guid[lfn] = guid
        self.lfn_pfn[lfn] = []
        for pfn, _ftype, se in pFile.pfns:
          pfn_lfn[pfn] = lfn
          self.pfn_se[pfn] = se
          self.lfn_pfn[lfn].append(pfn)

      for inputFile, status in self.XMLSummary_o.inputStatus:

        # The inputFile starts with 'LFN:' or 'PFN:'
        cleanedName = inputFile[4:]
        if status == 'full':
          # it is an LFN
          successful_lfn.add( cleanedName )
        elif status == 'fail':
          # it is a PFN
          lfn = pfn_lfn.get( cleanedName )
          if not lfn:
            self.log.error( "Failed pfn %s is not listed in the catalog" % cleanedName )
            continue
          lfn_pfn_fail.setdefault( lfn, [] ).append( cleanedName )
        else:
          # intermediate status, think of it...
          pass

      # The lfn in successful and not in lfn_pfn_failed succeeded immediately
      immediately_successful = successful_lfn - set( lfn_pfn_fail )


      for lfn in immediately_successful:
        # We take the first replica in the catalog
        pfn = self.__getNthPfnForLfn( lfn, 0 )
        remoteSE = self.pfn_se.get( pfn )

        if not remoteSE:
          continue

        oDataOperation = self.__initialiseAccountingObject( remoteSE, True )
        gDataStoreClient.addRegister( oDataOperation )

      # For each file that had failure
      for lfn in lfn_pfn_fail:
        failedPfns = lfn_pfn_fail[lfn]

        # We add the accounting for the failure
        for pfn in failedPfns:
          remoteSE = self.pfn_se.get( pfn )
          if not remoteSE:
            continue

          oDataOperation = self.__initialiseAccountingObject( remoteSE, False )
          gDataStoreClient.addRegister( oDataOperation )

        # If there were more options to try, the next one is successful
        if len( failedPfns ) < len( self.lfn_pfn[lfn] ):
          pfn = self.__getNthPfnForLfn( lfn, len( failedPfns ) )
          remoteSE = self.pfn_se.get( pfn )

          if not remoteSE:
            continue

          oDataOperation = self.__initialiseAccountingObject( remoteSE, True )
          gDataStoreClient.addRegister( oDataOperation )

      gDataStoreClient.commit()

    except Exception as e:
      self.log.warn( str( e ) )

    finally:
      super( AnalyseFileAccess, self ).finalize( self.version )

    return S_OK()

  def __initialiseAccountingObject( self, destSE, successful ):
    """ create accouting record """
    accountingDict = {}

    accountingDict['OperationType'] = 'fileAccess'
    accountingDict['User'] = 'AnalyseFileAccess'
    accountingDict['Protocol'] = 'xroot'
    accountingDict['RegistrationTime'] = 0.0
    accountingDict['RegistrationOK'] = 0
    accountingDict['RegistrationTotal'] = 0
    accountingDict['Destination'] = destSE
    accountingDict['TransferTotal'] = 1
    accountingDict['TransferOK'] = 1 if successful else 0
    accountingDict['TransferSize'] = 0
    accountingDict['TransferTime'] = 0.0
    accountingDict['FinalStatus'] = 'Successful' if successful else 'Failed'
    accountingDict['Source'] = self.siteName
    oDataOperation = DataOperation()
    oDataOperation.setValuesFromDict( accountingDict )

    if 'StartTime' in self.step_commons:
      oDataOperation.setStartTime( self.step_commons['StartTime'] )
      oDataOperation.setEndTime( self.step_commons['StartTime'] )

    return oDataOperation


  def __getNthPfnForLfn( self, lfn, pfnIndex ):
    """ Returns the Nth pfn from the catalog order for the given lfn
        :param lfn : lfn from the catalog
        :param pfnIndex : nth element

        :returns the pfn or None
    """

    pfns = self.lfn_pfn.get( lfn )
    if not pfns:
      self.log.error("Could not find lfn %s in catalog"%lfn)
      return None
    if len(pfns) <= pfnIndex:
      self.log.error("Index %s out of range, length %s"%(pfnIndex, len(pfns)))
      return None

    pfn = pfns[pfnIndex]

    return pfn

# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
