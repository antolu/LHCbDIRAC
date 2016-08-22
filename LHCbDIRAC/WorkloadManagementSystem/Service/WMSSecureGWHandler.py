""" WMSSecureGW service -  a generic gateway service

    Mostly used by BOINC
"""

import json
import os
from types import IntType, LongType, DictType, StringTypes, ListType, FloatType
from DIRAC import S_OK, S_ERROR, gLogger, gConfig
from DIRAC.Core.Security import Properties
from DIRAC.Core.Security import CS
from DIRAC.Core.Utilities.Subprocess import pythonCall
from DIRAC.Core.Utilities.File import mkDir
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.RequestManagementSystem.Client.Request import Request
from DIRAC.RequestManagementSystem.Client.Operation import Operation
from DIRAC.RequestManagementSystem.Client.ReqClient import ReqClient
from DIRAC.ConfigurationSystem.Client.Helpers import Registry
from DIRAC.FrameworkSystem.Client.ProxyManagerClient  import gProxyManager
from DIRAC.Core.Security.X509Chain import X509Chain
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.ConfigurationSystem.Client.Helpers import cfgPath
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.Resources.Storage.StorageElement import StorageElement
from DIRAC.Core.Utilities.ReturnValues import returnSingleResult


__RCSID__ = "$Id: $"

#pylint: disable=no-self-use

class WMSSecureGWHandler( RequestHandler ):
  """ WMSSecure class
  """
  @classmethod
  def initializeHandler( cls, serviceInfo ): #pylint: disable=unused-argument
    """ Handler initialization
    """
    from DIRAC.DataManagementSystem.Service.FileCatalogHandler import FileCatalogHandler
    if FileCatalogHandler.types_hasAccess != cls.types_hasAccess:
      raise Exception( "FileCatalog hasAccess types has been changed." )
    if FileCatalogHandler.types_exists != cls.types_exists:
      raise Exception( "FileCatalog exists types has been changed." )
    if FileCatalogHandler.types_addFile != cls.types_addFile:
      raise Exception( "FileCatalog addFile types has been changed." )
    from DIRAC.DataManagementSystem.Service.StorageElementProxyHandler import StorageElementProxyHandler
    if StorageElementProxyHandler.types_prepareFile != cls.types_prepareFile:
      raise Exception( "StorageElementProxyHandler prepareFile types has been changed." )
    from DIRAC.WorkloadManagementSystem.Service.MatcherHandler import MatcherHandler
    if MatcherHandler.types_requestJob != cls.types_requestJob:
      raise Exception( "Matcher requestJob types has been changed." )
    from DIRAC.WorkloadManagementSystem.Service.JobStateUpdateHandler import JobStateUpdateHandler
    if JobStateUpdateHandler.types_setJobStatus != cls.types_setJobStatus:
      raise Exception( "JobStateUpdate setJobStatus types has been changed." )
    if JobStateUpdateHandler.types_setJobSite != cls.types_setJobSite:
      raise Exception( "JobStateUpdate setJobSite types has been changed." )
    if JobStateUpdateHandler.types_setJobParameter != cls.types_setJobParameter:
      raise Exception( "JobStateUpdate setJobParameter types has been changed." )
    if JobStateUpdateHandler.types_setJobStatusBulk != cls.types_setJobStatusBulk:
      raise Exception( "JobStateUpdate setJobStatusBulk types has been changed." )
    if JobStateUpdateHandler.types_setJobParameters != cls.types_setJobParameters:
      raise Exception( "JobStateUpdate setJobParameters types has been changed." )
    if JobStateUpdateHandler.types_sendHeartBeat != cls.types_sendHeartBeat:
      raise Exception( "JobStateUpdate sendHeartBeat types has been changed." )
    from DIRAC.WorkloadManagementSystem.Service.JobManagerHandler import JobManagerHandler
    if JobManagerHandler.types_rescheduleJob != cls.types_rescheduleJob:
      raise Exception( "JobManager rescheduleJob types has been changed." )
    from DIRAC.WorkloadManagementSystem.Service.WMSAdministratorHandler import WMSAdministratorHandler
    if WMSAdministratorHandler.types_setPilotStatus != cls.types_setPilotStatus:
      raise Exception( "WMSAdministrator setPilotStatus types has been changed." )
    if WMSAdministratorHandler.types_setJobForPilot != cls.types_setJobForPilot:
      raise Exception( "WMSAdministrator setJobForPilot types has been changed." )
    if WMSAdministratorHandler.types_setPilotBenchmark != cls.types_setPilotBenchmark:
      raise Exception( "WMSAdministrator setPilotBenchmark types has been changed." )
    from DIRAC.WorkloadManagementSystem.Service.JobMonitoringHandler import JobMonitoringHandler
    if JobMonitoringHandler.types_getJobParameter != cls.types_getJobParameter:
      raise Exception( "JobMonitoring getJobParameter types has been changed." )
    from DIRAC.FrameworkSystem.Service.ProxyManagerHandler import ProxyManagerHandler
    if ProxyManagerHandler.types_getVOMSProxy != cls.types_getVOMSProxy:
      raise Exception( "ProxyManagerHandler getVOMSProxy types has been changed." )
    if ProxyManagerHandler.types_getProxy != cls.types_getProxy:
      raise Exception( "ProxyManagerHandler getProxy types has been changed." )
    from DIRAC.RequestManagementSystem.Service.ReqManagerHandler import ReqManagerHandler
    if ReqManagerHandler.types_putRequest != cls.types_putRequest:
      raise Exception( "ReqManagerHandler putRequest types has been changed." )

    

    return S_OK()


  types_requestJob = [ list( StringTypes ) + [DictType] ]
  def export_requestJob( self, resourceDescription ):
    """ Serve a job to the request of an agent which is the highest priority
        one matching the agent's site capacity
    """
    matcher = RPCClient( 'WorkloadManagement/Matcher', timeout = 600 )
    result = matcher.requestJob( resourceDescription )
    return result

  ##########################################################################################
  types_setJobStatus = [list( StringTypes ) + [ IntType, LongType], StringTypes, StringTypes, StringTypes]
  def export_setJobStatus( self, jobID, status, minorStatus, source = 'Unknown', datetime = None ):
    """ Set the major and minor status for job specified by its JobId.
        Set optionally the status date and source component which sends the
        status information.
    """
    jobReport = RPCClient( 'WorkloadManagement/JobStateUpdate' )
    jobStatus = jobReport.setJobStatus( int( jobID ), status, minorStatus, source, datetime )
    return jobStatus

  ###########################################################################
  types_setJobSite = [list( StringTypes ) + [ IntType, LongType], StringTypes]
  def export_setJobSite( self, jobID, site ):
    """Allows the site attribute to be set for a job specified by its jobID.
    """
    jobReport = RPCClient( 'WorkloadManagement/JobStateUpdate' )
    jobSite = jobReport.setJobSite( jobID, site )
    return jobSite

  ###########################################################################
  types_setJobParameter = [list( StringTypes ) + [IntType, LongType], StringTypes, StringTypes]
  def export_setJobParameter( self, jobID, name, value ):
    """ Set arbitrary parameter specified by name/value pair
        for job specified by its JobId
    """
    jobReport = RPCClient( 'WorkloadManagement/JobStateUpdate' )
    jobParam = jobReport.setJobParameter( jobID, name, value )
    return jobParam

  ###########################################################################
  types_setJobStatusBulk = [list( StringTypes ) + [ IntType, LongType], DictType]
  def export_setJobStatusBulk( self, jobID, statusDict ):
    """ Set various status fields for job specified by its JobId.
        Set only the last status in the JobDB, updating all the status
        logging information in the JobLoggingDB. The statusDict has datetime
        as a key and status information dictionary as values
    """
    jobReport = RPCClient( 'WorkloadManagement/JobStateUpdate' )
    jobStatus = jobReport.setJobStatusBulk( jobID, statusDict )
    return jobStatus

    ###########################################################################
  types_setJobParameters = [list( StringTypes ) + [ IntType, LongType], ListType]
  def export_setJobParameters( self, jobID, parameters ):
    """ Set arbitrary parameters specified by a list of name/value pairs
        for job specified by its JobId
    """
    jobReport = RPCClient( 'WorkloadManagement/JobStateUpdate' )
    jobParams = jobReport.setJobParameters( jobID, parameters )
    return jobParams


  ###########################################################################
  types_sendHeartBeat = [list( StringTypes ) + [ IntType, LongType], DictType, DictType]
  def export_sendHeartBeat( self, jobID, dynamicData, staticData ):
    """ Send a heart beat sign of life for a job jobID
    """
    jobReport = RPCClient( 'WorkloadManagement/JobStateUpdate', timeout = 120 )
    result = jobReport.sendHeartBeat( jobID, dynamicData, staticData )
    return result


  ##########################################################################################
  types_rescheduleJob = [ ]
  def export_rescheduleJob( self, jobIDs ):
    """  Reschedule a single job. If the optional proxy parameter is given
         it will be used to refresh the proxy in the Proxy Repository
    """
    jobManager = RPCClient( 'WorkloadManagement/JobManager' )
    result = jobManager.rescheduleJob( jobIDs )
    return result

  ##########################################################################################
  types_setPilotStatus = [StringTypes, StringTypes]
  def export_setPilotStatus( self, pilotRef, status, destination = None, reason = None, gridSite = None, queue = None ):
    """ Set the pilot agent status
    """
    wmsAdmin = RPCClient( 'WorkloadManagement/WMSAdministrator' )
    result = wmsAdmin.setPilotStatus( pilotRef, status, destination,
                                      reason, gridSite, queue )
    return result

  ##############################################################################
  types_setJobForPilot = [ list( StringTypes ) + [ IntType, LongType], StringTypes]
  def export_setJobForPilot( self, jobID, pilotRef, destination = None ):
    """ Report the DIRAC job ID which is executed by the given pilot job
    """
    wmsAdmin = RPCClient( 'WorkloadManagement/WMSAdministrator' )
    result = wmsAdmin.setJobForPilot( jobID , pilotRef, destination )
    return result

  ##########################################################################################
  types_setPilotBenchmark = [StringTypes, FloatType]
  def export_setPilotBenchmark( self, pilotRef, mark ):
    """ Set the pilot agent benchmark
    """
    wmsAdmin = RPCClient( 'WorkloadManagement/WMSAdministrator' )
    result = wmsAdmin.setPilotBenchmark( pilotRef, mark )
    return result


  ##############################################################################
  types_getJobParameter = [ list( StringTypes ) + [ IntType, LongType] , StringTypes ]
  @staticmethod
  def export_getJobParameter( jobID, parName ):
    monitoring = RPCClient( 'WorkloadManagement/JobMonitoring', timeout = 120 )
    result = monitoring.getJobParameter( jobID, parName )
    return result

  ##############################################################################
  types_getVOMSProxy = [ basestring, basestring, basestring, ( int, long ) ]
  def export_getVOMSProxy( self, userDN, userGroup, requestPem, requiredLifetime, vomsAttribute = False ): #pylint: disable=unused-argument
    """
    Always return the Boinc proxy.
    """
    return self.__getProxy( requestPem, requiredLifetime )


  ##############################################################################
  types_getProxy = [ basestring, basestring, basestring, ( int, long ) ]
  def export_getProxy( self, userDN, userGroup, requestPem, requiredLifetime ): #pylint: disable=unused-argument
    """Get the Boinc User proxy
    """
    return self.__getProxy( requestPem, requiredLifetime )


  def __getProxy ( self, requestPem, requiredLifetime ):
    """Get the Boinc User proxy
    """
    userDN, userGroup, userName = self.__getOwnerGroupDN( 'BoincUser' )  #pylint: disable=unused-variable
    result = self.__checkProperties( userDN, userGroup )
    if not result[ 'OK' ]:
      return result
    forceLimited = result[ 'Value' ]
    chain = X509Chain()
    proxyFile = "/tmp/x509up_u" + basestring( os.getuid() )
    retVal = chain.loadProxyFromFile( proxyFile )
    if not retVal[ 'OK' ]:
      return retVal
    retVal = chain.generateChainFromRequestString( requestPem,
                                                   lifetime = requiredLifetime,
                                                   requireLimited = forceLimited )
    gLogger.debug( "Got the proxy" )
    return S_OK( retVal[ 'Value' ] )



  def __checkProperties( self, requestedUserDN, requestedUserGroup ):
    """
    Check the properties and return if they can only download limited proxies if authorized
    """
    credDict = self.getRemoteCredentials()
    gLogger.debug ( "in credDict %s" % credDict[ 'properties' ] )
    if Properties.FULL_DELEGATION in credDict[ 'properties' ]:
      return S_OK( False )
    if Properties.LIMITED_DELEGATION in credDict[ 'properties' ]:
      return S_OK( True )
    if Properties.PRIVATE_LIMITED_DELEGATION in credDict[ 'properties' ]:
      if credDict[ 'DN' ] != requestedUserDN:
        return S_ERROR( "You are not allowed to download any proxy" )
      if Properties.PRIVATE_LIMITED_DELEGATION in Registry.getPropertiesForGroup( requestedUserGroup ):
        return S_ERROR( "You can't download proxies for that group" )
      return S_OK( True )
    # Not authorized!
    return S_ERROR( "You can't get proxies! Bad boy!" )


  ########################################################################

  types_hasAccess = [[basestring, dict], [ basestring, list, dict ]]
  def export_hasAccess( self, paths, opType ):  # pylint: disable=unused-argument
    """ Access
    """
    successful = {}
    for path in paths:
      successful[path] = True
    resDict = {'Successful':successful, 'Failed':{}}
    return S_OK( resDict )

  types_exists = [[ ListType, DictType ] + list( StringTypes )]
  def export_exists( self, lfns ):
    """ Check whether the supplied paths exists """
    successful = {}
    for lfn in lfns:
      successful[lfn] = False
    resDict = {'Successful':successful, 'Failed':{}}
    return S_OK(resDict)

  ########################################################################

  types_addFile = [[ ListType, DictType ] + list( StringTypes )]
  def export_addFile( self, lfns ):
    """ Register supplied files """
    failed={}
    for lfn in lfns:
      failed [lfn]=True
    return S_OK({'Successful':{}, 'Failed':failed} )

  types_putRequest = [ StringTypes ]
  def export_putRequest( self, requestJSON ):
    """ put a new request into RequestDB """

    requestDict = json.loads( requestJSON )
    requestName = requestDict.get( "RequestID", requestDict.get( 'RequestName', "***UNKNOWN***" ) ) #pylint: disable=unused-variable
    request = Request( requestDict )
    operation = Operation()  # # create new operation
    operation.Type = "WMSSecureOutputData"
    request.insertBefore( operation, request[0] )
    userDN, userGroup, userName = self.__getOwnerGroupDN( 'ProductionManager' ) #pylint: disable=unused-variable
    request.OwnerDN = userDN
    request.OwnerGroup = userGroup
    return ReqClient().putRequest( request )

  def __getOwnerGroupDN ( self, shifterType ):
    opsHelper = Operations()
    userName = opsHelper.getValue( cfgPath( 'BoincShifter', shifterType, 'User' ), '' )
    if not userName:
      return S_ERROR( "No shifter User defined for %s" % shifterType )
    result = CS.getDNForUsername( userName )
    if not result[ 'OK' ]:
      return result
    userDN = result[ 'Value' ][0]
    result = CS.findDefaultGroupForDN( userDN )
    if not result['OK']:
      return result
    defaultGroup = result['Value']
    userGroup = opsHelper.getValue( cfgPath( 'BoincShifter', shifterType, 'Group' ), defaultGroup )
    return userDN, userGroup, userName


  ################################################################################
  types_prepareFile = [ StringTypes, StringTypes ]
  def export_prepareFile(self, se, pfn):
    """ This method simply gets the file to the local storage area
    """
    gLogger.debug( "se %s, pfn %s" % ( se, pfn ) )
    res = pythonCall( 300, self.__prepareFile, se, pfn )
    gLogger.debug( "Preparing File %s" % res )
    if res['OK']:
      return res['Value']
    return res

  def __prepareFile( self, se, pfn ):
    """ proxied prepare file """

    res = self.__prepareSecurityDetails()
    if not res['OK']:
      return res
    # Clear the local ache
    base = gConfig.getValue( "Systems/DataManagement/boincInstance/Services/StorageElementProxy/BasePath" )
    getFileDir = "%s/getFile" % base
    mkDir(getFileDir)
    # Get the file to the cache
    storageElement = StorageElement( se )
    res = returnSingleResult( storageElement.getFile( pfn, localPath = getFileDir ) )
    if not res['OK']:
      gLogger.error( "prepareFile: Failed to get local copy of file.", res['Message'] )
      return res
    return res

  def __prepareSecurityDetails(self):
    """ Obtains the connection details for the client
    """
    try:
      clientDN, clientGroup, clientUserName = self.__getOwnerGroupDN('ProductionManager')
      gLogger.debug( "Getting proxy for %s %s" % (clientGroup, clientDN ) )
      res = gProxyManager.downloadVOMSProxy( clientDN, clientGroup )
      if not res['OK']:
        return res
      chain = res['Value']
      base = ""
      base = gConfig.getValue( "Systems/DataManagement/boincInstance/Services/StorageElementProxy/BasePath" )
      proxyBase = "%s/proxies" % base
      mkDir(proxyBase)
      proxyLocation = "%s/proxies/%s-%s" % ( base, clientUserName, clientGroup )
      gLogger.debug("Obtained proxy chain, dumping to %s." % proxyLocation)
      res = gProxyManager.dumpProxyToFile( chain, proxyLocation )
      if not res['OK']:
        return res
      gLogger.debug("Updating environment.")
      os.environ['X509_USER_PROXY'] = res['Value']
      return res
    except Exception as error:
      exStr = "__getConnectionDetails: Failed to get client connection details."
      gLogger.exception( exStr, '', error )
      return S_ERROR(exStr)
