__RCSID__ = "$Id: $"

import json
from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Core.Security import Properties
from DIRAC.Core.Security import CS
from types import StringType, StringTypes, IntType, LongType, ListType, DictType, FloatType
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.RequestManagementSystem.Client.Request import Request
from DIRAC.RequestManagementSystem.Client.Operation import Operation
from DIRAC.RequestManagementSystem.Client.ReqClient import ReqClient
from DIRAC.Core.Security.X509Chain import X509Chain
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.ConfigurationSystem.Client.Helpers import cfgPath
from DIRAC.Core.DISET.RPCClient import RPCClient


class WMSSecureGW( RequestHandler ):

  @classmethod
  def initializeHandler( cls, serviceInfo ):
    """ Handler initialization
    """
    return S_OK()


  types_requestJob = [ [StringType, DictType] ]
  def export_requestJob( self, resourceDescription ):
    """ Serve a job to the request of an agent which is the highest priority
        one matching the agent's site capacity
    """
    matcher = RPCClient( 'WorkloadManagement/Matcher', timeout = 600 )
    result = matcher.requestJob( resourceDescription )
    return result

  ##########################################################################################
  types_setJobStatus = [[StringType, IntType, LongType], StringType, StringType, StringType]
  def export_setJobStatus( self, jobID, status, minorStatus, source = 'Unknown', datetime = None ):
    """ Set the major and minor status for job specified by its JobId.
        Set optionally the status date and source component which sends the
        status information.
    """
    jobReport = RPCClient( 'WorkloadManagement/JobStateUpdate' )
    jobStatus = jobReport.setJobStatus( int( jobID ), status, minorStatus, source, datetime )
    return jobStatus

  ###########################################################################
  types_setJobSite = [[StringType, IntType, LongType], StringType]
  def export_setJobSite( self, jobID, site ):
    """Allows the site attribute to be set for a job specified by its jobID.
    """
    jobReport = RPCClient( 'WorkloadManagement/JobStateUpdate' )
    jobSite = jobReport.setJobSite( jobID, site )
    return jobSite

     ###########################################################################
  types_setJobParameter = [[StringType, IntType, LongType], StringType, StringType]
  def export_setJobParameter( self, jobID, name, value ):
    """ Set arbitrary parameter specified by name/value pair
        for job specified by its JobId
    """
    jobReport = RPCClient( 'WorkloadManagement/JobStateUpdate' )
    jobParam = jobReport.setJobParameter( jobID, name, value )
    return jobParam

   ###########################################################################
  types_setJobStatusBulk = [[StringType, IntType, LongType], DictType]
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
  types_setJobParameters = [[StringType, IntType, LongType], ListType]
  def export_setJobParameters( self, jobID, parameters ):
    """ Set arbitrary parameters specified by a list of name/value pairs
        for job specified by its JobId
    """
    jobReport = RPCClient( 'WorkloadManagement/JobStateUpdate' )
    jobParams = jobReport.setJobParameters( jobID, parameters )
    return jobParams


  ###########################################################################
  types_sendHeartBeat = [[StringType, IntType, LongType], DictType, DictType]
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
  types_setJobForPilot = [ [StringType, IntType, LongType], StringTypes]
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
  types_getJobParameter = [ [StringType, IntType, LongType] , StringTypes ]
  @staticmethod
  def export_getJobParameter( jobID, parName ):
    monitoring = RPCClient( 'WorkloadManagement/JobMonitoring', timeout = 120 )
    result = monitoring.getJobParameter( jobID, parName )
    return result

  ##############################################################################
  types_getProxy = [ StringType, StringType, StringType, ( IntType, LongType ) ]
  def export_getProxy( self, userDN, userGroup, requestPem, requiredLifetime ):
    userDN, userGroup = self.__getOwnerGroupDN( 'BoincUser' )
    result = self.checkProperties( userDN, userGroup )
    if not result[ 'OK' ]:
      return result
    forceLimited = result[ 'Value' ]
    chain = X509Chain()
    f = open( '/tmp/x509up_u500', 'r' )
    proxy = f.read()
    retVal = chain.loadProxyFromString( proxy )
    print retVal
    if not retVal[ 'OK' ]:
      return retVal
    retVal = chain.generateChainFromRequestString( requestPem,
                                                   lifetime = requiredLifetime,
                                                   requireLimited = forceLimited )
    print retVal
    return S_OK( retVal[ 'Value' ] )

  def checkProperties( self, requestedUserDN, requestedUserGroup ):
    """
    Check the properties and return if they can only download limited proxies if authorized
    """
    credDict = self.getRemoteCredentials()
    print credDict
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

  types_hasAccess = [StringTypes, [ ListType, DictType ] + list( StringTypes ) ]
  def export_hasAccess( self, opType, paths ):
    """ Access
    """ 
    successful = {}
    for path in paths:
      successful[path] = True
    resDict = {'Successful':successful, 'Failed':{}}
    return S_OK( resDict )

  types_exists = [ [ ListType, DictType ] + list( StringTypes ) ]
  def export_exists( self, lfns ):
    """ Check whether the supplied paths exists """     
    successful = {}
    for lfn in lfns:
      print lfn
      successful[lfn] = False
    resDict = {'Successful':successful, 'Failed':{}}
    return S_OK(resDict)
 
   ########################################################################

  types_addFile = [ [ ListType, DictType ] + list( StringTypes ) ]
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
    requestName = requestDict.get( "RequestID", requestDict.get( 'RequestName', "***UNKNOWN***" ) )
    request = Request( requestDict )
    operation = Operation()  # # create new operation
    operation.Type = "WMSSecureOutputData"
    request.insertBefore( operation, request[0] )
    userDN, userGroup = self.__getOwnerGroupDN( 'ProductionManager' )
    request.OwnerDN = userDN
    request.OwnerGroup = userGroup
    return ReqClient().putRequest( request )

  def __getOwnerGroupDN ( self, shifterType ):
    opsHelper = Operations()
    userName = opsHelper.getValue( cfgPath( 'Shifter',shifterType, 'User' ), '' )
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
    userGroup = opsHelper.getValue( cfgPath( 'Shifter', shifterType, 'Group' ), defaultGroup )
    return userDN, userGroup

    

