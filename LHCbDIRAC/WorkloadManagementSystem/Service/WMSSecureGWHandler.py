""" WMSSecureGW service -  a generic gateway service

    Mostly used by BOINC
"""

import json
from types import DictType, StringTypes, ListType
from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.Security import Properties, CS
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.RequestManagementSystem.Client.Request import Request
from DIRAC.RequestManagementSystem.Client.Operation import Operation
from DIRAC.RequestManagementSystem.Client.ReqClient import ReqClient
from DIRAC.ConfigurationSystem.Client.Helpers import Registry
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.ConfigurationSystem.Client.Helpers import cfgPath
from DIRAC.Core.DISET.RPCClient import RPCClient

__RCSID__ = "$Id: $"

# pylint: disable=no-self-use


class WMSSecureGWHandler(RequestHandler):
  """ WMSSecure class
  """
  @classmethod
  def initializeHandler(cls, serviceInfo):  # pylint: disable=unused-argument
    """ Handler initialization
    """
    from DIRAC.DataManagementSystem.Service.FileCatalogHandler import FileCatalogHandler
    if FileCatalogHandler.types_hasAccess != cls.types_hasAccess:
      raise Exception("FileCatalog hasAccess types has been changed.")
    if FileCatalogHandler.types_exists != cls.types_exists:
      raise Exception("FileCatalog exists types has been changed.")
    if FileCatalogHandler.types_addFile != cls.types_addFile:
      raise Exception("FileCatalog addFile types has been changed.")
    from DIRAC.WorkloadManagementSystem.Service.MatcherHandler import MatcherHandler
    if MatcherHandler.types_requestJob != cls.types_requestJob:
      raise Exception("Matcher requestJob types has been changed.")
    from DIRAC.WorkloadManagementSystem.Service.JobStateUpdateHandler import JobStateUpdateHandler
    if JobStateUpdateHandler.types_setJobStatus != cls.types_setJobStatus:
      raise Exception("JobStateUpdate setJobStatus types has been changed.")
    if JobStateUpdateHandler.types_setJobSite != cls.types_setJobSite:
      raise Exception("JobStateUpdate setJobSite types has been changed.")
    if JobStateUpdateHandler.types_setJobParameter != cls.types_setJobParameter:
      raise Exception("JobStateUpdate setJobParameter types has been changed.")
    if JobStateUpdateHandler.types_setJobStatusBulk != cls.types_setJobStatusBulk:
      raise Exception("JobStateUpdate setJobStatusBulk types has been changed.")
    if JobStateUpdateHandler.types_setJobParameters != cls.types_setJobParameters:
      raise Exception("JobStateUpdate setJobParameters types has been changed.")
    if JobStateUpdateHandler.types_sendHeartBeat != cls.types_sendHeartBeat:
      raise Exception("JobStateUpdate sendHeartBeat types has been changed.")
    from DIRAC.WorkloadManagementSystem.Service.JobManagerHandler import JobManagerHandler
    if JobManagerHandler.types_rescheduleJob != cls.types_rescheduleJob:
      raise Exception("JobManager rescheduleJob types has been changed.")
    from DIRAC.WorkloadManagementSystem.Service.WMSAdministratorHandler import WMSAdministratorHandler
    if WMSAdministratorHandler.types_setPilotStatus != cls.types_setPilotStatus:
      raise Exception("WMSAdministrator setPilotStatus types has been changed.")
    if WMSAdministratorHandler.types_setJobForPilot != cls.types_setJobForPilot:
      raise Exception("WMSAdministrator setJobForPilot types has been changed.")
    if WMSAdministratorHandler.types_setPilotBenchmark != cls.types_setPilotBenchmark:
      raise Exception("WMSAdministrator setPilotBenchmark types has been changed.")
    from DIRAC.WorkloadManagementSystem.Service.JobMonitoringHandler import JobMonitoringHandler
    if JobMonitoringHandler.types_getJobParameter != cls.types_getJobParameter:
      raise Exception("JobMonitoring getJobParameter types has been changed.")
    from DIRAC.FrameworkSystem.Service.ProxyManagerHandler import ProxyManagerHandler
    if ProxyManagerHandler.types_getVOMSProxy != cls.types_getVOMSProxy:
      raise Exception("ProxyManagerHandler getVOMSProxy types has been changed.")
    if ProxyManagerHandler.types_getProxy != cls.types_getProxy:
      raise Exception("ProxyManagerHandler getProxy types has been changed.")
    from DIRAC.RequestManagementSystem.Service.ReqManagerHandler import ReqManagerHandler
    if ReqManagerHandler.types_putRequest != cls.types_putRequest:
      raise Exception("ReqManagerHandler putRequest types has been changed.")
    from DIRAC.AccountingSystem.Service.DataStoreHandler import DataStoreHandler
    if DataStoreHandler.types_commitRegisters != cls.types_commitRegisters:
      raise Exception("DataStoreHandler commitRegisters types has been changed.")

    return S_OK()

  types_requestJob = [[basestring, dict]]

  def export_requestJob(self, resourceDescription):
    """ Serve a job to the request of an agent which is the highest priority
        one matching the agent's site capacity
    """
    matcher = RPCClient('WorkloadManagement/Matcher', timeout=600)
    result = matcher.requestJob(resourceDescription)
    return result

  ###########################################################################
  types_setJobStatus = [[basestring, int, long], basestring, basestring, basestring]

  def export_setJobStatus(self, jobID, status, minorStatus, source='Unknown', datetime=None):
    """ Set the major and minor status for job specified by its JobId.
        Set optionally the status date and source component which sends the
        status information.
    """
    jobReport = RPCClient('WorkloadManagement/JobStateUpdate')
    jobStatus = jobReport.setJobStatus(int(jobID), status, minorStatus, source, datetime)
    return jobStatus

  ###########################################################################
  types_setJobSite = [[basestring, int, long], basestring]

  def export_setJobSite(self, jobID, site):
    """Allows the site attribute to be set for a job specified by its jobID.
    """
    jobReport = RPCClient('WorkloadManagement/JobStateUpdate')
    jobSite = jobReport.setJobSite(jobID, site)
    return jobSite

  ###########################################################################
  types_setJobParameter = [[basestring, int, long], basestring, basestring]

  def export_setJobParameter(self, jobID, name, value):
    """ Set arbitrary parameter specified by name/value pair
        for job specified by its JobId
    """
    jobReport = RPCClient('WorkloadManagement/JobStateUpdate')
    jobParam = jobReport.setJobParameter(int(jobID), name, value)
    return jobParam

  ###########################################################################
  types_setJobStatusBulk = [[basestring, int, long], dict]

  def export_setJobStatusBulk(self, jobID, statusDict):
    """ Set various status fields for job specified by its JobId.
        Set only the last status in the JobDB, updating all the status
        logging information in the JobLoggingDB. The statusDict has datetime
        as a key and status information dictionary as values
    """
    jobReport = RPCClient('WorkloadManagement/JobStateUpdate')
    jobStatus = jobReport.setJobStatusBulk(jobID, statusDict)
    return jobStatus

  ###########################################################################
  types_setJobParameters = [[basestring, int, long], list]

  def export_setJobParameters(self, jobID, parameters):
    """ Set arbitrary parameters specified by a list of name/value pairs
        for job specified by its JobId
    """
    jobReport = RPCClient('WorkloadManagement/JobStateUpdate')
    jobParams = jobReport.setJobParameters(jobID, parameters)
    return jobParams

  ###########################################################################
  types_sendHeartBeat = [[basestring, int, long], dict, dict]

  def export_sendHeartBeat(self, jobID, dynamicData, staticData):
    """ Send a heart beat sign of life for a job jobID
    """
    jobReport = RPCClient('WorkloadManagement/JobStateUpdate', timeout=120)
    result = jobReport.sendHeartBeat(jobID, dynamicData, staticData)
    return result

  ##########################################################################################
  types_rescheduleJob = []

  def export_rescheduleJob(self, jobIDs):
    """  Reschedule a single job. If the optional proxy parameter is given
         it will be used to refresh the proxy in the Proxy Repository
    """
    jobManager = RPCClient('WorkloadManagement/JobManager')
    result = jobManager.rescheduleJob(jobIDs)
    return result

  ##########################################################################################
  types_setPilotStatus = [basestring, basestring]
  def export_setPilotStatus(self, pilotRef, status, destination=None, reason=None, gridSite=None, queue=None):
    """ Set the pilot agent status
    """
    wmsAdmin = RPCClient('WorkloadManagement/WMSAdministrator')
    result = wmsAdmin.setPilotStatus(pilotRef, status, destination,
                                     reason, gridSite, queue)
    return result

  ##############################################################################
  types_setJobForPilot = [(basestring, int, long), basestring]
  def export_setJobForPilot(self, jobID, pilotRef, destination=None):
    """ Report the DIRAC job ID which is executed by the given pilot job
    """
    wmsAdmin = RPCClient('WorkloadManagement/WMSAdministrator')
    result = wmsAdmin.setJobForPilot(jobID, pilotRef, destination)
    return result

  ##########################################################################################
  types_setPilotBenchmark = [basestring, float]
  def export_setPilotBenchmark(self, pilotRef, mark):
    """ Set the pilot agent benchmark
    """
    wmsAdmin = RPCClient('WorkloadManagement/WMSAdministrator')
    result = wmsAdmin.setPilotBenchmark(pilotRef, mark)
    return result

  ##############################################################################
  types_getJobParameter = [[basestring, int, long], basestring]

  @staticmethod
  def export_getJobParameter(jobID, parName):
    monitoring = RPCClient('WorkloadManagement/JobMonitoring', timeout=120)
    result = monitoring.getJobParameter(jobID, parName)
    return result

  ##############################################################################
  types_getVOMSProxy = [basestring, basestring, basestring, (int, long)]

  def export_getVOMSProxy(self, userDN, userGroup, requestPem,
                          requiredLifetime, vomsAttribute=False):  # pylint: disable=unused-argument
    """
    Always return the Boinc proxy.
    """
    userDN, userGroup, _ = self.__getOwnerGroupDN('BoincUser')
    rpcClient = RPCClient("Framework/BoincProxyManager", timeout=120)
    retVal = rpcClient.getProxy(userDN, userGroup, requestPem, requiredLifetime)
    return retVal

  ##############################################################################
  types_getProxy = [basestring, basestring, basestring, (int, long)]

  def export_getProxy(self, userDN, userGroup, requestPem, requiredLifetime):  # pylint: disable=unused-argument
    """Get the Boinc User proxy
    """
    userDN, userGroup, _ = self.__getOwnerGroupDN('BoincUser')
    rpcClient = RPCClient("Framework/BoincProxyManager", timeout=120)
    retVal = rpcClient.getProxy(userDN, userGroup, requestPem, requiredLifetime)
    return retVal

  ##############################################################################
  def __checkProperties(self, requestedUserDN, requestedUserGroup):
    """
    Check the properties and return if they can only download limited proxies if authorized
    """
    credDict = self.getRemoteCredentials()
    gLogger.debug("in credDict %s" % credDict['properties'])
    if Properties.FULL_DELEGATION in credDict['properties']:
      return S_OK(False)
    if Properties.LIMITED_DELEGATION in credDict['properties']:
      return S_OK(True)
    if Properties.PRIVATE_LIMITED_DELEGATION in credDict['properties']:
      if credDict['DN'] != requestedUserDN:
        return S_ERROR("You are not allowed to download any proxy")
      if Properties.PRIVATE_LIMITED_DELEGATION in Registry.getPropertiesForGroup(requestedUserGroup):
        return S_ERROR("You can't download proxies for that group")
      return S_OK(True)
    # Not authorized!
    return S_ERROR("You can't get proxies! Bad boy!")

  ########################################################################

  types_hasAccess = [[basestring, dict], [basestring, list, dict]]

  def export_hasAccess(self, paths, opType):  # pylint: disable=unused-argument
    """ Access
    """
    successful = {}
    for path in paths:
      successful[path] = True
    resDict = {'Successful': successful, 'Failed': {}}
    return S_OK(resDict)

  types_exists = [[ListType, DictType] + list(StringTypes)]

  def export_exists(self, lfns):
    """ Check whether the supplied paths exists """
    successful = {}
    for lfn in lfns:
      successful[lfn] = False
    resDict = {'Successful': successful, 'Failed': {}}
    return S_OK(resDict)

  ########################################################################

  types_addFile = [[ListType, DictType] + list(StringTypes)]

  def export_addFile(self, lfns):
    """ Register supplied files """
    failed = {}
    for lfn in lfns:
      failed[lfn] = True
    return S_OK({'Successful': {}, 'Failed': failed})

  types_putRequest = [basestring]

  def export_putRequest(self, requestJSON):
    """ put a new request into RequestDB """

    requestDict = json.loads(requestJSON)
    request = Request(requestDict)
    operation = Operation()  # # create new operation
    operation.Type = "WMSSecureOutputData"
    request.insertBefore(operation, request[0])
    userDN, userGroup, _ = self.__getOwnerGroupDN('ProductionManager')
    request.OwnerDN = userDN
    request.OwnerGroup = userGroup
    return ReqClient().putRequest(request)

  def __getOwnerGroupDN(self, shifterType):
    opsHelper = Operations()
    userName = opsHelper.getValue(cfgPath('BoincShifter', shifterType, 'User'), '')
    if not userName:
      return S_ERROR("No shifter User defined for %s" % shifterType)
    result = CS.getDNForUsername(userName)
    if not result['OK']:
      return result
    userDN = result['Value'][0]
    result = CS.findDefaultGroupForDN(userDN)
    if not result['OK']:
      return result
    defaultGroup = result['Value']
    userGroup = opsHelper.getValue(cfgPath('BoincShifter', shifterType, 'Group'), defaultGroup)
    return userDN, userGroup, userName

  ########################################################################

  types_commitRegisters = [list]

  def export_commitRegisters(self, entriesList):
    acc = RPCClient('Accounting/DataStore')
    retVal = acc.commitRegisters(entriesList)
    return retVal
