########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/WorkflowLib/Module/ModuleBase.py,v 1.7 2009/04/20 06:41:50 rgracian Exp $
########################################################################

""" ModuleBase - base class for LHCb workflow modules. Defines several
    common utility methods

"""

__RCSID__ = "$Id: ModuleBase.py,v 1.7 2009/04/20 06:41:50 rgracian Exp $"

from DIRAC  import S_OK, S_ERROR, gLogger, gConfig
from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.RequestManagementSystem.Client.DISETSubRequest import DISETSubRequest
from DIRAC.Core.Security.Misc import getProxyInfoAsString

class ModuleBase(object):

  #############################################################################
  def __init__(self):
    """ Initialization of module base.
    """
    self.log = gLogger.getSubLogger('ModuleBase')
    result = getProxyInfoAsString()
    if not result:
      self.log.error('Could not obtain proxy information in module environment with result:\n' %result)
    else:
      if not result['OK']:
        self.log.error('Could not obtain proxy information in module environment with message:\n%s' %result['Message'])
      else:
        self.log.info('Payload proxy information:\n%s' %result['Value'])

  #############################################################################
  def setApplicationStatus(self,status, sendFlag=True):
    """Wraps around setJobApplicationStatus of state update client
    """
    if not self.jobID:
      return S_OK('JobID not defined') # e.g. running locally prior to submission

    self.log.verbose('setJobApplicationStatus(%s,%s)' %(self.jobID,status))

    if self.workflow_commons.has_key('JobReport'):
      self.jobReport  = self.workflow_commons['JobReport']

    if not self.jobReport:
      return S_OK('No reporting tool given')
    jobStatus = self.jobReport.setApplicationStatus(status,sendFlag)
    if not jobStatus['OK']:
      self.log.warn(jobStatus['Message'])

    return jobStatus

  #############################################################################
  def sendStoredStatusInfo(self):
    """Wraps around sendStoredStatusInfo of state update client
    """
    if not self.jobID:
      return S_OK('JobID not defined') # e.g. running locally prior to submission

    if self.workflow_commons.has_key('JobReport'):
      self.jobReport  = self.workflow_commons['JobReport']

    if not self.jobReport:
      return S_OK('No reporting tool given')

    sendStatus = self.jobReport.sendStoredStatusInfo()
    if not sendStatus['OK']:
      self.log.error(sendStatus['Message'])

    return sendStatus

  #############################################################################
  def setJobParameter(self,name,value, sendFlag = True):
    """Wraps around setJobParameter of state update client
    """
    if not self.jobID:
      return S_OK('JobID not defined') # e.g. running locally prior to submission

    self.log.verbose('setJobParameter(%s,%s,%s)' %(self.jobID,name,value))

    if self.workflow_commons.has_key('JobReport'):
      self.jobReport  = self.workflow_commons['JobReport']

    if not self.jobReport:
      return S_OK('No reporting tool given')
    jobParam = self.jobReport.setJobParameter(str(name),str(value),sendFlag)
    if not jobParam['OK']:
      self.log.warn(jobParam['Message'])

    return jobParam

  #############################################################################
  def sendStoredJobParameters(self):
    """Wraps around sendStoredJobParameters of state update client
    """
    if not self.jobID:
      return S_OK('JobID not defined') # e.g. running locally prior to submission

    if self.workflow_commons.has_key('JobReport'):
      self.jobReport  = self.workflow_commons['JobReport']

    if not self.jobReport:
      return S_OK('No reporting tool given')

    sendStatus = self.jobReport.sendStoredJobParameters()
    if not sendStatus['OK']:
      self.log.error(sendStatus['Message'])

    return sendStatus

  #############################################################################
  def setFileStatus(self,production,lfn,status):
    """ set the file status for the given production in the Production Database
    """
    self.log.verbose('setFileStatus(%s,%s,%s)' %(production,lfn,status))

    if self.workflow_commons.has_key('FileReport'):
      self.fileReport = self.workflow_commons['FileReport']
    else:
      self.fileReport = None

    if not self.fileReport:
      return S_OK('No reporting tool given')
    result = self.fileReport.setFileStatus(production,lfn,status)
    if not result['OK']:
      self.log.warn(result['Message'])

    return result

  #############################################################################
  def setReplicaProblematic(self,lfn,se,pfn='',reason='Access failure'):
    """ Set replica status to Problematic in the File Catalog
    """

    rm = ReplicaManager()
    site = gConfig.getValue('/LocalSite/Site','Unknown')
    source = "Job %d at %s" % (self.jobID,site)
    result = rm.setReplicaProblematic((lfn,pfn,se,reason),source)
    if not result['OK'] or result['Value']['Failed']:
      # We have failed the report, let's attempt the Integrity DB faiover
      integrityDB = RPCClient('DataManagement/DataIntegrity',timeout=120)
      fileMetadata = {'Prognosis':reason,'LFN':lfn,'PFN':pfn,'StorageElement':se}
      result = integrityDB.insertProblematic(source,fileMetadata)
      if not result['OK']:
        # Add it to the request
        if self.workflow_commons.has_key('Request'):
          request  = self.workflow_commons['Request']
          subrequest = DISETSubRequest(result['rpcStub']).getDictionary()
          request.addSubRequest(subrequest,'integrity')

    return S_OK()


