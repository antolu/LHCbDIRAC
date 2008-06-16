########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/WorkflowLib/Module/ModuleBase.py,v 1.4 2008/06/16 16:33:44 atsareg Exp $
########################################################################

""" ModuleBase - base class for LHCb workflow modules. Defines several
    common utility methods

"""

__RCSID__ = "$Id: ModuleBase.py,v 1.4 2008/06/16 16:33:44 atsareg Exp $"

from DIRAC  import S_OK, S_ERROR, gLogger, gConfig
from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.RequestManagementSystem.Client.DISETSubRequest import DISETSubRequest

class ModuleBase(object):


  #############################################################################
  def setApplicationStatus(self,status):
    """Wraps around setJobApplicationStatus of state update client
    """
    if not self.jobID:
      return S_OK('JobID not defined') # e.g. running locally prior to submission

    self.log.verbose('setJobApplicationStatus(%s,%s,%s)' %(self.jobID,status,'JobFinalization'))

    if self.workflow_commons.has_key('JobReport'):
      self.jobReport  = self.workflow_commons['JobReport']

    if not self.jobReport:
      return S_OK('No reporting tool given')
    jobStatus = self.jobReport.setApplicationStatus(status)
    if not jobStatus['OK']:
      self.log.warn(jobStatus['Message'])

    return jobStatus

  #############################################################################
  def setJobParameter(self,name,value):
    """Wraps around setJobParameter of state update client
    """
    if not self.jobID:
      return S_OK('JobID not defined') # e.g. running locally prior to submission

    self.log.verbose('setJobParameter(%s,%s,%s)' %(self.jobID,name,value))

    if self.workflow_commons.has_key('JobReport'):
      self.jobReport  = self.workflow_commons['JobReport']

    if not self.jobReport:
      return S_OK('No reporting tool given')
    jobParam = self.jobReport.setJobParameter(str(name),str(value))
    if not jobParam['OK']:
      self.log.warn(jobParam['Message'])

    return jobParam

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
      integrityDB = RPCClient('DataManagement/DataIntegrity')
      fileMetadata = {'Prognosis':reason,'LFN':lfn,'PFN':pfn,'StorageElement':se}
      result = integrityDB.insertProblematic(source,fileMetadata)
      if not result['OK']:
        # Add it to the request
        if self.workflow_commons.has_key('Request'):
          request  = self.workflow_commons['Request']
          subrequest = DISETSubRequest(result['rpcStub']).getDictionary()
          request.addSubRequest(subrequest,'integrity')

    return S_OK()


