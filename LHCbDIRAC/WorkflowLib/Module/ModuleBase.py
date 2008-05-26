########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/WorkflowLib/Module/ModuleBase.py,v 1.1 2008/05/26 14:37:54 atsareg Exp $
########################################################################

""" ModuleBase - base class for LHCb workflow modules. Defines several
    common utility methods

"""

__RCSID__ = "$Id: ModuleBase.py,v 1.1 2008/05/26 14:37:54 atsareg Exp $"

from DIRAC  import S_OK, S_ERROR, gLogger, gConfig

class ModuleBase(object):

    
  #############################################################################
  def __report(self,status):
    """Wraps around setJobApplicationStatus of state update client
    """
    if not self.jobID:
      return S_OK('JobID not defined') # e.g. running locally prior to submission

    self.log.verbose('setJobApplicationStatus(%s,%s,%s)' %(self.jobID,status,'JobFinalization'))
    if not self.jobReport:
      return S_OK('No reporting tool given')
    jobStatus = self.jobReport.setApplicationStatus(status)
    if not jobStatus['OK']:
      self.log.warn(jobStatus['Message'])

    return jobStatus

  #############################################################################
  def __setJobParam(self,name,value):
    """Wraps around setJobParameter of state update client
    """
    if not self.jobID:
      return S_OK('JobID not defined') # e.g. running locally prior to submission

    self.log.verbose('setJobParameter(%s,%s,%s)' %(self.jobID,name,value))
    if not self.jobReport:
      return S_OK('No reporting tool given')
    jobParam = self.jobReport.setJobParameter(str(name),str(value))
    if not jobParam['OK']:
      self.log.warn(jobParam['Message'])

    return jobParam