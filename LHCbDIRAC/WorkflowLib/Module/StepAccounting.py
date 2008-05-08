########################################################################
# $Id: StepAccounting.py,v 1.1 2008/05/08 15:22:23 atsareg Exp $
########################################################################

""" StepFinalization module performs several common operations at the end of
    a workflow step, in particular prepares and sends the step accounting
    data
"""

__RCSID__ = "$Id: StepAccounting.py,v 1.1 2008/05/08 15:22:23 atsareg Exp $"


from DIRAC  import S_OK, S_ERROR, gLogger, gConfig
from DIRAC.AccountingSystem.Client.Types.JobStep import JobStep
from DIRAC.AccountingSystem.Client.DataStoreClient import DataStoreClient
from DIRAC.Core.Utilities.GridCredentials import *

import os, time

class StepFinalization(object):

  def __init__(self):

    self.PRODUCTION_ID = None
    self.JOB_ID = None
    self.STEP_NUMBER = None
    self.STEP_TYPE = None

    if self.workflow_commons.has_key('AccountingClient'):
      self.accounting = self.workflow_commons['AccountingClient']
      self.globalAccounting = True
    else:
      self.accounting = DataStoreClient()
      self.globalAccounting = False

  def execute(self):

    jobID = os.environ['JOBID']
    parameters = []
    sname = 'Step_%d' % int(self.STEP_NUMBER)
    setup = gConfig.getValue('/DIRAC/Setup','')
    userDN = getCurrentDN()
    if userDN:
      user = getNicknameForDN(userDN)
    else:
      user = ''
    group = getDIRACGroup()
    site = gConfig.getValue('/LocalSite/Site','localSite')
    status = 'Done'
    if self.step_commons.has_key('Status'):
      if step_commons['Status'] == "Error":
        status = 'Failed'

    parameters.append((jobID,sname+' Status',status))
    parameters.append((jobID,sname+' Type',self.STEP_TYPE))

    ########################################################################
    # Timing
    exectime = 0
    if self.step_commons.has_key('StartTime'):
      exectime = time.time() - self.step_commons['StartTime']
    cputime = 0
    if self.step_commons.has_key('StartStats'):
      stats = os.times()
      cputime = stats[0]+stats[2]-self.step_commons['StartStats'][0]-self.step_commons['StartStats'][2]
    normcpu = cputime
    if os.environ.has_key('CPU_NORMALIZATION_FACTOR'):
      norm = float(os.environ['CPU_NORMALIZATION_FACTOR'])
      normcpu = cputime*norm

    parameters.append((jobID,sname+' CPUTime',cputime))
    parameters.append((jobID,sname+' NormCPUTime',normcpu))
    parameters.append((jobID,sname+' ExecutionTime',exectime))

    ########################################################################
    # Data


    stepAccount = JobStep()
    stepAccount.setValuesFromDict()
    return result
