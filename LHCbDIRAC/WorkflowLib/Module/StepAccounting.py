########################################################################
# $Id: StepAccounting.py,v 1.2 2008/05/10 19:42:32 atsareg Exp $
########################################################################

""" StepAccounting module performs several common operations at the end of
    a workflow step, in particular prepares and sends the step accounting
    data
"""

__RCSID__ = "$Id: StepAccounting.py,v 1.2 2008/05/10 19:42:32 atsareg Exp $"


from DIRAC  import S_OK, S_ERROR, gLogger, gConfig
from DIRAC.AccountingSystem.Client.Types.JobStep import JobStep
from DIRAC.AccountingSystem.Client.DataStoreClient import DataStoreClient
from DIRAC.Core.Utilities.GridCredentials import *

import os, time

class StepAccounting(object):

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
      if step_commons['Status'] == "Failed":
        status = 'Failed'

    # Check if the step is worth accounting
    do_accounting = True
    if not self.step_commons.has_key('ApplicationName'):
      do_accounting = False

    if do_accounting:
      appName = self.step_commons['ApplicationName']
      appVersion = "Unknown"
      if self.step_commons.has_key('ApplicationVersion'):
        appVersion = self.step_commons['ApplicationVersion']
      eventType = "Unknown"
      if self.step_commons.has_key('EventType'):
        eventType = self.step_commons['EventType']
      appStatus = "Unknown"
      if self.step_commons.has_key('ApplicationStatus'):
        appStatus = self.step_commons['ApplicationStatus']

      stepDictionary = {}
      stepDictionary['ApplicationName'] = appName
      stepDictionary['ApplicationVersion'] = appVersion
      if appStatus != "Unknown":
        stepDictionary['FinalState'] = appStatus
      else:
        stepDictionary['FinalState'] = status
      stepDictionary['EventType'] = eventType
      stepDictionary['JobGroup'] = self.PRODUCTION_ID
      stepDictionary['User'] = user
      stepDictionary['Group'] = group
      stepDictionary['Site'] = site

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

    if do_accounting:
      stepDictionary['CPUTime'] = cputime
      stepDictionary['NormCPUTime'] = normcpu
      stepDictionary['ExecTime'] = exectime

    ########################################################################
    # Data

    # Assume that the application module managed to define these values
    for item in ['InputData','OutputData','InputEvents','OutputEvents']:
      if self.step_commons.has_key(items):
        parameters.append((jobID,sname+' '+item,self.step_commons[item]))
        if do_accounting:
          stepDictionary[item] = self.step_commons[item]

    ########################################################################
    # Data collected, send it now
    if do_accounting:
      stepAccount = JobStep()
      stepAccount.setValuesFromDict(stepDictionary)
      result = stepAccount.commit()
      if not result['OK']:
        # Failover request
        if self.workflow_commons.has_key('Request'):
          request = self.workflow_commons['Request']
          request.addSubRequest('accounting',DISETSubRequest(result['rpStub']))

    # Send step parameters
    if self.workflow_commons.has_key('JobReport'):
      jobReport = self.workflow_commons['JobReport']
      result = jobReport.setJobParameters(parameters)

    # This is the final step module. Its output status is the status of the whole step
    if appStatus == "Failed":
      return S_ERROR('Application failed')
    if status == "Failed":
      return S_ERROR('Workflow failure')
    return S_OK()
