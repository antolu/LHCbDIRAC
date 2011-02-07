########################################################################
# $Id$
########################################################################

""" StepAccounting module performs several common operations at the end of
    a workflow step, in particular prepares and sends the step accounting
    data
"""

__RCSID__ = "$Id$"

import os, time

import DIRAC
from DIRAC  import S_OK, S_ERROR, gLogger, gConfig
from LHCbDIRAC.AccountingSystem.Client.Types.JobStep import JobStep
from DIRAC.AccountingSystem.Client.DataStoreClient import DataStoreClient
from DIRAC.Core.Security.Misc import getProxyInfo

from LHCbDIRAC.Workflow.Modules.ModuleBase import ModuleBase

class StepAccounting( ModuleBase ):

  def __init__( self ):

    self.PRODUCTION_ID = None
    self.JOB_ID = None
#    self.STEP_NUMBER = None
    self.STEP_TYPE = None

    self.user = ''
    self.group = ''
    self.parameters = []

    self.stepDictionary = {}

    if self.workflow_commons.has_key( 'AccountingClient' ):
      self.accounting = self.workflow_commons['AccountingClient']
      self.globalAccounting = True
    else:
      self.accounting = DataStoreClient()
      self.globalAccounting = False

  ########################################################################

  def execute( self ):

    jobID = os.environ['JOBID']
#    sname = 'Step_%d' % int(self.step_commons['STEP_NUMBER'])
    setup = gConfig.getValue( '/DIRAC/Setup', '' )
    result = getProxyInfo()
    if result['OK']:
      proxyDict = result[ 'Value' ]
      userDN = proxyDict[ 'identity' ]
      if 'group' in proxyDict:
        self.group = proxyDict[ 'group' ]
      else:
        self.group = 'unknown'
      if 'username' in proxyDict:
        self.user = proxyDict[ 'username' ]
      else:
        self.user = 'unknown'

    status = 'Done'

    self.resolveInputVariables()


    # This is the final step module. Its output status is the status of the whole step
    if appStatus == "Failed":
      return S_ERROR( 'Application failed' )
    if status == "Failed":
      return S_ERROR( 'Workflow failure' )
    return S_OK()


  ########################################################################


  def resolveInputVariables( self ):
    """ By convention all workflow parameters are resolved here.
    """

    self.log.debug( self.workflow_commons )
    self.log.debug( self.step_commons )

#    buono per FinalState?
    if self.step_commons.has_key( 'Status' ):
      if self.step_commons['Status'] == "Failed":
        status = 'Failed'

    # Check if the step is worth accounting
    do_accounting = True
    if not self.step_commons.has_key( 'applicationName' ):
      do_accounting = False

    if do_accounting:
      appName = self.step_commons['applicationName']
      appVersion = "Unknown"
      if self.step_commons.has_key( 'applicationVersion' ):
        appVersion = self.step_commons['applicationVersion']
      eventType = "Unknown"
      if self.step_commons.has_key( 'eventType' ):
        eventType = self.step_commons['eventType']
      appStatus = "Unknown"

      # ApplicationStatus never provided
      if self.step_commons.has_key( 'ApplicationStatus' ):
        appStatus = self.step_commons['ApplicationStatus']

      self.stepDictionary['ApplicationName'] = appName
      self.stepDictionary['ApplicationVersion'] = appVersion
      if appStatus != "Unknown":
        self.stepDictionary['FinalState'] = appStatus
      else:
        self.stepDictionary['FinalState'] = status
      self.stepDictionary['EventType'] = eventType
      self.stepDictionary['JobGroup'] = self.PRODUCTION_ID
      self.stepDictionary['User'] = self.user
      self.stepDictionary['Group'] = self.group
      self.stepDictionary['Site'] = DIRAC.siteName()

#    che me ne faccio?
#    self.parameters.append((jobID,sname+' Status',status))
#    self.parameters.append((jobID,sname+' Type',self.STEP_TYPE))

    ########################################################################
    # Timing
    exectime = 0
    if self.step_commons.has_key( 'StartTime' ):
      exectime = time.time() - self.step_commons['StartTime']
    cputime = 0
    if self.step_commons.has_key( 'StartStats' ):
      stats = os.times()
      cputime = stats[0] + stats[2] - self.step_commons['StartStats'][0] - self.step_commons['StartStats'][2]
    normcpu = cputime
    cpuNormFactor = gConfig.getValue ( "/LocalSite/CPUNomalizationFactor", 0.0 )
    if cpuNormFactor:
      normcpu = cputime * cpuNormFactor

    self.parameters.append( ( jobID, sname + ' CPUTime', cputime ) )
    self.parameters.append( ( jobID, sname + ' NormCPUTime', normcpu ) )
    self.parameters.append( ( jobID, sname + ' ExecutionTime', exectime ) )

    if do_accounting:
      self.stepDictionary['CPUTime'] = cputime
      self.stepDictionary['NormCPUTime'] = normcpu
      self.stepDictionary['ExecTime'] = exectime

    ########################################################################
    # Data

    # Assume that the application module managed to define these values
    for item in ['InputData', 'OutputData', 'InputEvents', 'OutputEvents']:
      if self.step_commons.has_key( item ):
        parameters.append( ( jobID, sname + ' ' + item, self.step_commons[item] ) )
        if do_accounting:
          stepDictionary[item] = self.step_commons[item]

    ########################################################################
    # Data collected, send it now
    if do_accounting:
      stepAccount = JobStep()
      stepAccount.setValuesFromDict( stepDictionary )
      result = stepAccount.commit()
      if not result['OK']:
        # Failover request
        if self.workflow_commons.has_key( 'Request' ):
          request = self.workflow_commons['Request']
          request.addSubRequest( 'accounting', DISETSubRequest( result['rpStub'] ) )

    # Send step parameters
    if self.workflow_commons.has_key( 'JobReport' ):
      jobReport = self.workflow_commons['JobReport']
      result = jobReport.setJobParameters( parameters )
