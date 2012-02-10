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
from DIRAC import S_OK, S_ERROR, gConfig, gLogger

from LHCbDIRAC.Workflow.Modules.ModuleBase import ModuleBase

class StepAccounting( ModuleBase ):

  def __init__( self ):

    self.log = gLogger.getSubLogger( "StepAccounting" )
    super( StepAccounting, self ).__init__( self.log )

    self.version = __RCSID__

#    if self.workflow_commons.has_key( 'AccountingClient' ):
#      self.accounting = self.workflow_commons['AccountingClient']
#      self.globalAccounting = True
#    else:
#      self.accounting = DataStoreClient()
#      self.globalAccounting = False

  ########################################################################

  def execute( self, production_id = None, prod_job_id = None, wms_job_id = None,
               workflowStatus = None, stepStatus = None,
               wf_commons = None, step_commons = None,
               step_number = None, step_id = None,
               js = None, xf_o = None ):

    try:
      super( StepAccounting, self ).execute( self.version, production_id, prod_job_id, wms_job_id,
                                             workflowStatus, stepStatus,
                                             wf_commons, step_commons, step_number, step_id )

      # Check if the step is worth accounting
      if not self.step_commons.has_key( 'applicationName' ):
        self.log.info( 'Not an application step: it will not be accounted' )
        return S_OK()

      if not js:
        from LHCbDIRAC.AccountingSystem.Client.Types.JobStep import JobStep
        jobStep = JobStep()
      else:
        jobStep = js

      if not xf_o:
        try:
          xf_o = self.step_commons['XMLSummary_o']
        except KeyError:
          self.log.error( 'XML Summary object could not be found (not produced?), skipping the report' )
          return S_OK()

      self._resolveInputVariables()

      dataDict = {'JobGroup': str( self.production_id ),
                  'RunNumber': self.runNumber,
                  'EventType': self.eventType,
                  'ProcessingType': self.stepProcPass, #this is the processing pass of the step
                  'ProcessingStep': self.BKstepID, #the step ID
                  'Site': DIRAC.siteName(),
                  'FinalStepState': self.stepStat,

                  'CPUTime': self.CPUTime,
                  'NormCPUTime': self.normCPUTime,
                  'ExecTime': self.execTime,
                  'InputData': sum( xf_o.inputFileStats.values() ),
                  'OutputData': sum( xf_o.outputFileStats.values() ),
                  'InputEvents': xf_o.inputEventsTotal,
                  'OutputEvents': xf_o.outputEventsTotal
                  }

      jobStep.setValuesFromDict( dataDict )

      if not self._enableModule():
        self.log.info( 'Not enabled, would have accounted for %s' % dataDict )
        return S_OK()

      result = jobStep.commit()
      if not result['OK']:
        # Failover request
        if self.workflow_commons.has_key( 'Request' ):
          from DIRAC.RequestManagementSystem.Client.DISETSubRequest import DISETSubRequest
          request = self.workflow_commons['Request']
          request.addSubRequest( DISETSubRequest( result['rpStub'] ).getDictionary(), 'accounting' )

      return S_OK()

    except Exception, e:
      self.log.exception( e )
      return S_ERROR( e )

    finally:
      super( StepAccounting, self ).finalize( self.version )

  ########################################################################

  def _resolveInputVariables( self ):
    """ By convention all workflow parameters are resolved here.
    """

    super( StepAccounting, self )._resolveInputVariables()

    if self.stepStatus['OK']:
      self.stepStat = 'Done'
    else:
      self.stepStat = 'Failed'

    self.BKstepID = self.step_commons['BKStepID']
    self.stepProcPass = self.step_commons['StepProcPass']

    self.runNumber = 'Unknown'
    if self.workflow_commons.has_key( 'runNumber' ):
      self.runNumber = self.workflow_commons['runNumber']

    self.eventType = 'Unknown'
    if self.step_commons.has_key( 'eventType' ):
      self.eventType = self.step_commons['eventType']

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

    self.CPUTime = cputime
    self.normCPUTime = normcpu
    self.execTime = exectime

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
