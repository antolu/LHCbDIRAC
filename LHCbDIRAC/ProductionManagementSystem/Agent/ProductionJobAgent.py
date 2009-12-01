########################################################################
# $HeadURL$
########################################################################

"""  The Production Job Agent automatically submits production jobs after
     they have been created.  This interacts with WMS Admin interface to
     setup the current shift production manager credential and uses the
     Dirac Production interface to submit the jobs.
"""

__RCSID__ = "$Id$"

from DIRAC.Core.Base.AgentModule                          import AgentModule
from DIRAC.Core.DISET.RPCClient                           import RPCClient
from DIRAC.Core.Utilities.Shifter                         import setupShifterProxyInEnv

from LHCbDIRAC.Interfaces.API.DiracProduction             import DiracProduction

from DIRAC                                                import S_OK, S_ERROR, gConfig, gMonitor

import os, time, string, datetime

AGENT_NAME = 'ProductionManagement/ProductionJobAgent'

class ProductionJobAgent(AgentModule):

  #############################################################################
  def initialize(self):
    """Sets defaults
    """
    self.check_reserved = self.am_getOption( 'CheckReservedJobs', 0 )
    self.last_reserved_check = None
    self.pollingTime = self.am_getOption('PollingTime',120)
    self.jobsToSubmitPerProduction = self.am_getOption('JobsToSubmitPerProduction',50)
    self.productionStatus = self.am_getOption('SubmitStatus','automatic')
    self.enableFlag = None
    gMonitor.registerActivity("SubmittedJobs","Automatically submitted jobs","Production Monitoring","Jobs", gMonitor.OP_ACUM)
    return S_OK()

  #############################################################################
  def execute(self):
    """The ProductionJobAgent execution method.
    """
    self.enableFlag = self.am_getOption('EnableFlag','True')
    if not self.enableFlag == 'True':
      self.log.info('ProductionJobAgent is disabled by configuration option %s/EnableFlag' %(self.section))
      return S_OK('Disabled via CS flag')

    result = setupShifterProxyInEnv( "ProductionManager" )
    if not result[ 'OK' ]:
      return S_ERROR( "Can't get shifter's proxy: %s" % result[ 'Message' ] )

    diracProd=DiracProduction()
    result = diracProd.getActiveProductions()
    self.log.verbose(result)
    if not result['OK']:
      self.log.warn('Failed to get list of active productions')
      self.log.warn(result)
      return result
    activeProductions = result['Value']
    if not activeProductions:
      self.log.info('No active productions discovered, no work to do')
      return S_OK('Loop completed')

    for production,status in activeProductions.items():
      if status.lower()==self.productionStatus:
        self.log.info('Attempting to submit %s jobs for production %s' %(self.jobsToSubmitPerProduction,production))
        start = time.time()
        result = diracProd.submitProduction(production,self.jobsToSubmitPerProduction)
        timing = time.time() - start
        if not result['OK']:
          self.log.warn(result['Message'])
        else:
          jobsDict = result['Value']
          if jobsDict.has_key('Successful'):
            jobsList = jobsDict['Successful']
            submittedJobs = len(jobsList)
            gMonitor.addMark("SubmittedJobs",int(submittedJobs))
            if submittedJobs:
              self.log.info('Production %s submission time: %.2f seconds for %s jobs' % (production,timing,submittedJobs))
            else:
              self.log.info('No jobs to submit for production %s' %(production))
      else:
        self.log.verbose('Nothing to do for productionID %s with status %s' %(production,status))

    if self.check_reserved:
      if not self.last_reserved_check or \
        (datetime.datetime.utcnow()-self.last_reserved_check)>datetime.timedelta(hours=1):
        result = self.checkReservedJobs()
        if not result['OK']:
          self.log.warn('Failed to chceck Reserved job: %s' % result['Message'])
        else:
          if not result['Value']:
            self.last_reserved_check = datetime.datetime.utcnow()

    return S_OK('Productions submitted')
    
  def checkReservedJobs(self):
    """ Check if there are jobs in Reserved state for more than an hour,
        verify that there were not submitted and reset them to Created
    """  
    
    self.log.info("Checking Reserved jobs")    
    prodClient = RPCClient('ProductionManagement/ProductionManager',timeout=120)
    wmsClient = RPCClient('WorkloadManagement/JobMonitoring',timeout=120)
    result = prodClient.getAllProductions()
    if not result['OK']:
      return result

    if not result['Value']:
      return S_OK()

    for prod in result['Value']:
      production = int(prod['TransID'])
      prodStatus = prod['Status']
      if not prodStatus in ['Active','Stopped']:
        continue
      time_stamp_older = str(datetime.datetime.utcnow() - datetime.timedelta(hours=1))
      time_stamp_newer = str(datetime.datetime.utcnow() - datetime.timedelta(days=7))
      # Get Reserved jobs - 100 at a time
      result = prodClient.selectJobs(production,'Reserved',250,'',time_stamp_older,time_stamp_newer)
      if not result['OK']:
        continue
      jobNameList = []
      jobIDs = result['Value'].keys()
      for jobID in jobIDs:
        jobNameList.append(str(production).zfill(8)+'_'+str(jobID).zfill(8))
      if jobNameList:
        result = wmsClient.getJobs({'JobName':jobNameList})
        print result
        if result['OK']:
          if result['Value']:
            # Some jobs have been submitted, let's get their status one by one
            for jobWmsID in result['Value']:
              result = wmsClient.getJobPrimarySummary(int(jobWmsID))
              print result
              if result['OK']:
                status = result['Value']['Status']
                jobName = result['Value']['JobName']
                jobID = int(jobName.split('_')[1])
                self.log.info('Restoring status for job %s of production %s from Reserved to %s/%s' % (jobID,production,status,jobWmsID))
                result = prodClient.setJobStatusAndWmsID(long(production),long(jobID),status,jobWmsID)  
                #print "AT >>>> setting job status",long(production),long(jobID),status,jobWmsID
                #result = S_OK()
                if not result['OK']:
                  self.log.warn(result['Message'])
          else:
            # The jobs were not submitted
            for jobID in jobIDs:
              self.log.info('Resetting status for job %s of production %s from Reserved to %s' % (jobID,production,'Created'))
              result = prodClient.setJobStatus(long(production),long(jobID),'Created')  
              #print "AT >>>> resetting job status",long(production),long(jobID),'Created'
              #result = S_OK()
              if not result['OK']:
                self.log.warn(result['Message'])
        else:
          continue       
    
    return S_OK()

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
