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
from LHCbDIRAC.ProductionManagementSystem.Client.ProductionClient import ProductionClient

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
    self.prodClient = ProductionClient()
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
    wmsClient = RPCClient('WorkloadManagement/JobMonitoring',timeout=120)
    result = self.prodClient.getTransformations()
    if not result['OK']:
      return result

    if not result['Value']:
      return S_OK()

    for prod in result['Value']:
      production = int(prod['TransformationID'])
      prodStatus = prod['Status']
      if not prodStatus in ['Active','Stopped']:
        continue
      time_stamp_older = str(datetime.datetime.utcnow() - datetime.timedelta(hours=1))
      time_stamp_newer = str(datetime.datetime.utcnow() - datetime.timedelta(days=7))
      # Get Reserved jobs - 100 at a time
      condDict = {"TransformationID":production,"WmsStatus":'Reserved'}
      res = self.prodClient.getTransformationTasks(condDict=condDict,older=time_stamp_older,newer=time_stamp_newer, timeStamp='LastUpdateTime', limit=100)
      if not res['OK']:
        return res
      if not res['Records']:
        continue
      jobNameList = []
      jobIDs = []
      for tuple in res['Records']:  # tuple = prodJobID,prodID,wmsStatus,wmsID,targetSE,creationTime,lastUpdateTime
        jobID = tuple[0]
        jobIDs.append(jobID)
        jobNameList.append(str(production).zfill(8)+'_'+str(jobID).zfill(8))
      if jobNameList:
        result = wmsClient.getJobs({'JobName':jobNameList})
        if result['OK']:
          if result['Value']:
            # Some jobs have been submitted, let's get their status one by one
            for jobWmsID in result['Value']:
              result = wmsClient.getJobPrimarySummary(int(jobWmsID))
              if result['OK']:
                status = result['Value']['Status']
                jobName = result['Value']['JobName']
                jobID = int(jobName.split('_')[1])
                self.log.info('Restoring status for job %s of production %s from Reserved to %s/%s' % (jobID,production,status,jobWmsID))
                result = self.prodClient.setTaskStatusAndWmsID(long(production),long(jobID),status,jobWmsID)  
                if not result['OK']:
                  self.log.warn(result['Message'])
          else:
            # The jobs were not submitted
            for jobID in jobIDs:
              self.log.info('Resetting status for job %s of production %s from Reserved to %s' % (jobID,production,'Created'))
              result = self.prodClient.setTaskStatus(long(production),long(jobID),'Created')  
              if not result['OK']:
                self.log.warn(result['Message'])
        else:
          continue       
    
    return S_OK()

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
