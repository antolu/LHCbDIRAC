########################################################################
# $HeadURL$
########################################################################

"""  The Transformation Agent prepares production jobs for processing data
     according to transformation definitions in the Production database.
"""

__RCSID__ = "$Id$"

from DIRAC                                            import S_OK, S_ERROR, gConfig, gLogger, gMonitor
from DIRAC.Core.DISET.RPCClient                       import RPCClient
from DIRAC.Core.Base.AgentModule                      import AgentModule

from LHCbDIRAC.ProductionManagementSystem.DB.ProductionDB import ProductionDB


import os, time


AGENT_NAME = 'ProductionManagement/ProductionUpdateAgent'
UPDATE_STATUS = ['Created','Submitted','Received','Checking','Staging','Waiting','Matched','Running','Stalled']
#UPDATE_STATUS = []
WAITING_STATUS = ['Submitted','Received','Checking','Staging','Waiting']
RUNNING_STATUS = ['Running','Stalled']
FINAL_STATUS = ['Done','Failed','Completed']
# Update job statuses no more frequently than NEWER minutes
NEWER = 2

class ProductionUpdateAgent(AgentModule):

  #############################################################################
  def initialize(self):
    """ Make the necessary initilizations
    """
    self.pollingTime = self.am_getOption('PollingTime',120)
    self.prodDB = ProductionDB()
    gMonitor.registerActivity("Iteration","Agent Loops",self.name,"Loops/min",gMonitor.OP_SUM)
    return result

  ##############################################################################
  def execute(self):
    """ Main execution method
    """
    dataLog = RPCClient('DataManagement/DataLogging')

    result = self.prodDB.getTransformations()
    for transDict in result['Value']:
      transID = long(transDict['TransID'])

      # Get the jobs which status is to be updated
      result = self.prodDB.selectWMSJobs(transID,UPDATE_STATUS,NEWER)
      if not result['OK']:
        gLogger.warn('Failed to get jobs from Production Database: '+str(result['Message']))
        continue
      jobDict = result['Value']
      if not jobDict:
        continue
      jobIDs = jobDict.keys()
      # Get the job statuses from WMS
      jobSvc = RPCClient('WorkloadManagement/JobMonitoring')
      result = jobSvc.getJobsStatus(jobIDs)
      if not result['OK']:
        gLogger.warn('Failed to get job status from the WMS system')
        continue

      statusDict = result['Value']
      updateDict = {}
      for jobWMS in jobIDs:
        jobID = jobDict[jobWMS][0]
        old_status = jobDict[jobWMS][1]
        if jobWMS in statusDict.keys():
          status = statusDict[jobWMS]['Status']    
        else:
          status = "Removed"   
           
        if old_status != status:
          if status == "Removed":
            gLogger.verbose('Production/Job %d/%d removed from WMS while it is in %s status' % (transID,jobID,old_status))
            gLogger.verbose('Setting Production/Job job to Failed')
            #result = self.prodDB.setTaskStatus(transID,jobID,'Failed')            
            #if not result['OK']:
            #  gLogger.warn('Failed to set job status for jobID: '+str(jobID))
            if not updateDict.has_key('Failed'):
              updateDict['Failed'] = []
            updateDict['Failed'].append(jobID)    
          else:
            gLogger.verbose('Setting job status for Production/Job %d/%d to %s' % (transID,jobID,status))
            #result = self.prodDB.setTaskStatus(transID,jobID,status)
            #if not result['OK']:
            #  gLogger.warn('Failed to set job status for jobID: '+str(jobID))
            if not updateDict.has_key(status):
              updateDict[status] = []
            updateDict[status].append(jobID) 
            
      for status in updateDict:
        gLogger.verbose('Setting status %s for Production %d for jobs %s' % (status,transID,str(updateDict[status])))
        result = self.prodDB.setTaskStatus(transID,updateDict[status],status)            
        if not result['OK']:
          gLogger.warn('Failed to set job status %s for jobs: %s' % (status,str(updateDict[status])))        
            
#        if old_status == "Submitted" and status in WAITING_STATUS:
#          result = self.prodDB.getJobInfo(transID,jobID)
#          if not result['OK']:
#            gLogger.warn('Failed to get job info for production %d job %d' % (int(transID),int(jobID)))    
#        elif old_status in WAITING_STATUS and status in RUNNING_STATUS:
#          result = self.prodDB.getJobInfo(transID,jobID)
#          if not result['OK']:
#            gLogger.warn('Failed to get job info for production %d job %d' % (int(transID),int(jobID)))
#        elif old_status in RUNNING_STATUS and status in FINAL_STATUS:
#          result = self.prodDB.getJobInfo(transID,jobID)
#          if not result['OK']:
#            gLogger.warn('Failed to get job info for production %d job %d' % (int(transID),int(jobID)))

    return S_OK()
