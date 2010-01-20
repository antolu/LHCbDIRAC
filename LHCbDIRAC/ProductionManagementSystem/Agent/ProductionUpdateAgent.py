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
from DIRAC.Core.Utilities.List                        import sortList
from LHCbDIRAC.ProductionManagementSystem.DB.ProductionDB import ProductionDB

import os, time,datetime

AGENT_NAME = 'ProductionManagement/ProductionUpdateAgent'
UPDATE_STATUS = ['Created','Submitted','Received','Checking','Staging','Waiting','Matched','Running','Stalled']
WAITING_STATUS = ['Submitted','Received','Checking','Staging','Waiting']
RUNNING_STATUS = ['Running','Stalled']
FINAL_STATUS = ['Done','Failed','Completed']
# Update job statuses no more frequently than NEWER minutes
NEWER = 10

class ProductionUpdateAgent(AgentModule):

  #############################################################################
  def initialize(self):
    """ Make the necessary initilizations
    """
    self.pollingTime = self.am_getOption('PollingTime',120)
    self.transformationTypes = self.am_getOption('TransformationTypes',['DataReconstruction','DataStripping','MCStripping','Merge'])
    self.prodDB = ProductionDB()
    return S_OK()

  ##############################################################################
  def execute(self):
    """ Main execution method
    """
    result = self.prodDB.getTransformations()
    for transDict in result['Value']:
      transID = long(transDict['TransformationID'])
      type = transDict['Type']
      if type not in self.transformationTypes:
        continue
      gLogger.info("Attempting to update transformation %d" % transID)
      time_stamp = str(datetime.datetime.utcnow() - datetime.timedelta(minutes=NEWER))
      res = self.prodDB.getTransformationTasks(condDict={'TransformationID':transID,'WmsStatus':UPDATE_STATUS},timeStamp='LastUpdateTime',older=time_stamp )
      if not res['OK']:
        gLogger.error("Failed to get WMS job IDs for production %d" % prodID,res['Message'])
        return res
      jobDictList = res['Value']
      gLogger.info("Selected %d tasks to be updated" % len(jobDictList))
      if not jobDictList:
        continue
      jobIDs = []
      for jobDict in jobDictList:
        wmsID = jobDict['JobWmsID']
        if wmsID:
          jobIDs.append(wmsID)
      # Get the job statuses from WMS
      jobSvc = RPCClient('WorkloadManagement/JobMonitoring')
      result = jobSvc.getJobsStatus(jobIDs)
      if not result['OK']:
        gLogger.warn('Failed to get job status from the WMS system')
        continue
      statusDict = result['Value']

      updateDict = {}
      for jobDict in jobDictList:
        jobWMS = int(jobDict['JobWmsID'])
        if not jobWMS:
          continue
        jobID = jobDict['JobID']
        old_status = jobDict['WmsStatus']
        status = "Removed"
        if jobWMS in statusDict.keys():
          status = statusDict[jobWMS]['Status']
        if old_status != status:
          if status == "Removed":
            gLogger.verbose('Production/Job %d/%d removed from WMS while it is in %s status' % (transID,jobID,old_status))
            status = "Failed"
          gLogger.verbose('Setting job status for Production/Job %d/%d to %s' % (transID,jobID,status))
          if not updateDict.has_key(status):
            updateDict[status] = []
          updateDict[status].append(jobID) 
            
      for status,taskIDs in updateDict.items():
        gLogger.info('Setting status %s for Production %d for %d jobs' % (status,transID,len(taskIDs)))
        result = self.prodDB.setTaskStatus(transID,taskIDs,status)            
        if not result['OK']:
          gLogger.warn('Failed to set job status %s for jobs: %s' % (status,str(taskIDs)))        
    return S_OK()
