########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/ProductionManagementSystem/Agent/ProductionUpdateAgent.py,v 1.4 2008/03/02 12:57:20 atsareg Exp $
########################################################################

"""  The Transformation Agent prepares production jobs for processing data
     according to transformation definitions in the Production database.
"""

__RCSID__ = "$Id: ProductionUpdateAgent.py,v 1.4 2008/03/02 12:57:20 atsareg Exp $"

from DIRAC.Core.Base.Agent    import Agent
from DIRAC                    import S_OK, S_ERROR, gConfig, gLogger, gMonitor
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.ProductionManagementSystem.DB.ProductionDB import ProductionDB
import os, time


AGENT_NAME = 'ProductionManagement/ProductionUpdateAgent'
UPDATE_STATUS = ['Created','Submitted','Received','Checking','Waiting','Matched','Running','Stalled']
WAITING_STATUS = ['Submitted','Received','Checking','Waiting']
RUNNING_STATUS = ['Running','Completed']
FINAL_STATUS = ['Done','Failed']
# Update job statuses no more frequently than NEWER minutes
NEWER = 1

class ProductionUpdateAgent(Agent):

  #############################################################################
  def __init__(self):
    """ Standard constructor for Agent
    """
    Agent.__init__(self,AGENT_NAME)

  #############################################################################
  def initialize(self):
    """ Make the necessary initilizations
    """
    result = Agent.initialize(self)
    self.pollingTime = gConfig.getValue(self.section+'/PollingTime',120)

    self.jobSvc = RPCClient('WorkloadManagement/JobMonitoring')
    self.prodDB = ProductionDB()
    self.dataLog = RPCClient('DataManagement/DataLogging')
    gMonitor.registerActivity("Iteration","Agent Loops",self.name,"Loops/min",gMonitor.OP_SUM)
    return result

  ##############################################################################
  def execute(self):
    """ Main execution method
    """

    result = self.prodDB.getAllProductions()
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
      result = self.jobSvc.getJobsStatus(jobIDs)
      if not result['OK']:
        gLogger.warn('Failed to get job status from the WMS system')
        continue

      statusDict = result['Value']
      for jobWMS in jobIDs:
        jobID = jobDict[jobWMS][0]
        old_status = jobDict[jobWMS][1]
        status = statusDict[jobWMS]['Status']
        if old_status != status:
          result = self.prodDB.setJobStatus(transID,jobID,status)
          if not result['OK']:
            gLogger.warn('Failed to set job status for jobID: '+str(jobID))
        if old_status == "Submitted" and status in WAITING_STATUS:
          result = self.prodDB.getJobInfo(transID,jobID)
          if not result['OK']:
            gLogger.warn('Failed to get job info for production %d job %d' % (int(transID),int(jobID)))
          if result['Value'].has_key('InputData'):
            lfns = result['Value']['Input'].split(',')
            for lfn in lfns:
              result = self.dataLog.addFileRecord(lfn,'Job waiting','','','ProductionUpdate agent')    
        elif old_status in WAITING_STATUS and status in RUNNING_STATUS:
          result = self.prodDB.getJobInfo(transID,jobID)
          if not result['OK']:
            gLogger.warn('Failed to get job info for production %d job %d' % (int(transID),int(jobID)))
          if result['Value'].has_key('InputData'):
            lfns = result['Value']['Input'].split(',')
            for lfn in lfns:
              result = self.dataLog.addFileRecord(lfn,'Job running','','','ProductionUpdate agent')
        elif old_status in RUNNING_STATUS and status in FINAL_STATUS:
          result = self.prodDB.getJobInfo(transID,jobID)
          if not result['OK']:
            gLogger.warn('Failed to get job info for production %d job %d' % (int(transID),int(jobID)))
          if result['Value'].has_key('InputData'):
            lfns = result['Value']['Input'].split(',')
            for lfn in lfns:
              if status == "Done":
                result = self.dataLog.addFileRecord(lfn,'Job done','','','ProductionUpdate agent')
              elif status == "Failed":
                result = self.dataLog.addFileRecord(lfn,'Job failed','','','ProductionUpdate agent')
              elif status == "Stalled":
                result = self.dataLog.addFileRecord(lfn,'Job stalled','','','ProductionUpdate agent')
              else:
                gLogger.warn('Unknown status %s for job %d/%d' % (status,jobID,jobWMS))  

    return S_OK()
