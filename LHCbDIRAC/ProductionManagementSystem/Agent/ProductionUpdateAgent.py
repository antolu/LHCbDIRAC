########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/ProductionManagementSystem/Agent/ProductionUpdateAgent.py,v 1.2 2008/02/14 23:26:35 atsareg Exp $
########################################################################

"""  The Transformation Agent prepares production jobs for processing data
     according to transformation definitions in the Production database.
"""

__RCSID__ = "$Id: ProductionUpdateAgent.py,v 1.2 2008/02/14 23:26:35 atsareg Exp $"

from DIRAC.Core.Base.Agent    import Agent
from DIRAC                    import S_OK, S_ERROR, gConfig, gLogger, gMonitor
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.ProductionManagementSystem.DB.ProductionDB import ProductionDB
import os, time


AGENT_NAME = 'ProductionManagement/ProductionUpdateAgent'
UPDATE_STATUS = ['Created','Submitted','Waiting','Matched','Running']
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
    gMonitor.registerActivity("Iteration","Agent Loops",self.name,"Loops/min",gMonitor.OP_SUM)
    return result

  ##############################################################################
  def execute(self):
    """Main execution method
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
        status = statusDict[jobWMS]['Status']
        result = self.prodDB.setJobStatus(transID,jobID,status)
        if not result['OK']:
          gLogger.warn('Failed to set job status for jobID: '+str(jobID))

    return S_OK()
