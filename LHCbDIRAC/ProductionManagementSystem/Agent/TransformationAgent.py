########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/ProductionManagementSystem/Agent/TransformationAgent.py,v 1.1 2008/02/08 10:05:40 atsareg Exp $
########################################################################

"""  The Transformation Agent prepares production jobs for processing data
     according to transformation definitions in the Production database.
"""

__RCSID__ = "$Id: TransformationAgent.py,v 1.1 2008/02/08 10:05:40 atsareg Exp $"

from DIRAC.Core.Base.Agent    import Agent
from DIRAC                    import S_OK, S_ERROR, gConfig, gLogger, gMonitor
import os, time

AGENT_NAME = 'ProductionManagement/TransformationAgent'

class TransformationAgent(Agent):

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

    self.server = RPCClient('ProductionManagement/ProductionManager')
    gMonitor.registerActivity("Iteration","Agent Loops",self.name,"Loops/min",gMonitor.OP_SUM)
    return result

  ##############################################################################
  def execute(self):
    """Main execution method
    """

    gMonitor.addMark('Iteration',1)

    transName = gConfig.getValue(self.section+'/Transformation')
    if transName:
      self.singleTransformation = transName
      gLogger.info("Initializing Replication Agent for transformation %s." % transName)
    else:
      self.singleTransformation = False
      gLogger.info("ReplicationPlacementAgent.execute: Initializing general purpose agent.")

    result = self.server.getAllTransformations()
    activeTransforms = []
    if not result['OK']:
      gLogger.error("ReplicationPlacementAgent.execute: Failed to get transformations.", res['Message'])

    for transDict in result['Value']:
      transName = transDict['Name']
      transStatus = transDict['Status']

      processTransformation = True
      if self.singleTransformation:
        if not self.singleTransformation == transName:
          gLogger.verbose("ReplicationPlacementAgent.execute: Skipping %s (not selected)." % transName)
          processTransformation = False

      if processTransformation:
        startTime = time.time()
        # process the active transformations
        if transStatus == 'Active':
          gLogger.info(self.name+".execute: Processing transformation '%s'." % transName)
          result = self.processTransformation(transDict, False)
          gLogger.info(self.name+".execute: Transformation '%s' processed in %s seconds." % (transName,time.time()-startTime))

        # flush transformations
        elif transStatus == 'Flush':
          gLogger.info(self.name+".execute: Flushing transformation '%s'." % transName)
          result = self.processTransformation(transDict, True)
          if not result['OK']:
            gLogger.error(self.name+".execute: Failed to flush transformation '%s'." % transName, res['Message'])
          else:
            gLogger.info(self.name+".execute: Transformation '%s' flushed in %s seconds." % (transName,time.time()-startTime))
            result = self.server.setTransformationStatus(transName, 'Stopped')
            if not result['OK']:
              gLogger.error(self.name+".execute: Failed to update transformation status to 'Stopped'.", res['Message'])
            else:
              gLogger.info(self.name+".execute: Updated transformation status to 'Stopped'.")

        # skip the transformations of other statuses
        else:
          gLogger.verbose("ReplicationPlacementAgent.execute: Skipping transformation '%s' with status %s." % (transName,transStatus))

    return S_OK()

  #############################################################################
  def processTransformation(self, transDict, flush=False):
    """Process one Transformation defined by its dictionary
    """

    prodID = transDict['name']
    result = self.prodSvc.getInputData(prodID,'')
    if result['Status'] == "OK":
      data = result['Value']
    else:
      print "Failed to get data for transformation",prodID
      return S_ERROR("Failed to get data for transformation "+prodID)

    gLogger.debug("Input data number of files %d" % len(data))

    if flush:
      while len(data) >0:
        ldata = len(data)
        data = self.generateJob(data,prodID,sflag,group_size, flush)
        if ldata == len(data):
          break
    else:
      while len(data) >= group_size:
        ldata = len(data)
        data = self.generateJob(data,prodID,transID,sname,sflag,group_size, flush)
        if ldata == len(data):
          break

    return S_OK()

  #####################################################################################
  def generateJob(self,data,production,sflag,group_size,flush=False):
    """ Generates a job based on the input data, adds job to the repository
        and returns a reduced list of the lfns that rest to be processed
        If flush is true, the group_size is not taken into account
    """
    # Sort files by SE
    datadict = {}
    for lfn,se in data:
      #if not se in self.seBlackList:
        if not datadict.has_key(se):
          datadict[se] = []
        datadict[se].append(lfn)

    data_m = data

    if sflag:
      # Group files by SE

      if flush: # flush mode
        # Find the SE with maximum sufficient amount of data
        lmax = 0
        lse = ''
        for se in datadict.keys():
          ldata = len(datadict[se])
          if ldata > lmax:
            lmax = ldata
            lse = se
        if lmax < group_size:
          group_size = lmax

      else: # normal  mode
        # Find the SE with the minimally sufficient amount of data
        lmin = len(data)+1
        lse = ''
        for se in datadict.keys():
          ldata = len(datadict[se])
          if ldata < lmin and ldata >= group_size:
            lmin = ldata
            lse = se

      if lse:
        # We have found a SE with minimally(or max in case of flush) sufficient amount of data
        lfns = datadict[lse][:group_size]
        result = self.addJobToProduction(production,lfns,lse)
        if result['Status'] == "OK":
          jobID = result['JobID']
          if jobID:
            result = self.server.setFileStatusForTransformation(production,'Assigned',lfns)
            if result['Status'] != "OK":
              gLogger.error("Failed to update file status for production "+production)

            result = self.server.setFileJobID(production,jobID,lfns)
            if result['Status'] != "OK":
              gLogger.error("Failed to set file job ID for production "+production)

            result = self.server.setFileSEForTransformation(production,lse,lfns)
            if result['Status'] != "OK":
              gLogger.error("Failed to set file job ID for production "+transID)

          # Remove used files from the initial list
          data_m = []
          for lfn,se in data:
            if lfn not in lfns:
              data_m.append((lfn,se))
        else:
          gLogger.warn("Failed to add a new job to repository: "+result['Message'])

      else:
        gLogger.verbose("Neither SE has enough input data")
    else:
      # Do not discriminate sites
      pass

    return data_m

  ######################################################################################
  def addJobToProduction(self,production,lfns,se):
    """ Adds a new job to the production giving an lfns list of input files.
        Argument se can be used to specify the target destination if necessary
    """

    inputVector = {}
    inputVector['InputData'] = lfns
    result = self.server.addJobToProduction(production,inputVector)
    return result