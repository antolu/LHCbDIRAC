########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/ProductionManagementSystem/Agent/TransformationAgent.py,v 1.5 2008/02/19 14:21:02 gkuznets Exp $
########################################################################

"""  The Transformation Agent prepares production jobs for processing data
     according to transformation definitions in the Production database.
"""

__RCSID__ = "$Id: TransformationAgent.py,v 1.5 2008/02/19 14:21:02 gkuznets Exp $"

from DIRAC.Core.Base.Agent    import Agent
from DIRAC                    import S_OK, S_ERROR, gConfig, gLogger, gMonitor
from DIRAC.Core.DISET.RPCClient import RPCClient
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
    self.dataLog = RPCClient('DataManagement/DataLogging')
    self.server = RPCClient('ProductionManagement/ProductionManager')
    gMonitor.registerActivity("Iteration","Agent Loops",self.name,"Loops/min",gMonitor.OP_SUM)
    return result

  ##############################################################################
  def execute(self):
    """Main execution method
    """

    gMonitor.addMark('Iteration',1)

    transID = gConfig.getValue(self.section+'/Transformation','')
    if transID:
      self.singleTransformation = long(transID)
      gLogger.info("Initializing Replication Agent for transformation %s." % transID)
    else:
      self.singleTransformation = False
      gLogger.info("ReplicationPlacementAgent.execute: Initializing general purpose agent.")

    result = self.server.getAllProductions()
    activeTransforms = []
    if not result['OK']:
      gLogger.error("ReplicationPlacementAgent.execute: Failed to get transformations.", res['Message'])

    for transDict in result['Value']:
      transID = long(transDict['TransID'])
      transStatus = transDict['Status']

      processTransformation = True
      if self.singleTransformation:
        if not self.singleTransformation == transID:
          gLogger.verbose("ReplicationPlacementAgent.execute: Skipping %s (not selected)." % transID)
          processTransformation = False

      if processTransformation:
        startTime = time.time()
        # process the active transformations
        if transStatus == 'Active':
          gLogger.info(self.name+".execute: Processing transformation '%s'." % transID)
          result = self.processTransformation(transDict, False)
          gLogger.info(self.name+".execute: Transformation '%s' processed in %s seconds." % (transID,time.time()-startTime))

        # flush transformations
        elif transStatus == 'Flush':
          gLogger.info(self.name+".execute: Flushing transformation '%s'." % transID)
          result = self.processTransformation(transDict, True)
          if not result['OK']:
            gLogger.error(self.name+".execute: Failed to flush transformation '%s'." % transID, res['Message'])
          else:
            gLogger.info(self.name+".execute: Transformation '%s' flushed in %s seconds." % (transID,time.time()-startTime))
            result = self.server.setTransformationStatus(transID, 'Stopped')
            if not result['OK']:
              gLogger.error(self.name+".execute: Failed to update transformation status to 'Stopped'.", res['Message'])
            else:
              gLogger.info(self.name+".execute: Updated transformation status to 'Stopped'.")

        # skip the transformations of other statuses
        else:
          gLogger.verbose("ReplicationPlacementAgent.execute: Skipping transformation '%s' with status %s." % (transID,transStatus))

    return S_OK()

  #############################################################################
  def processTransformation(self, transDict, flush=False):
    """Process one Transformation defined by its dictionary
    """

    prodID = long(transDict['TransID'])
    group_size = int(transDict['GroupSize'])
    result = self.server.getInputData2(prodID,'')
    sflag = True #WARNING KGG this is possibly an error
    if result['OK']:
      data = result['Value']
    else:
      print "Failed to get data for transformation", prodID, result['Message']
      return S_ERROR("Failed to get data for transformation %d %s"%(prodID,result['Message']))

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
        data = self.generateJob(data,prodID,sflag,group_size, flush)
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
        if result['OK']:
          jobID = long(result['Value'])
          if jobID:
            result = self.server.setFileStatusForTransformation(production,'Assigned',lfns)
            if not result['OK']:
              gLogger.error("Failed to update file status for production %d"%production)

            result = self.server.setFileJobID(production,jobID,lfns)
            if not result['OK']:
              gLogger.error("Failed to set file job ID for production %d"%production)

            result = self.server.setFileSEForTransformation(production,lse,lfns)
            if not result['OK']:
              gLogger.error("Failed to set SE for production %d"%production)
            for lfn in lfns:
              result = self.dataLog(lfn,'Job created',minor='JobID: %s' % JobID,
                                    source='TransformationAgent')  

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
  def addJobToProduction(self, prodID, lfns, se):
    """ Adds a new job to the production giving an lfns list of input files.
        Argument se can be used to specify the target destination if necessary
    """

    #inputVector = {}
    #inputVector['InputData'] = lfns
    #lfns is the list!! we have to convert it into string with proper separator
    vector =""
    for lfn in lfns:
      vector = vector + 'LFN:'+lfn+';'
    #removing last ';'
    vector = vector.rstrip(';')
      
    result = self.server.addProductionJob(prodID, vector, se)
    return result
