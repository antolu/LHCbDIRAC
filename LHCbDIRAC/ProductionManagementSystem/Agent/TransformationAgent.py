########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/ProductionManagementSystem/Agent/TransformationAgent.py,v 1.12 2008/06/20 05:26:54 rgracian Exp $
########################################################################

"""  The Transformation Agent prepares production jobs for processing data
     according to transformation definitions in the Production database.
"""

__RCSID__ = "$Id: TransformationAgent.py,v 1.12 2008/06/20 05:26:54 rgracian Exp $"

from DIRAC.Core.Base.Agent      import Agent
from DIRAC                      import S_OK, S_ERROR, gConfig, gLogger, gMonitor
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.DataManagementSystem.Client.Catalog.LcgFileCatalogCombinedClient import LcgFileCatalogCombinedClient
import os, time, random

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
    self.checkLFC = gConfig.getValue(self.section+'/CheckLFCFlag','no')
    #self.checkLFC = 'yes'
    self.dataLog = RPCClient('DataManagement/DataLogging')
    self.server = RPCClient('ProductionManagement/ProductionManager')
    self.lfc = LcgFileCatalogCombinedClient()
    gMonitor.registerActivity("Iteration","Agent Loops",self.name,"Loops/min",gMonitor.OP_SUM)
    self.CERNShare = 0.144
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
      gLogger.info("TransformationAgent.execute: Initializing general purpose agent.")

    result = self.server.getAllProductions()
    activeTransforms = []
    if not result['OK']:
      gLogger.error("TransformationAgent.execute: Failed to get transformations.", result['Message'])

    for transDict in result['Value']:
      transID = long(transDict['TransID'])
      transStatus = transDict['Status']

      processTransformation = True
      if self.singleTransformation:
        if not self.singleTransformation == transID:
          gLogger.verbose("TransformationAgent.execute: Skipping %s (not selected)." % transID)
          processTransformation = False

      if processTransformation:
        startTime = time.time()
        # process the active transformations
        if transStatus == 'Active':
          gLogger.info(self.name+".execute: Processing transformation '%s'." % transID)
          result = self.processTransformation(transDict, False)
          if result['OK']:
            nJobs = result['Value']
            gLogger.info(self.name+".execute: Transformation '%s' processed in %s seconds." % (transID,time.time()-startTime))
            if nJobs > 0:
              gLogger.info('%d job(s) generated' % nJobs)
          else:
            gLogger.warn('Error while processing: '+result['Message'])    

        # flush transformations
        elif transStatus == 'Flush':
          gLogger.info(self.name+".execute: Flushing transformation '%s'." % transID)
          result = self.processTransformation(transDict, True)
          if not result['OK']:
            gLogger.error(self.name+".execute: Failed to flush transformation '%s'." % transID, res['Message'])
          else:
            gLogger.info(self.name+".execute: Transformation '%s' flushed in %s seconds." % (transID,time.time()-startTime))
            nJobs = result['Value']
            if nJobs > 0:
              gLogger.info('%d job(s) generated' % nJobs)
            result = self.server.setTransformationStatus(transID, 'Stopped')
            if not result['OK']:
              gLogger.error(self.name+".execute: Failed to update transformation status to 'Stopped'.", res['Message'])
            else:
              gLogger.info(self.name+".execute: Updated transformation status to 'Stopped'.")

        # skip the transformations of other statuses
        else:
          gLogger.verbose("TransformationAgent.execute: Skipping transformation '%s' with status %s." % (transID,transStatus))

    return S_OK()

  #############################################################################
  def processTransformation(self, transDict, flush=False):
    """ Process one Transformation defined by its dictionary. Jobs are generated
        using various plugin functions of the kind generateJob_<plugin_name>.
        The plugin name is defined in the Production parameters. If not defined,
        'Standard' plugin is used. 
    """

    available_plugins = ['CCRC_RAW','Standard']

    prodID = long(transDict['TransID'])
    prodName = transDict['Name']
    group_size = int(transDict['GroupSize'])
    plugin = transDict['Plugin']
    if not plugin in available_plugins:
      plugin = 'Standard' 
    result = self.server.getInputData(prodName,'')
    sflag = True #WARNING KGG this is possibly an error
    if result['OK']:
      data = result['Value']
    else:
      print "Failed to get data for transformation", prodID, result['Message']
      return S_ERROR("Failed to get data for transformation %d %s"%(prodID,result['Message']))

    if self.checkLFC == 'yes' and data:
      result = self.checkDataWithLFC(data)     

    gLogger.debug("Input data number of files %d" % len(data))

    nJobs = 0

    if flush:
      while len(data) >0:
        ldata = len(data)
        data = eval('self.generateJob_'+plugin+'(data,prodID,sflag,group_size, flush)')
        #if plugin == 'CCRC_RAW':
        #  data = self.generateJob_CCRC_RAW(data,prodID,sflag,group_size, flush)
        #else:  
        #  data = self.generateJob(data,prodID,sflag,group_size, flush)
        if ldata == len(data):
          break
        else:
          nJobs += 1  
    else:
      while len(data) >= group_size:
        ldata = len(data)
        data = eval('self.generateJob_'+plugin+'(data,prodID,sflag,group_size, flush)')
        #if plugin == 'CCRC_RAW':
        #  data = self.generateJob_CCRC_RAW(data,prodID,sflag,group_size, flush)
        #else:  
        #  data = self.generateJob(data,prodID,sflag,group_size, flush)
        if ldata == len(data):
          break
        else:
          nJobs += 1  

    return S_OK(nJobs)
  
  #####################################################################################  
  def generateJob_CCRC_RAW(self,data,production,sflag,group_size,flush=False):  
    """ Generate a job according to the CCRC 2008 site shares 
    """
    
    # Sort files by LFN
    datadict = {}
    for lfn,se in data:
      if not datadict.has_key(lfn):
        datadict[lfn] = []
      datadict[lfn].append(se)
      
    data_m = data  
      
    lse = ''  
    selectedLFN = ''
    for lfn,seList in datadict.items():
      if len(seList) == 1:
        if seList[0].find('CERN') == -1:
          gLogger.warn('Single replica of %s not at CERN: %s' % (lfn,seList[0]))
          print lfn,seList
        continue
        
      if len(seList) > 1:
        # Check that CERN se is in the list
        okCERN = False
        seCERN = ''
        seOther = ''
        for se in seList:
          if se.find('CERN') != -1:
            seCERN = se
            okCERN = True
          else:
            seOther = se  
            
        if okCERN:     
          # Try to satisfy the CERN share
          if random.random() < self.CERNShare:
            lse = seCERN
          else:
            lse = seOther
          selectedLFN = lfn  
          break
        else:
          gLogger.warn('No replicas of %s at CERN' % lfn)
          continue
      
    if not lse and flush:
      # Send job to where it can go
      for lfn,seList in datadict.items():
        selectedLFN = lfn
        lse = seList[0]  
      
    if lse:
      lfns = [selectedLFN]
      result = self.addJobToProduction(production,lfns,lse)
      if result['OK']:
        jobID = long(result['Value'])
        if jobID:
          result = self.server.setFileStatusForTransformation(production,[('Assigned',lfns)])
          if not result['OK']:
            gLogger.error("Failed to update file status for production %d"%production)

          result = self.server.setFileJobID(production,jobID,lfns)
          if not result['OK']:
            gLogger.error("Failed to set file job ID for production %d"%production)

          result = self.server.setFileSEForTransformation(production,lse,lfns)
          if not result['OK']:
            gLogger.error("Failed to set SE for production %d"%production)
          for lfn in lfns:
            result = self.dataLog.addFileRecord(lfn,'Job created','JobID: %s' % jobID,'','TransformationAgent')  

        # Remove used files from the initial list
        data_m = []
        for lfn,se in data:
          if lfn not in lfns:
            data_m.append((lfn,se))
      else:
        gLogger.warn("Failed to add a new job to repository: "+result['Message'])
    else:
      gLogger.warn('No eligible LFNs for production %d'%production)  
             
    return data_m              

  #####################################################################################
  def generateJob_Standard(self,data,production,sflag,group_size,flush=False):
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
            result = self.server.setFileStatusForTransformation(production,[('Assigned',lfns)])
            if not result['OK']:
              gLogger.error("Failed to update file status for production %d"%production)

            result = self.server.setFileJobID(production,jobID,lfns)
            if not result['OK']:
              gLogger.error("Failed to set file job ID for production %d"%production)

            result = self.server.setFileSEForTransformation(production,lse,lfns)
            if not result['OK']:
              gLogger.error("Failed to set SE for production %d"%production)
            for lfn in lfns:
              result = self.dataLog.addFileRecord(lfn,'Job created','ProdID: %s JobID: %s' % (production,jobID),'','TransformationAgent')  

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

  ######################################################################################
  def checkDataWithLFC(self,data):
    """ Check the lfns and replicas with the LFC catalog
    """
    
    # Sort files by LFN
    datadict = {}
    for lfn,se in data:
      print "AT >>>>>>",lfn,se
      if not datadict.has_key(lfn):
        datadict[lfn] = []
      datadict[lfn].append(se)
      
    lfns = datadict.keys()
    result = self.lfc.getReplicas(lfns)
    if result['OK']:
      print result
      
