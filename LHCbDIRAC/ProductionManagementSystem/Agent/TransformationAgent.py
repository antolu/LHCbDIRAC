########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/ProductionManagementSystem/Agent/TransformationAgent.py,v 1.21 2008/08/19 15:28:30 atsareg Exp $
########################################################################

"""  The Transformation Agent prepares production jobs for processing data
     according to transformation definitions in the Production database.
"""

__RCSID__ = "$Id: TransformationAgent.py,v 1.21 2008/08/19 15:28:30 atsareg Exp $"

from DIRAC.Core.Base.Agent      import Agent
from DIRAC                      import S_OK, S_ERROR, gConfig, gLogger, gMonitor
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.DataManagementSystem.Client.Catalog.LcgFileCatalogCombinedClient import LcgFileCatalogCombinedClient
from DIRAC.LHCbSystem.Utilities.AncestorFiles import getAncestorFiles
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
    self.checkLFC = gConfig.getValue(self.section+'/CheckLFCFlag','yes')
    self.lfc = LcgFileCatalogCombinedClient()
    self.shifterDN = gConfig.getValue('/Operations/Production/ShiftManager','')
    gMonitor.registerActivity("Iteration","Agent Loops",self.name,"Loops/min",gMonitor.OP_SUM)
    self.CERNShare = 0.144
    return result

  ##############################################################################
  def execute(self):
    """Main execution method
    """

    gMonitor.addMark('Iteration',1)
    server = RPCClient('ProductionManagement/ProductionManager')
    transID = gConfig.getValue(self.section+'/Transformation','')
    if transID:
      self.singleTransformation = long(transID)
      gLogger.info("Initializing Replication Agent for transformation %s." % transID)
    else:
      self.singleTransformation = False
      gLogger.info("TransformationAgent.execute: Initializing general purpose agent.")

    result = server.getAllProductions()
    activeTransforms = []
    if not result['OK']:
      gLogger.error("TransformationAgent.execute: Failed to get transformations.", result['Message'])
      return S_OK()

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
            result = server.setTransformationStatus(transID, 'Stopped')
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
    server = RPCClient('ProductionManagement/ProductionManager')
    result = server.getInputData(prodName,'')

    if result['OK']:
      data = result['Value']
    else:
      print "Failed to get data for transformation", prodID, result['Message']
      return S_ERROR("Failed to get data for transformation %d %s"%(prodID,result['Message']))

    if self.checkLFC == 'yes' and data:
      result = self.checkDataWithLFC(prodID,data)
      if result['OK']:
        data = result['Value']

    ancestorDepth = 0
    if transDict.has_key('Additional'):
      if transDict['Additional'].has_key('AncestorDepth'):
        ancestorDepth = int(transDict['Additional']['AncestorDepth'])

    if ancestorDepth > 0:
      data_m = []
      ancestorSEDict = {}
      for lfn,se in data:
        # Find SEs allowed by the ancestor presence
        if not ancestorSEDict.has_key(lfn):
          result = self.checkAncestors(lfn,ancestorDepth)
          if result['OK']:
            ancestorSites = [ self.getSiteForSE(x) for x in result['Value'] ]
          else:
            ancestorSites = []
        if self.getSiteForSE(se) in ancestorSites:
          data_m.append((lfn,se))
      data = data_m

    nJobs = 0
    if flush:
      while len(data) >0:
        ldata = len(data)
        data = eval('self.generateJob_'+plugin+'(data,prodID,transDict,flush)')
        if ldata == len(data):
          break
        else:
          nJobs += 1
    else:
      while len(data) >= group_size:
        ldata = len(data)
        data = eval('self.generateJob_'+plugin+'(data,prodID,transDict,flush)')
        if ldata == len(data):
          break
        else:
          nJobs += 1

    gLogger.verbose('%d jobs created' % nJobs)
    return S_OK(nJobs)

  #####################################################################################
  def generateJob_CCRC_RAW(self,data,production,transDict,flush=False):
    """ Generate a job according to the CCRC 2008 site shares
    """

    group_size = int(transDict['GroupSize'])
    dataLog = RPCClient('DataManagement/DataLogging')
    server = RPCClient('ProductionManagement/ProductionManager')
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
          result = server.setFileStatusForTransformation(production,[('Assigned',lfns)])
          if not result['OK']:
            gLogger.error("Failed to update file status for production %d"%production)

          result = server.setFileJobID(production,jobID,lfns)
          if not result['OK']:
            gLogger.error("Failed to set file job ID for production %d"%production)

          result = server.setFileSEForTransformation(production,lse,lfns)
          if not result['OK']:
            gLogger.error("Failed to set SE for production %d"%production)
          for lfn in lfns:
            result = dataLog.addFileRecord(lfn,'Job created','JobID: %s' % jobID,'','TransformationAgent')

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
  def generateJob_Standard(self,data,production,transDict,flush=False):
    """ Generates a job based on the input data, adds job to the repository
        and returns a reduced list of the lfns that rest to be processed
        If flush is true, the group_size is not taken into account
    """

    group_size = int(transDict['GroupSize'])
    dataLog = RPCClient('DataManagement/DataLogging')
    server = RPCClient('ProductionManagement/ProductionManager')
    # Sort files by SE
    datadict = {}
    for lfn,se in data:
      #if se in self.seBlackList:
      #  continue
      if not datadict.has_key(se):
        datadict[se] = []
      datadict[se].append(lfn)

    data_m = data

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
          result = server.setFileStatusForTransformation(production,[('Assigned',lfns)])
          if not result['OK']:
            gLogger.error("Failed to update file status for production %d"%production)

          result = server.setFileJobID(production,jobID,lfns)
          if not result['OK']:
            gLogger.error("Failed to set file job ID for production %d"%production)

          result = server.setFileSEForTransformation(production,lse,lfns)
          if not result['OK']:
            gLogger.error("Failed to set SE for production %d"%production)
          for lfn in lfns:
            result = dataLog.addFileRecord(lfn,'Job created','ProdID: %s JobID: %s' % (production,jobID),'','TransformationAgent')

        # Remove used files from the initial list
        data_m = []
        for lfn,se in data:
          if lfn not in lfns:
            data_m.append((lfn,se))
      else:
        gLogger.warn("Failed to add a new job to repository: "+result['Message'])

    else:
      gLogger.verbose("Neither SE has enough input data")

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
    server = RPCClient('ProductionManagement/ProductionManager')
    result = server.addProductionJob(prodID, vector, se)
    return result

  ######################################################################################
  def checkDataWithLFC(self,production,data):
    """ Check the lfns and replicas with the LFC catalog
    """

    # Sort files by LFN
    datadict = {}
    for lfn,se in data:
      if not datadict.has_key(lfn):
        datadict[lfn] = []
      datadict[lfn].append(se)

    lfns = datadict.keys()
    start = time.time()
    result = self.lfc.getReplicas(lfns,self.shifterDN)
    delta = time.time() - start
    gLogger.verbose('LFC results for %d files obtained in %.2f seconds' % (len(lfns),delta))
    lfc_datadict = {}
    lfc_data = []
    if not result['OK']:
      return result

    replicas = result['Value']['Successful']
    for lfn, replicaDict in replicas.items():
      lfc_datadict[lfn] = []
      for se,pfn in replicaDict.items():
        lfc_datadict[lfn].append(se)
        lfc_data.append((lfn,se))
    lfc_lfns = lfc_datadict.keys()
    # Check the input files if they are known by LFC
    missing_lfns = []
    for lfn in lfns:
      if lfn not in lfc_lfns:
        missing_lfns.append(lfn)
        gLogger.warn('LFN: %s not found in the LFC' % lfn)
    if missing_lfns:
      # Mark this file in the transformation
      server = RPCClient('ProductionManagement/ProductionManager')
      result = server.setFileStatusForTransformation(production,[('MissingLFC',missing_lfns)])
      if not result['OK']:
        gLogger.warn(result['Message'])

    return S_OK(lfc_data)

  def checkAncestors(self,lfn,ancestorDepth):
    """ Check ancestor availablity on sites. Returns a list of SEs where all the ancestors
        are present
    """

    result = getAncestorFiles(lfn,ancestorDepth)
    if not result['OK']:
      gLogger.warn(result['Message'])
      return result

    fileList = result['Value']
    if not fileList:
      return S_ERROR('No ancestors returned')

    # Determine common SEs now
    result = self.lfc.getReplicas(fileList,self.shifterDN)
    if not result['OK']:
      gLogger.warn(result['Message'])
      return S_ERROR('Failed to get results from LFC: %s' % result['Message'])

    replicas = result['Value']['Successful']
    ancestorSEs = []
    for lfn, replicaDict in replicas.items():
      ancestorSEs = replicaDict.keys()
      break

    for lfn, replicaDict in replicas.items():
      SEs = replicaDict.keys()
      tmp_SEs = []
      for se in SEs:
        if se in ancestorSEs:
          tmp_SEs.append(se)
      ancestorSEs = tmp_SEs
      if not ancestorSEs:
        break

    return S_OK(ancestorSEs)

  def getSiteForSE(self,se):
    """ Get site name for the given SE
    """

    result = gConfig.getSections('/Resources/Sites')
    if not result['OK']:
      return result
    gridTypes = result['Value']
    for gridType in gridTypes:
      result = gConfig.getSections('/Resources/Sites/'+gridType)
      if not result['OK']:
        continue
      siteList = result['Value']
      for site in siteList:
        ses = gConfig.getValue('/Resources/Sites/%s/%s/SE' % (gridType,site),[])
        if se in ses:
          return S_OK(site)

    return S_OK('')
