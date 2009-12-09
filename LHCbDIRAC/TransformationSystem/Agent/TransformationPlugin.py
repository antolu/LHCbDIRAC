"""  TransformationPlugin is a class wrapping the supported LHCb transformation plugins
"""
from DIRAC                                                  import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.Core.Utilities.SiteSEMapping                     import getSitesForSE,getSEsForSite
from DIRAC.Core.Utilities.List                              import breakListIntoChunks, sortList, uniqueElements
from DIRAC.DataManagementSystem.Client.ReplicaManager       import ReplicaManager
from LHCbDIRAC.BookkeepingSystem.Client.AncestorFiles       import getAncestorFiles
from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient   import BookkeepingClient
import time,random

from DIRAC.TransformationSystem.Agent.TransformationPlugin  import TransformationPlugin as DIRACTransformationPlugin

class TransformationPlugin(DIRACTransformationPlugin):

  def __init__(self,plugin):
    DIRACTransformationPlugin.__init__(self,plugin)

  def _ByRun(self,plugin='Standard'):
    res = self._groupByRun(self.data)
    if not res['OK']:
      return res
    allReplicas = self.data.copy()
    allTasks = []
    for runID,runLfns in res['Value'].items():
      runReplicas = {}
      for runLfn in runLfns:
        runReplicas[runLfn] = allReplicas[runLfn]
      self.data = runReplicas
      res = eval('self._%s()' % plugin)
      if not res['OK']:
        return res
      allTasks.extend(res['Value'])
    return S_OK(allTasks)  

  def _ByRunBySize(self):
    return self._ByRun(plugin='BySize')

  def _ByRunCCRC_RAW(self):
    return self._ByRun(plugin='CCRC_RAW')

  def _CCRC_RAW(self):
    res = gConfig.getOptionsDict("/Operations/Shares/CPU")
    if (not res['OK']) or (not res['Value']) or (not res['Value'].has_key("LCG.CERN.ch")):
      gLogger.warn("CPU shares should be defined in /Operations/Shares/CPU")
      shares = {"LCG.CERN.ch": 0.144}
    else:
      shares = res['Value']
    res = getSEsForSite('LCG.CERN.ch')
    if not res['OK']:
      return res
    cernSEs = res['Value']
    res = self._groupByReplicas()
    if not res['OK']:
      return res
    tasks = []
    for replicaSE,lfns in res['Value']:
      ses = replicaSE.split(',')
      if len(ses) == 2:
        cernSE = ''
        for se in ses:
          if se in cernSEs:
            cernSE = se
        if cernSE:
          ses.remove(cernSE)
          otherSE = ses[0]
          if random.random() < shares["LCG.CERN.ch"]:
            tasks.append((cernSE,lfns))
          else:
            tasks.append((otherSE,lfns))
    return S_OK(tasks)

  def _groupByRun(self,lfnDict):
    # group data by run
    bk = BookkeepingClient()
    start = time.time()
    res = bk.getFileMetadata(lfnDict.keys())
    gLogger.verbose("Obtained BK file metadata in %.2f seconds" % (time.time()-start))
    if not res['OK']: 
      gLogger.error("Failed to get bookkeeping metadata",res['Message'])
      return res
    runDict = {}
    for lfn,metadata in res['Value'].items():
      runNumber = 0
      if metadata.has_key("RunNumber"):
        runNumber = metadata["RunNumber"]
      if not runDict.has_key(runNumber):
        runDict[runNumber] = []
      runDict[runNumber].append(lfn)
    return S_OK(runDict)

  def _checkAncestors(self,filesReplicas,ancestorDepth):
    """ Check ancestor availability on sites. Returns a list of SEs where all the ancestors are present
    """
    # Get the ancestors from the BK
    res = getFileAncestors(filesReplicas.keys(),ancestorDepth)
    if not res['OK']:
      gLogger.warn(res['Message'])
      return res
    ancestorDict = res['Value']
    
    # Get the replicas for all the ancestors
    allAncestors = []
    for ancestors in ancestorDict.values():
      allAncestors.extend(ancestors)
    rm = ReplicaManager()
    startTime = time.time()
    res = rm.getActiveReplicas(allAncestors)
    if not res['OK']:
      return res
    for lfn in res['Value']['Failed'].keys():
      self.data.pop(lfn)
    gLogger.info("Replica results for %d files obtained in %.2f seconds" % (len(res['Value']['Successful']),time.time()-startTime))
    ancestorReplicas = res['Value']['Successful']

    seSiteCache = {}
    dataLfns = self.data.keys()
    for lfn in dataLfns:
      lfnSEs = self.data[lfn].keys()
      lfnSites = {}
      for se in lfnSEs:
        if not seSiteCache.has_key(se):
          seSiteCache[se] = self._getSiteForSE(se)['Value']
        lfnSites[seSiteCache[se]] = se
      for ancestorLfn in ancestorDict[lfn]:
        ancestorSEs = ancestorReplicas[ancestorLfn]
        for se in ancestorSEs:
          if not seSiteCache.has_key(se):
            seSiteCache[se] = self._getSiteForSE(se)['Value']
          ancestorSites.append(seSiteCache[se])
        for lfnSite in lfnSites.keys():
          if not lfnSite in ancestorSites:
            if self.data[lfn].has_key(lfnSites[lfnSite]):
              self.data[lfn].pop(lfnSites[lfnSite])
    ancestorProblems = []
    for lfn,replicas in self.data.items():
      if len(replicas) == 0:
        gLogger.error("File ancestors not found at corresponding sites",lfn)
        ancestorProblems.append(lfn)          
    return S_OK()
  
  #TODO: Update the way the data replicas are passed.

  def _MCBroadcast(self):
    """ This plug-in takes files found at the sourceSE and broadcasts to a given number of targetSEs
    """
    if not self.params:
      return S_ERROR("TransformationPlugin._MCBroadcast: The 'MCBroadcast' plugin requires additional parameters.")

    destinations = int(self.params['Destinations'])

    seFiles = {}
    for lfn,se in self.data:
      lfnTargetSEs = self.params['TargetSE'].split(',')
      random.shuffle(lfnTargetSEs)
      lfnSourceSEs = self.params['SourceSE'].split(',')
      random.shuffle(lfnSourceSEs)
      sourceSites = [se.split('_')[0].split('-')[0]]
      if se in lfnSourceSEs:
        # If the file is not at CERN then it should be
        if not 'CERN' in sourceSites:
          targets = ['CERN_MC_M-DST']
          sourceSites.append('CERN')
        # Otherwise make sure it is at another tape SE
        else:
          otherTape = se
          while otherTape == se:
            random.shuffle(lfnSourceSEs) 
            otherTape = lfnSourceSEs[-1]
          targets = [otherTape]
          sourceSites.append(otherTape.split('_')[0].split('-')[0])
        for targetSE in lfnTargetSEs:
          possibleTargetSite = targetSE.split('_')[0].split('-')[0]
          if not possibleTargetSite in sourceSites: 
            if len(sourceSites) < destinations:
              targets.append(targetSE)
              sourceSites.append(possibleTargetSite)
        strTargetSE = ','.join(targets)
        if not seFiles.has_key(se):
          seFiles[se] = {}
        if not seFiles[se].has_key(strTargetSE):
          seFiles[se][strTargetSE] = []
        seFiles[se][strTargetSE].append(lfn)
    return S_OK(seFiles)

  def _LoadBalance(self):
    """ This plug-in will load balances the input files across the selected target SEs.
    """
    if not self.params:
      return S_ERROR("TransformationPlugin._LoadBalance: The 'LoadBalance' plugin requires additional parameters.")

    targetSEs = {}
    totalRatio = 0

    ses = self.params['TargetSE'].split(',')
    for targetSE in ses:
      targetSEs[targetSE] = int(self.params[targetSE])
      totalRatio += int(self.params[targetSE])

    sourceSE = ''
    if self.params.has_key('SourceSE'):
      sourceSE = self.params['SourceSE']
    seFiles = {}

    selectedFiles = []
    for lfn,se in self.data:
      useFile = False
      if not sourceSE:
        useFile = True
      elif sourceSE == se:
        useFile = True
      if useFile:
        selectedFiles.append(lfn)

    multiplier = int(len(selectedFiles)/float(totalRatio))
    if multiplier > 0:
      currIndex = 0
      seFiles[sourceSE] = {}
      for targetSE,load in targetSEs.items():
        offset = (load*multiplier)
        seFiles[sourceSE][targetSE] = selectedFiles[currIndex:currIndex+offset]
        currIndex += offset
    return S_OK(seFiles)
  
  
  
