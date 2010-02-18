"""  TransformationPlugin is a class wrapping the supported LHCb transformation plugins
"""
from DIRAC                                                               import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.Core.Utilities.SiteSEMapping                                  import getSitesForSE,getSEsForSite
from DIRAC.Core.Utilities.List                                           import breakListIntoChunks, sortList, uniqueElements,randomize
from DIRAC.DataManagementSystem.Client.ReplicaManager                    import ReplicaManager
from LHCbDIRAC.BookkeepingSystem.Client.AncestorFiles                    import getAncestorFiles
from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient                import BookkeepingClient
from LHCbDIRAC.ProductionManagementSystem.Client.TransformationDBClient  import TransformationDBClient
import time,random

from DIRAC.TransformationSystem.Agent.TransformationPlugin               import TransformationPlugin as DIRACTransformationPlugin

class TransformationPlugin(DIRACTransformationPlugin):

  def __init__(self,plugin):
    DIRACTransformationPlugin.__init__(self,plugin)

  def _RAWShares(self):
    possibleTargets = ['CNAF-RAW','GRIDKA-RAW','IN2P3-RAW','NIKHEF-RAW','PIC-RAW','RAL-RAW']

    # Get the requested shares from the CS 
    res = self._getShares('CPU')
    if not res['OK']:
      return res  
    cpuShares = res['Value']
    # Remove the CERN component and renormalise
    if cpuShares.has_key('LCG.CERN.ch'):
      cpuShares.pop('LCG.CERN.ch')
    cpuShares = self._normaliseShares(cpuShares)
    gLogger.info("Obtained the following target shares (%):")
    for site in sortList(cpuShares.keys()):
      gLogger.info("%s: %.1f" % (site.ljust(15),cpuShares[site]))

    # Get the existing destinations from the transformationDB
    res = self._getExistingCounters()
    if not res['OK']:
      gLogger.error("Failed to get existing file share",res['Message'])
      return res
    existingCount = res['Value']
    if existingCount:
      gLogger.info("Existing storage element utilization (%):") 
      normalisedExistingCount = self._normaliseShares(existingCount)
      for se in sortList(normalisedExistingCount.keys()):
        gLogger.info("%s: %.1f" % (se.ljust(15),normalisedExistingCount[se]))

    bk = BookkeepingClient()
    transClient = TransformationDBClient()
    res = self._groupByRun(self.data)
    if not res['OK']:
      return res
    runFileDict = res['Value']
    tasks = []
    for runID in sortList(runFileDict.keys()):
      unusedLfns = runFileDict[runID]
      start = time.time()
      res = bk.getRunFiles(runID)
      gLogger.verbose("Obtained BK run files in %.2f seconds" % (time.time()-start))
      if not res['OK']:
        gLogger.error("Failed to get run files","%s %s" % (runID,res['Message']))
        continue
      runFiles = res['Value']
      start = time.time()
      res = transClient.getTransformationFiles(condDict = {'TransformationID':self.params['TransformationID'],'LFN':runFiles.keys(),'Status':['Assigned','Processed']})
      gLogger.verbose("Obtained transformation run files in %.2f seconds" % (time.time()-start))
      if not res['OK']:
        gLogger.error("Failed to get transformation files for run","%s %s" % (runID,res['Message']))
        continue
      if res['Value']:
        assignedSE = res['Value'][0]['UsedSE']  
      else:
        res = self._getNextDestination(existingCount,cpuShares)
        if not res['OK']:
          gLogger.error("Failed to get next destination SE",res['Message']) 
          continue
        targetSite = res['Value']
        res = getSEsForSite(targetSite)
        if not res['OK']:
          continue
        ses = res['Value']
        for se in res['Value']:
          if se in possibleTargets:
            assignedSE = se
        if not assignedSE:
          continue
      tasks.append((assignedSE,unusedLfns))
      if not existingCount.has_key(assignedSE):
        existingCount[assignedSE] = 0
      existingCount[assignedSE] += len(unusedLfns)
    return S_OK(tasks)

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

  def _LHCbDSTBroadcast(self):
    """ This plug-in takes files found at the sourceSE and broadcasts to a given number of targetSEs being sure to get a copy to CERN"""
    sourceSEs = self.params.get('SourceSE',['CERN_M-DST','CNAF_M-DST','GRIDKA_M-DST','IN2P3_M-DST','NIKHEF_M-DST','PIC_M-DST','RAL_M-DST'])
    targetSEs = self.params.get('TargetSE',['CERN_M-DST','CNAF-DST','GRIDKA-DST','IN2P3-DST','NIKHEF-DST','PIC-DST','RAL-DST'])
    destinations = int(self.params.get('Destinations',6))
    return self._lhcbBroadcast(sourceSEs, targetSEs, destinations, 'CERN_M-DST')

  def _LHCbMCDSTBroadcast(self):
    """ This plug-in takes files found at the sourceSE and broadcasts to a given number of targetSEs being sure to get a copy to CERN"""
    sourceSEs = self.params.get('SourceSE',['CERN_MC_M-DST','CNAF_MC_M-DST','GRIDKA_MC_M-DST','IN2P3_MC_M-DST','NIKHEF_MC_M-DST','PIC_MC_M-DST','RAL_MC_M-DST'])
    targetSEs = self.params.get('TargetSE',['CERN_MC_M-DST','CNAF_MC-DST','GRIDKA_MC-DST','IN2P3_MC-DST','NIKHEF_MC-DST','PIC_MC-DST','RAL_MC-DST'])
    destinations = int(self.params.get('Destinations',2))
    return self._lhcbBroadcast(sourceSEs, targetSEs, destinations, 'CERN_MC_M-DST')

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

  def _lhcbBroadcast(self,sourceSEs,targetSEs,destinations,cernSE):
    filesOfInterest = {}
    fileGroups = self._getFileGroups(self.data)
    for replicaSE,lfns in fileGroups.items():
      sources = replicaSE.split(',')
      atSource = False
      for se in sources:
        if se in sourceSEs:
          atSource = True
      if atSource:
        for lfn in lfns:
          filesOfInterest[lfn] = sources

    targetSELfns = {}
    for lfn,sources in filesOfInterest.items():
      targets = []
      sourceSites = self._getSitesForSEs(sources)
      if not 'LCG.CERN.ch' in sourceSites:
        sourceSites.append('LCG.CERN.ch')  
        targets.append(cernSE)
      else:
        randomTape = cernSE
        while randomTape == cernSE:
          randomTape = randomize(sourceSEs)[0]
        site = self._getSiteForSE(randomTape)['Value']
        sourceSites.append(site)
        targets.append(randomTape)
      for targetSE in randomize(targetSEs):
        if (destinations) and (len(targets) >= destinations):
          continue
        site = self._getSiteForSE(targetSE)['Value']
        if not site in sourceSites:
          targets.append(targetSE)
          sourceSites.append(site)
      strTargetSEs = str.join(',',sortList(targets))
      if not targetSELfns.has_key(strTargetSEs):
        targetSELfns[strTargetSEs] = []
      targetSELfns[strTargetSEs].append(lfn)
    tasks = []
    for strTargetSEs in sortList(targetSELfns.keys()):
      lfns = targetSELfns[strTargetSEs]
      tasks.append((strTargetSEs,lfns))
    return S_OK(tasks)
  
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
