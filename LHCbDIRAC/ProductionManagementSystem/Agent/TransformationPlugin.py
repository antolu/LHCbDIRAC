"""  TransformationPlugin is a class wrapping the supported LHCb transformation plugins
"""
from DIRAC                                                               import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.Core.Utilities.SiteSEMapping                                  import getSitesForSE,getSEsForSite
from DIRAC.Core.Utilities.List                                           import breakListIntoChunks, sortList, uniqueElements,randomize
from DIRAC.DataManagementSystem.Client.ReplicaManager                    import ReplicaManager
from LHCbDIRAC.BookkeepingSystem.Client.AncestorFiles                    import getAncestorFiles
from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient                import BookkeepingClient
from LHCbDIRAC.ProductionManagementSystem.Client.TransformationDBClient  import TransformationDBClient
import time,random,sys

from DIRAC.TransformationSystem.Agent.TransformationPlugin               import TransformationPlugin as DIRACTransformationPlugin

class TransformationPlugin(DIRACTransformationPlugin):

  def __init__(self,plugin):
    DIRACTransformationPlugin.__init__(self,plugin)

  def _AtomicRun(self):
    possibleTargets = ['CERN-RAW','CNAF-RAW','GRIDKA-RAW','IN2P3-RAW','NIKHEF-RAW','PIC-RAW','RAL-RAW']
    bk = BookkeepingClient()
    transClient = TransformationDBClient()
    # Get the requested shares from the CS
    res = self._getShares('CPU',True)
    if not res['OK']:
      return res
    cpuShares = res['Value']
    gLogger.info("Obtained the following target shares (%):")
    for site in sortList(cpuShares.keys()):
      gLogger.info("%s: %.1f" % (site.ljust(15),cpuShares[site]))

    # Get the existing destinations from the transformationDB
    res = self._getExistingCounters(requestedSites=cpuShares.keys())
    if not res['OK']:
      gLogger.error("Failed to get executed share",res['Message'])
      return res
    existingCount = res['Value']
    if existingCount:
      gLogger.info("Existing site utilization (%):")
      normalisedExistingCount = self._normaliseShares(existingCount)
      for se in sortList(normalisedExistingCount.keys()):
        gLogger.info("%s: %.1f" % (se.ljust(15),normalisedExistingCount[se]))

    # Group the remaining data by run
    res = self.__groupByRunAndParam(self.data,param='Standard')
    if not res['OK']:
      return res
    runFileDict = res['Value']

    # For each of the runs determine the destination of any previous files
    tasks = []
    for runID in sortList(runFileDict.keys()):
      unusedLfns = runFileDict[runID][None]
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
        res = getSitesForSE(assignedSE,gridName='LCG')
        if not res['OK']:
          continue
        targetSite = ''
        for site in res['Value']:
          if site in cpuShares.keys():
            targetSite = site
        if not targetSite:
          continue
      else:
        # Must get the replicas of the files and determine the corresponding sites
        distinctSEs = []
        candidates = []
        for lfn in unusedLfns:
          ses = self.data[lfn].keys()
          for se in ses:
            if se not in distinctSEs:
              res = getSitesForSE(se,gridName='LCG')
              if not res['OK']:
                continue
              for site in res['Value']:
                if site in cpuShares.keys():
                  if not site in candidates:
                    candidates.append(site)
                    distinctSEs.append(se)
        if len(candidates) < 2:
          continue
        res = self._getNextSite(existingCount,cpuShares,randomize(candidates))
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
      for lfn in unusedLfns:
        if assignedSE in self.data[lfn].keys():
          tasks.append((assignedSE,[lfn]))
        if not existingCount.has_key(targetSite):
          existingCount[targetSite] = 0
        existingCount[targetSite] += 1
    return S_OK(tasks)

  def _RAWShares(self):
    possibleTargets = ['CNAF-RAW','GRIDKA-RAW','IN2P3-RAW','NIKHEF-RAW','PIC-RAW','RAL-RAW']
    transClient = TransformationDBClient()
    transID = self.params['TransformationID']

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

    # Ensure that our files only have one existing replica at CERN
    replicaGroups = self._getFileGroups(self.data)
    alreadyReplicated = {}
    for replicaSE,lfns in replicaGroups.items():
      existingSEs = replicaSE.split(',')
      if len(existingSEs) > 1:
        for lfn in lfns:
          self.data.pop(lfn)
        existingSEs.remove('CERN-RAW')
        targetSE = existingSEs[0]
        if not alreadyReplicated.has_key(targetSE):
          alreadyReplicated[targetSE] = []
        alreadyReplicated[targetSE].extend(lfns)
    for se,lfns in alreadyReplicated.items():
      gLogger.info("Attempting to update %s files to Processed at %s" % (len(lfns),se))
      res = self.__groupByRunAndParam(dict.fromkeys(lfns),param='Standard')
      if not res['OK']:
        return res
      runFileDict = res['Value']
      for runID in sortList(runFileDict.keys()):
        res = transClient.setTransformationRunsSite(transID,runID,se)
        if not res['OK']:
          gLogger.error("Failed to assign TransformationRun site",res['Message'])
          return res
      transClient.setFileUsedSEForTransformation(self.params['TransformationID'],se,lfns)

    # Get the existing destinations from the transformationDB
    res = self._getExistingCounters(requestedSites=cpuShares.keys())
    if not res['OK']:
      gLogger.error("Failed to get existing file share",res['Message'])
      return res
    existingCount = res['Value']
    if existingCount:
      gLogger.info("Existing storage element utilization (%):") 
      normalisedExistingCount = self._normaliseShares(existingCount)
      for se in sortList(normalisedExistingCount.keys()):
        gLogger.info("%s: %.1f" % (se.ljust(15),normalisedExistingCount[se]))

    # Group the remaining data by run 
    res = self.__groupByRunAndParam(self.data,param='Standard')
    if not res['OK']:
      return res
    runFileDict = res['Value']

    # For each of the runs determine the destination of any previous files
    runSiteDict = {}
    if runFileDict:
      res = transClient.getTransformationRuns({'TransformationID':transID,'RunNumber':runFileDict.keys()})
      if not res['OK']:
        gLogger.error("Failed to obtain TransformationRuns",res['Message']) 
        return res
      for runDict in res['Value']:
        runSiteDict[runDict['RunNumber']] = runDict['SelectedSite']

    # Choose the destination SE
    tasks = []
    for runID in sortList(runFileDict.keys()):
      unusedLfns = runFileDict[runID][None]
      assignedSE = None
      if not runSiteDict.has_key(runID):
        continue
      if runSiteDict[runID]:
        assignedSE = runSiteDict[runID]
        res = getSitesForSE(assignedSE,gridName='LCG')
        if not res['OK']:
          continue
        targetSite = ''
        for site in res['Value']:
          if site in cpuShares.keys():
            targetSite = site
        if not targetSite:
          continue
      else:
        res = self._getNextSite(existingCount,cpuShares)
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

      # Update the TransformationRuns table with the assigned (if this fails do not create the tasks)
      res = transClient.setTransformationRunsSite(transID,runID,assignedSE)
      if not res['OK']:
        gLogger.error("Failed to assign TransformationRun site",res['Message'])      
        continue
      #Create the tasks
      tasks.append((assignedSE,unusedLfns))
      if not existingCount.has_key(targetSite):
        existingCount[targetSite] = 0
      existingCount[targetSite] += len(unusedLfns)
    return S_OK(tasks)

  def _MergeByRunWithFlush(self):
    return self.__mergeByRun(requireFlush=True)

  def _MergeByRun(self):
    return self.__mergeByRun(requireFlush=False)

  def __mergeByRun(self,requireFlush=False):
    res = self.__groupByRunAndParam(self.data,param='FileType')
    if not res['OK']:
      return res
    allReplicas = self.data.copy()
    allTasks = []
    runFiles = res['Value']
    transClient = TransformationDBClient()
    res = transClient.getTransformationRuns({'TransformationID':self.params['TransformationID'],'RunNumber':runFiles.keys()})
    if not res['OK']:
      return res
    transStatus = self.params['Status']
    for runDict in res['Value']:
      runID = runDict['RunNumber']
      status = runDict['Status']
      if (requireFlush and (status != 'Flush')):
        if transStatus != 'Flush':
          gLogger.info("Run %d not in flush status" % runID)
          continue
      gLogger.info("Flushing run %d" % runID)
      paramDict = runFiles[runID]
      for paramValue in sortList(paramDict.keys()):
        runParamLfns = paramDict[paramValue]
        runParamReplicas = {}
        for lfn in runParamLfns:
          runParamReplicas[lfn] = allReplicas[lfn]
        self.data = runParamReplicas
        self.params['Status'] = 'Flush'
        res = self._BySize()
        self.params['Status'] = transStatus
        if not res['OK']:
          return res
        allTasks.extend(res['Value'])
      if requireFlush:
        transClient.setTransformationRunStatus(self.params['TransformationID'],runID,'Active')
    return S_OK(allTasks)

  def _ByRun(self,param='', plugin='Standard'):
    res = self.__groupByRunAndParam(self.data,param=param)
    if not res['OK']:
      return res
    allReplicas = self.data.copy()
    allTasks = []
    runDict = res['Value']
    for runID in sortList(runDict.keys()):
      paramDict = runDict[runID]
      for paramValue in sortList(paramDict.keys()):
        runParamLfns = paramDict[paramValue]
        runParamReplicas = {}
        for lfn in runParamLfns:
          runParamReplicas[lfn] = allReplicas[lfn]
        self.data = runParamReplicas
        res = eval('self._%s()' % plugin)
        if not res['OK']:
          return res
        allTasks.extend(res['Value'])
    return S_OK(allTasks)  

  def _ByRunBySize(self):
    return self._ByRun(plugin='BySize')
  
  def _ByRunFileTypeSize(self):
    return self._ByRun(param='FileType',plugin='BySize')
  
  def _ByRunFileType(self):
    return self._ByRun(param='FileType')

  def _ByRunEventTypeSize(self):
    return self._ByRun(param='EventTypeId',plugin='BySize')
  
  def _ByRunEventType(self):
    return self._ByRun(param='EventTypeId')

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

  def __groupByRunAndParam(self,lfnDict,param=''):
    runDict = {}
    res = self.__getBookkeepingMetadata(lfnDict.keys())
    if not res['OK']:
      return res
    for lfn,metadata in res['Value'].items():
      runNumber = 0
      if metadata.has_key("RunNumber"):
        runNumber = metadata["RunNumber"]
      if not runDict.has_key(runNumber):
        runDict[runNumber] = {}
      paramValue = metadata.get(param)
      if not runDict[runNumber].has_key(paramValue):
        runDict[runNumber][paramValue] = []
      runDict[runNumber][paramValue].append(lfn)
    return S_OK(runDict)

  def __getBookkeepingMetadata(self,lfns):
    bk = BookkeepingClient()
    start = time.time()
    res = bk.getFileMetadata(lfns)
    gLogger.verbose("Obtained BK file metadata in %.2f seconds" % (time.time()-start))
    if not res['OK']: 
      gLogger.error("Failed to get bookkeeping metadata",res['Message'])
    return res

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
