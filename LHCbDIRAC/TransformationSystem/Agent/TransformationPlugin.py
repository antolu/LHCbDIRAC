"""  TransformationPlugin is a class wrapping the supported LHCb transformation plugins
"""
from DIRAC                                                             import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.Core.Utilities.SiteSEMapping                                import getSitesForSE, getSEsForSite
from DIRAC.Core.Utilities.List                                         import breakListIntoChunks, sortList, uniqueElements, randomize
from DIRAC.DataManagementSystem.Client.ReplicaManager                  import ReplicaManager
from LHCbDIRAC.NewBookkeepingSystem.Client.AncestorFiles                  import getAncestorFiles
from LHCbDIRAC.NewBookkeepingSystem.Client.BookkeepingClient              import BookkeepingClient
from LHCbDIRAC.TransformationSystem.Client.TransformationClient        import TransformationClient
import time, random, sys, re, types

from DIRAC.TransformationSystem.Agent.TransformationPlugin               import TransformationPlugin as DIRACTransformationPlugin

class TransformationPlugin( DIRACTransformationPlugin ):

  def __init__( self, plugin ):
    DIRACTransformationPlugin.__init__( self, plugin )
    self.transClient = TransformationClient()

  def _AtomicRun( self ):
    possibleTargets = ['LCG.CERN.ch', 'LCG.CNAF.it', 'LCG.GRIDKA.de', 'LCG.IN2P3.fr', 'LCG.PIC.es', 'LCG.RAL.uk', 'LCG.SARA.nl']
    transID = self.params['TransformationID']
    # Get the requested shares from the CS
    res = self._getShares( 'CPU', True )
    if not res['OK']:
      return res
    cpuShares = res['Value']
    gLogger.info( "Obtained the following target shares (%):" )
    for site in sortList( cpuShares.keys() ):
      gLogger.info( "%s: %.1f" % ( site.ljust( 15 ), cpuShares[site] ) )

    # Get the existing destinations from the transformationDB
    res = self._getExistingCounters( requestedSites = cpuShares.keys() )
    if not res['OK']:
      gLogger.error( "Failed to get executed share", res['Message'] )
      return res
    existingCount = res['Value']
    if existingCount:
      gLogger.info( "Existing site utilization (%):" )
      normalisedExistingCount = self._normaliseShares( existingCount )
      for se in sortList( normalisedExistingCount.keys() ):
        gLogger.info( "%s: %.1f" % ( se.ljust( 15 ), normalisedExistingCount[se] ) )

    # Group the remaining data by run
    res = self.__groupByRun( self.files )
    if not res['OK']:
      return res
    runFileDict = res['Value']

    runSitesToUpdate = {}
    # For each of the runs determine the destination of any previous files
    runSiteDict = {}
    if runFileDict:
      res = self.transClient.getTransformationRuns( {'TransformationID':transID, 'RunNumber':runFileDict.keys()} )
      if not res['OK']:
        gLogger.error( "Failed to obtain TransformationRuns", res['Message'] )
        return res
      for runDict in res['Value']:
        selectedSite = runDict['SelectedSite']
        runID = runDict['RunNumber']
        if selectedSite:
          runSiteDict[runID] = selectedSite
          continue
        res = self.transClient.getTransformationFiles( condDict = {'TransformationID':transID, 'RunNumber':runID, 'Status':['Assigned', 'Processed']} )
        if not res['OK']:
          gLogger.error( "Failed to get transformation files for run", "%s %s" % ( runID, res['Message'] ) )
          continue
        if res['Value']:
          assignedSE = res['Value'][0]['UsedSE']
          res = getSitesForSE( assignedSE, gridName = 'LCG' )
          if not res['OK']:
            continue
          for site in res['Value']:
            if site in cpuShares.keys():
              runSiteDict[runID] = site
              runSitesToUpdate[runID] = site

    # Choose the destination site for new runs
    for runID in sortList( runFileDict.keys() ):
      if runID in runSiteDict.keys():
        continue
      unusedLfns = runFileDict[runID]
      distinctSEs = []
      candidates = []
      for lfn in unusedLfns:
        ses = self.data[lfn].keys()
        for se in ses:
          if se not in distinctSEs:
            res = getSitesForSE( se, gridName = 'LCG' )
            if not res['OK']:
              continue
            for site in res['Value']:
              if site in cpuShares.keys():
                if not site in candidates:
                  candidates.append( site )
                  distinctSEs.append( se )
      if len( candidates ) < 2:
        gLogger.info( "Two candidate site not found for %d" % runID )
        continue
      res = self._getNextSite( existingCount, cpuShares, randomize( candidates ) )
      if not res['OK']:
        gLogger.error( "Failed to get next destination SE", res['Message'] )
        continue
      targetSite = res['Value']
      if not existingCount.has_key( targetSite ):
        existingCount[targetSite] = 0
      existingCount[targetSite] += len( unusedLfns )
      runSiteDict[runID] = targetSite
      runSitesToUpdate[runID] = targetSite

    # Create the tasks
    tasks = []
    for runID in sortList( runSiteDict.keys() ):
      targetSite = runSiteDict[runID]
      unusedLfns = runFileDict[runID]
      res = getSEsForSite( targetSite )
      if not res['OK']:
        continue
      possibleSEs = res['Value']
      for lfn in sortList( unusedLfns ):
        replicaSEs = self.data[lfn].keys()
        if len( replicaSEs ) < 2:
          continue
        for se in replicaSEs:
          if se in possibleSEs:
            tasks.append( ( se, [lfn] ) )

    # Update the TransformationRuns table with the assigned (if this fails do not create the tasks)
    for runID, targetSite in runSitesToUpdate.items():
      res = self.transClient.setTransformationRunsSite( transID, runID, targetSite )
      if not res['OK']:
        gLogger.error( "Failed to assign TransformationRun site", res['Message'] )
        continue
    return S_OK( tasks )

  def __groupByRun( self, files ):
    runFiles = {}
    for fileDict in files:
      runNumber = fileDict.get( 'RunNumber', 0 )
      lfn = fileDict.get( 'LFN', '' )
      if runNumber not in runFiles.keys():
        runFiles[runNumber] = []
      if lfn:
        runFiles[runNumber].append( lfn )
    return S_OK( runFiles )

  def _RAWShares( self ):
    possibleTargets = ['CNAF-RAW', 'GRIDKA-RAW', 'IN2P3-RAW', 'NIKHEF-RAW', 'PIC-RAW', 'RAL-RAW']
    transID = self.params['TransformationID']

    # Get the requested shares from the CS
    res = self._getShares( 'CPU' )
    if not res['OK']:
      return res
    cpuShares = res['Value']
    # Remove the CERN component and renormalise
    if cpuShares.has_key( 'LCG.CERN.ch' ):
      cpuShares.pop( 'LCG.CERN.ch' )
    cpuShares = self._normaliseShares( cpuShares )
    gLogger.info( "Obtained the following target shares (%):" )
    for site in sortList( cpuShares.keys() ):
      gLogger.info( "%s: %.1f" % ( site.ljust( 15 ), cpuShares[site] ) )

    # Ensure that our files only have one existing replica at CERN
    replicaGroups = self._getFileGroups( self.data )
    alreadyReplicated = {}
    for replicaSE, lfns in replicaGroups.items():
      existingSEs = replicaSE.split( ',' )
      if len( existingSEs ) > 1:
        for lfn in lfns:
          self.data.pop( lfn )
        existingSEs.remove( 'CERN-RAW' )
        targetSE = existingSEs[0]
        if not alreadyReplicated.has_key( targetSE ):
          alreadyReplicated[targetSE] = []
        alreadyReplicated[targetSE].extend( lfns )
    for se, lfns in alreadyReplicated.items():
      gLogger.info( "Attempting to update %s files to Processed at %s" % ( len( lfns ), se ) )
      res = self.__groupByRunAndParam( dict.fromkeys( lfns ), param = 'Standard' )
      if not res['OK']:
        return res
      runFileDict = res['Value']
      for runID in sortList( runFileDict.keys() ):
        res = self.transClient.setTransformationRunsSite( transID, runID, se )
        if not res['OK']:
          gLogger.error( "Failed to assign TransformationRun site", res['Message'] )
          return res
      self.transClient.setFileUsedSEForTransformation( self.params['TransformationID'], se, lfns )

    # Get the existing destinations from the transformationDB
    res = self._getExistingCounters( requestedSites = cpuShares.keys() )
    if not res['OK']:
      gLogger.error( "Failed to get existing file share", res['Message'] )
      return res
    existingCount = res['Value']
    if existingCount:
      gLogger.info( "Existing storage element utilization (%):" )
      normalisedExistingCount = self._normaliseShares( existingCount )
      for se in sortList( normalisedExistingCount.keys() ):
        gLogger.info( "%s: %.1f" % ( se.ljust( 15 ), normalisedExistingCount[se] ) )

    # Group the remaining data by run
    res = self.__groupByRunAndParam( self.data, param = 'Standard' )
    if not res['OK']:
      return res
    runFileDict = res['Value']

    # For each of the runs determine the destination of any previous files
    runSiteDict = {}
    if runFileDict:
      res = self.transClient.getTransformationRuns( {'TransformationID':transID, 'RunNumber':runFileDict.keys()} )
      if not res['OK']:
        gLogger.error( "Failed to obtain TransformationRuns", res['Message'] )
        return res
      for runDict in res['Value']:
        runSiteDict[runDict['RunNumber']] = runDict['SelectedSite']

    # Choose the destination SE
    tasks = []
    for runID in sortList( runFileDict.keys() ):
      unusedLfns = runFileDict[runID][None]
      assignedSE = None
      if not runSiteDict.has_key( runID ):
        continue
      if runSiteDict[runID]:
        assignedSE = runSiteDict[runID]
        res = getSitesForSE( assignedSE, gridName = 'LCG' )
        if not res['OK']:
          continue
        targetSite = ''
        for site in res['Value']:
          if site in cpuShares.keys():
            targetSite = site
        if not targetSite:
          continue
      else:
        res = self._getNextSite( existingCount, cpuShares )
        if not res['OK']:
          gLogger.error( "Failed to get next destination SE", res['Message'] )
          continue
        targetSite = res['Value']
        res = getSEsForSite( targetSite )
        if not res['OK']:
          continue
        ses = res['Value']
        for se in res['Value']:
          if se in possibleTargets:
            assignedSE = se
        if not assignedSE:
          continue

      # Update the TransformationRuns table with the assigned (if this fails do not create the tasks)
      res = self.transClient.setTransformationRunsSite( transID, runID, assignedSE )
      if not res['OK']:
        gLogger.error( "Failed to assign TransformationRun site", res['Message'] )
        continue
      #Create the tasks
      tasks.append( ( assignedSE, unusedLfns ) )
      if not existingCount.has_key( targetSite ):
        existingCount[targetSite] = 0
      existingCount[targetSite] += len( unusedLfns )
    return S_OK( tasks )

  def _MergeByRunWithFlush( self ):
    return self.__mergeByRun( requireFlush = True )

  def _MergeByRun( self ):
    return self.__mergeByRun( requireFlush = False )

  def __mergeByRun( self, requireFlush = False ):
    runFiles = {}
    for fileDict in self.files:
      runNumber = fileDict.get( 'RunNumber' )
      lfn = fileDict.get( 'LFN' )
      if ( runNumber ) and ( runNumber not in runFiles.keys() ):
        runFiles[runNumber] = []
      if runNumber and lfn:
        runFiles[runNumber].append( lfn )
    allReplicas = self.data.copy()
    allTasks = []
    res = self.transClient.getTransformationRuns( {'TransformationID':self.params['TransformationID'], 'RunNumber':runFiles.keys()} )
    if not res['OK']:
      return res
    transStatus = self.params['Status']
    for runDict in res['Value']:
      runID = runDict['RunNumber']
      runStatus = runDict['Status']
      runLfns = runFiles.get( runID, [] )
      if not runLfns:
        if requireFlush:
          self.transClient.setTransformationRunStatus( self.params['TransformationID'], runID, 'Flush' )
        continue
      runReplicas = {}
      for lfn in runLfns:
        if not allReplicas.has_key( lfn ):
          continue
        runReplicas[lfn] = allReplicas[lfn]
      if not runReplicas:
        continue
      self.data = runReplicas
      # Make sure we handle the flush correctly
      if transStatus == 'Flush':
        status = 'Flush'
      elif not requireFlush:
        status = 'Flush'
      else:
        status = runStatus
      self.params['Status'] = status
      res = self._BySize()
      self.params['Status'] = transStatus
      if not res['OK']:
        return res
      allTasks.extend( res['Value'] )
      if requireFlush:
        self.transClient.setTransformationRunStatus( self.params['TransformationID'], runID, 'Flush' )
    return S_OK( allTasks )

  def _ByRun( self, param = '', plugin = 'Standard' ):
    res = self.__groupByRunAndParam( self.data, param = param )
    if not res['OK']:
      return res
    allReplicas = self.data.copy()
    allTasks = []
    runDict = res['Value']
    for runID in sortList( runDict.keys() ):
      paramDict = runDict[runID]
      for paramValue in sortList( paramDict.keys() ):
        runParamLfns = paramDict[paramValue]
        runParamReplicas = {}
        for lfn in runParamLfns:
          runParamReplicas[lfn] = allReplicas[lfn]
        self.data = runParamReplicas
        res = eval( 'self._%s()' % plugin )
        if not res['OK']:
          return res
        allTasks.extend( res['Value'] )
    return S_OK( allTasks )

  def _ByRunBySize( self ):
    return self._ByRun( plugin = 'BySize' )

  def _ByRunFileTypeSize( self ):
    return self._ByRun( param = 'FileType', plugin = 'BySize' )

  def _ByRunFileType( self ):
    return self._ByRun( param = 'FileType' )

  def _ByRunEventTypeSize( self ):
    return self._ByRun( param = 'EventTypeId', plugin = 'BySize' )

  def _ByRunEventType( self ):
    return self._ByRun( param = 'EventTypeId' )

  def _LHCbDSTBroadcast( self ):
    """ This plug-in takes files found at the sourceSE and broadcasts to a given number of targetSEs being sure to get a copy to CERN"""
    master1SEs = self.params.get( 'Master1SEs', ['CERN_M-DST'] )
    master2SEs = self.params.get( 'Master2SEs', ['CERN_M-DST', 'CNAF_M-DST', 'GRIDKA_M-DST', 'IN2P3_M-DST', 'NIKHEF_M-DST', 'PIC_M-DST', 'RAL_M-DST'] )
    targetSEs = self.params.get( 'SecondarySEs', ['CERN_M-DST', 'CNAF-DST', 'GRIDKA-DST', 'IN2P3-DST', 'NIKHEF-DST', 'PIC-DST', 'RAL-DST'] )
    numberOfCopies = int( self.params.get( 'NumberOfReplicas', 4 ) )
    return self._lhcbBroadcast( randomize( master1SEs )[0], master2SEs, targetSEs, numberOfCopies )

  def _LHCbMCDSTBroadcast( self ):
    """ This plug-in takes files found at the sourceSE and broadcasts to a given number of targetSEs being sure to get a copy to CERN"""
    master1SEs = self.params.get( 'Master1SEs', ['CERN_MC_M-DST'] )
    master2SEs = self.params.get( 'Master2SEs', ['CERN_MC_M-DST', 'CNAF_MC_M-DST', 'GRIDKA_MC_M-DST', 'IN2P3_MC_M-DST', 'NIKHEF_MC_M-DST', 'PIC_MC_M-DST', 'RAL_MC_M-DST'] )
    targetSEs = self.params.get( 'SecondarySEs', ['CERN_MC_M-DST', 'CNAF_MC-DST', 'GRIDKA_MC-DST', 'IN2P3_MC-DST', 'NIKHEF_MC-DST', 'PIC_MC-DST', 'RAL_MC-DST'] )
    numberOfCopies = int( self.params.get( 'NumberOfReplicas', 3 ) )
    return self._lhcbBroadcast( randomize( master1SEs )[0], master2SEs, targetSEs, numberOfCopies )

  def __groupByRunAndParam( self, lfnDict, param = '' ):
    runDict = {}
    res = self.__getBookkeepingMetadata( lfnDict.keys() )
    if not res['OK']:
      return res
    for lfn, metadata in res['Value'].items():
      runNumber = 0
      if metadata.has_key( "RunNumber" ):
        runNumber = metadata["RunNumber"]
      if not runDict.has_key( runNumber ):
        runDict[runNumber] = {}
      paramValue = metadata.get( param )
      if not runDict[runNumber].has_key( paramValue ):
        runDict[runNumber][paramValue] = []
      runDict[runNumber][paramValue].append( lfn )
    return S_OK( runDict )

  def __getBookkeepingMetadata( self, lfns ):
    bk = BookkeepingClient()
    start = time.time()
    res = bk.getFileMetadata( lfns )
    gLogger.verbose( "Obtained BK file metadata in %.2f seconds" % ( time.time() - start ) )
    if not res['OK']:
      gLogger.error( "Failed to get bookkeeping metadata", res['Message'] )
    return res

  def _lhcbBroadcast( self, master1SE, master2SEs, secondarySEs, numberOfCopies ):
    transID = self.params['TransformationID']
    # Group the remaining data by run
    res = self.__groupByRun( self.files )
    if not res['OK']:
      return res
    runFileDict = res['Value']

    runSitesToUpdate = {}
    # For each of the runs determine the destination of any previous files
    runSiteDict = {}
    if runFileDict:
      res = self.transClient.getTransformationRuns( {'TransformationID':transID, 'RunNumber':runFileDict.keys()} )
      if not res['OK']:
        gLogger.error( "Failed to obtain TransformationRuns", res['Message'] )
        return res
      for runDict in res['Value']:
        selectedSite = runDict['SelectedSite']
        runID = runDict['RunNumber']
        if selectedSite:
          runSiteDict[runID] = selectedSite
          continue
        res = self.transClient.getTransformationFiles( condDict = {'TransformationID':transID, 'RunNumber':runID, 'Status':['Assigned', 'Processed']} )
        if not res['OK']:
          gLogger.error( "Failed to get transformation files for run", "%s %s" % ( runID, res['Message'] ) )
          continue
        if res['Value']:
          assignedSE = res['Value'][0]['UsedSE']
          if assignedSE:
            runSiteDict[runID] = assignedSE
            runSitesToUpdate[runID] = assignedSE

    for runID in sortList( runFileDict.keys() ):
      if runSiteDict.has_key( runID ):
        continue
      runLfns = runFileDict[runID]
      if not runLfns:
        continue
      exampleRunLfn = randomize( runLfns )[0]
      # Get rid of anything that is only is failover
      exampleRunLfnSEs = [se for se in sortList( self.data[exampleRunLfn].keys() ) if not re.search( "-FAILOVER", se )]
      if not exampleRunLfnSEs:
        continue

      # Ensure that we have a master copy at CERN
      runTargetSEs = [master1SE]
      if master1SE in exampleRunLfnSEs:
        exampleRunLfnSEs.remove( master1SE )
      selectedRunTargetSites = ['LCG.CERN.ch']
      # If we know the second master copy use it
      secondMaster = False
      for dummySE in list( exampleRunLfnSEs ):
        if len( runTargetSEs ) >= 2: break
        if dummySE in master2SEs and dummySE not in runTargetSEs:
          secondMasterSite = self._getSiteForSE( dummySE )['Value']
          if not secondMasterSite in selectedRunTargetSites:
            selectedRunTargetSites.append( secondMasterSite )
            runTargetSEs.append( dummySE )
            exampleRunLfnSEs.remove( dummySE )
            secondMaster = dummySE
      # If we do not have a second master copy then select one
      if not secondMaster:
        for possibleSecondMaster in randomize( master2SEs ):
          if len( runTargetSEs ) >= 2: break
          possibleSecondMasterSite = self._getSiteForSE( possibleSecondMaster )['Value']
          if not possibleSecondMasterSite in selectedRunTargetSites and possibleSecondMaster not in runTargetSEs:
            selectedRunTargetSites.append( possibleSecondMasterSite )
            runTargetSEs.append( possibleSecondMaster )
      # Now get select the secondary copies
      for dummySE in randomize( exampleRunLfnSEs ):
        if len( runTargetSEs ) >= numberOfCopies: break
        if dummySE in secondarySEs and dummySE not in runTargetSEs:
            possibleSecondarySite = self._getSiteForSE( dummySE )['Value']
            if not possibleSecondarySite in selectedRunTargetSites:
                selectedRunTargetSites.append( possibleSecondarySite )
                runTargetSEs.append( dummySE )
                exampleRunLfnSEs.remove( dummySE )
      #Now get the remainder of the required secondary copies
      for possibleSecondarySE in randomize( secondarySEs ):
        if len( runTargetSEs ) >= numberOfCopies: break
        if not possibleSecondarySE in runTargetSEs:
            possibleSecondarySite = self._getSiteForSE( possibleSecondarySE )['Value']
            if not possibleSecondarySite in selectedRunTargetSites:
              selectedRunTargetSites.append( possibleSecondarySite )
              runTargetSEs.append( possibleSecondarySE )
      stringrunTargetSEs = ','.join( sortList( runTargetSEs ) )
      runSiteDict[runID] = stringrunTargetSEs
      runSitesToUpdate[runID] = stringrunTargetSEs

    # Update the TransformationRuns table with the assigned (don't continue if it fails)
    for runID, targetSite in runSitesToUpdate.items():
      #if not runID:
      #  continue
      res = self.transClient.setTransformationRunsSite( transID, runID, targetSite )
      if not res['OK']:
        gLogger.error( "Failed to assign TransformationRun site", res['Message'] )
        return S_ERROR( "Failed to assign TransformationRun site" )

    #Now assign the individual files to their targets
    fileTargets = {}
    alreadyCompleted = []
    for fileDict in self.files:
      lfn = fileDict.get( 'LFN', '' )
      if not lfn:
        continue
      runID = fileDict.get( 'RunNumber', 'IGNORE' )
      if runID == 'IGNORE':
        continue
      if not runSiteDict.has_key( runID ):
        continue
      runTargetSEs = sortList( runSiteDict[runID].split( ',' ) )
      existingSites = self._getSitesForSEs( self.data[lfn].keys() )

      fileTargets[lfn] = []
      for runTargetSE in runTargetSEs:
        runTargetSEsite = self._getSiteForSE( runTargetSE )['Value']
        if not runTargetSEsite in existingSites:
          fileTargets[lfn].append( runTargetSE )
      if not fileTargets[lfn]:
        fileTargets.pop( lfn )
        alreadyCompleted.append( lfn )

    # Update the status of the already done files
    if alreadyCompleted:
      gLogger.info( "Found %s files that are already completed" % len( alreadyCompleted ) )
      self.transClient.setFileStatusForTransformation( transID, 'Processed', alreadyCompleted )

    # Now group all of the files by their target SEs
    storageElementGroups = {}
    for lfn, targetSEs in fileTargets.items():
      stringTargetSEs = ','.join( sortList( targetSEs ) )
      if not storageElementGroups.has_key( stringTargetSEs ):
        storageElementGroups[stringTargetSEs] = []
      storageElementGroups[stringTargetSEs].append( lfn )

    # Now create reasonable size tasks
    tasks = []
    for stringTargetSEs in sortList( storageElementGroups.keys() ):
      stringTargetLFNs = storageElementGroups[stringTargetSEs]
      for lfnGroup in breakListIntoChunks( sortList( stringTargetLFNs ), 100 ):
        tasks.append( ( stringTargetSEs, lfnGroup ) )
    return S_OK( tasks )

  def _checkAncestors( self, filesReplicas, ancestorDepth ):
    """ Check ancestor availability on sites. Returns a list of SEs where all the ancestors are present
    """
    # Get the ancestors from the BK
    res = getFileAncestors( filesReplicas.keys(), ancestorDepth )
    if not res['OK']:
      gLogger.warn( res['Message'] )
      return res
    ancestorDict = res['Value']

    # Get the replicas for all the ancestors
    allAncestors = []
    for ancestors in ancestorDict.values():
      allAncestors.extend( ancestors )
    rm = ReplicaManager()
    startTime = time.time()
    res = rm.getActiveReplicas( allAncestors )
    if not res['OK']:
      return res
    for lfn in res['Value']['Failed'].keys():
      self.data.pop( lfn )
    gLogger.info( "Replica results for %d files obtained in %.2f seconds" % ( len( res['Value']['Successful'] ), time.time() - startTime ) )
    ancestorReplicas = res['Value']['Successful']

    seSiteCache = {}
    dataLfns = self.data.keys()
    for lfn in dataLfns:
      lfnSEs = self.data[lfn].keys()
      lfnSites = {}
      for se in lfnSEs:
        if not seSiteCache.has_key( se ):
          seSiteCache[se] = self._getSiteForSE( se )['Value']
        lfnSites[seSiteCache[se]] = se
      for ancestorLfn in ancestorDict[lfn]:
        ancestorSEs = ancestorReplicas[ancestorLfn]
        for se in ancestorSEs:
          if not seSiteCache.has_key( se ):
            seSiteCache[se] = self._getSiteForSE( se )['Value']
          ancestorSites.append( seSiteCache[se] )
        for lfnSite in lfnSites.keys():
          if not lfnSite in ancestorSites:
            if self.data[lfn].has_key( lfnSites[lfnSite] ):
              self.data[lfn].pop( lfnSites[lfnSite] )
    ancestorProblems = []
    for lfn, replicas in self.data.items():
      if len( replicas ) == 0:
        gLogger.error( "File ancestors not found at corresponding sites", lfn )
        ancestorProblems.append( lfn )
    return S_OK()

  def _LHCbMCDSTBroadcastRandom( self ):
    """ This plug-in broadcasts files to master1, to master2 and to (NumberOfReplicas-2) secondary SEs  """

    master1SEs = self.params.get( 'Master1SEs', ['CERN_MC_M-DST'] )
    master2SEs = self.params.get( 'Master2SEs', ['CNAF_MC_M-DST', 'GRIDKA_MC_M-DST', 'IN2P3_MC_M-DST', 'NIKHEF_MC_M-DST', 'PIC_MC_M-DST', 'RAL_MC_M-DST'] )
    secondarySEs = self.params.get( 'SecondarySEs', ['CNAF_MC-DST', 'GRIDKA_MC-DST', 'IN2P3_MC-DST', 'NIKHEF_MC-DST', 'PIC_MC-DST', 'RAL_MC-DST'] )
    numberOfCopies = int( self.params.get( 'NumberOfReplicas', 3 ) )

    master1ActiveSEs = self.__getActiveSEs( master1SEs )
    master2ActiveSEs = self.__getActiveSEs( master2SEs )
    secondaryActiveSEs = self.__getActiveSEs( secondarySEs )

    storageElementGroups = {}

    replicaGroups = self._getFileGroups( self.data )

    for replicaSE, lfns in replicaGroups.items():

      existingSEs = replicaSE.split( ',' )

      master1se = []
      for se in existingSEs:
        if se in master1SEs:
          master1se.append( se )
      for se in master1se:
        existingSEs.remove( se )

      master2se = []
      for se in existingSEs:
        if se in master2SEs:
          master2se.append( se )
      for se in master2se:
        existingSEs.remove( se )

      secondaryses = []
      for se in existingSEs:
        if se in secondarySEs:
          secondaryses.append( se )

      if master1se and master2se and len( secondaryses ) >= numberOfCopies - 2:
        gLogger.info( "Found %s files that are already completed" % len( lfns ) )
        continue

      targetSEs = []

      if not master1se:
        if master1ActiveSEs:
          se = randomize( master1ActiveSEs )[0]
          targetSEs.append( se )
        else:
          gLogger.warning( "Can not select Active SE for Master1SE" )
          continue

      if not master2se:
        if master2ActiveSEs:
          se = randomize( master2ActiveSEs )[0]
          targetSEs.append( se )
        else:
          gLogger.warning( "Can not select Active SE for Master2SE" )
          continue

      if len( secondaryses ) < numberOfCopies - 2:
        candidateSEs = copy.copy( secondaryActiveSEs )
        needtocopy = numberOfCopies - 2
        for se in secondaryses:
          needtocopy = needtocopy - 1
          if se in candidateSEs:
            candidateSEs.remove( se )

        if len( candidateSEs ) >= needtocopy:
          candidateSEs = randomize( candidateSEs )
          for icopy in range( needtocopy ):
            targetSEs.append( candidateSEs[icopy] )
        else:
          gLogger.warning( "Can not select Active SE for SecondarySE" )
          continue

      if targetSEs:
        stringTargetSEs = ','.join( sortList( targetSEs ) )
        if not storageElementGroups.has_key( stringTargetSEs ):
          storageElementGroups[stringTargetSEs] = []
        storageElementGroups[stringTargetSEs].extend( lfns )

     # Now create reasonable size tasks
    tasks = []
    for stringTargetSEs in sortList( storageElementGroups.keys() ):
      stringTargetLFNs = storageElementGroups[stringTargetSEs]
      for lfnGroup in breakListIntoChunks( sortList( stringTargetLFNs ), 100 ):
        tasks.append( ( stringTargetSEs, lfnGroup ) )

    return S_OK( tasks )

  def __getActiveSEs( self, selist ):
    activeSE = []
    for se in selist:
      res = gConfig.getOption( '/Resources/StorageElements/%s/WriteAccess' % se, 'Unknown' )
      if res['OK'] and res['Value'] == 'Active':
        activeSE.append( se )
    return activeSE

  def _ReplicateDataset( self ):
    destSEs = self.params.get( 'destinationSEs', ["CERN-ARCHIVE"] )
    if type( DestSEs ) == types.StringType:
      destSEs = [destSEs]
    numberOfCopies = int( self.params.get( 'NumberOfReplicas', len( destSEs ) ) )
    return self.__simpleReplication( destSEs, numberOfCopies )

  def _ArchiveDataset( self ):
    return self.__simpleReplication( "CERN-ARCHIVE" )

  def __simpleReplication( self, destSEs, numberOfCopies = 0 ):
    if type( destSEs ) == types.StringType:
      destSEs = [destSEs]
    if not numberOfCopies:
      numberOfCopies = len( destSEs )
    replicaGroups = self._getFileGroups( self.data )
    storageElementGroups = {}
    for replicaSE, lfnGroup in replicaGroups.items():
      existingSEs = replicaSE.split( ',' )
      # If there is no choice on the SEs, send all files at once, otherwise make chunks
      if numberOfCopies >= len( destSEs ):
        lfnChunks = [lfnGroup]
      else:
        lfnChunks = breakListIntoChunks( lfnGroup, 50 )
      for lfns in lfnChunks:
        candidateSEs = randomize( destSEs )
        for se in existingSEs:
          if se in candidateSEs:
            candidateSEs.remove( se )
        activeSEs = self.__getActiveSEs( candidateSEs )
        if numberOfCopies <= len( activeSEs ):
          targetSEs = activeSEs[0:numberOfCopies]
        else:
          targetSEs = activeSEs
          needToCopy = numberOfCopies - len( targetSEs )
          for se in candidateSEs:
            if needToCopy == 0: break
            if se not in targetSEs:
              targetSEs.append( se )
              needToCopy -= 1
        if targetSEs:
          stringTargetSEs = ','.join( sortList( targetSEs ) )
          if not storageElementGroups.has_key( stringTargetSEs ):
            storageElementGroups[stringTargetSEs] = []
          storageElementGroups[stringTargetSEs].extend( lfns )

      # Now create reasonable size tasks
    tasks = []
    for stringTargetSEs in sortList( storageElementGroups.keys() ):
      stringTargetLFNs = storageElementGroups[stringTargetSEs]
      for lfnGroup in breakListIntoChunks( sortList( stringTargetLFNs ), 100 ):
        tasks.append( ( stringTargetSEs, lfnGroup ) )

    return S_OK( tasks )







