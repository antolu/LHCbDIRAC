"""  TransformationPlugin is a class wrapping the supported LHCb transformation plugins
"""
from DIRAC                                                             import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.Core.Utilities.SiteSEMapping                                import getSitesForSE, getSEsForSite
from DIRAC.Core.Utilities.List                                         import breakListIntoChunks, sortList, randomize
from DIRAC.DataManagementSystem.Client.ReplicaManager                  import ReplicaManager
from LHCbDIRAC.NewBookkeepingSystem.Client.AncestorFiles                  import getAncestorFiles
from LHCbDIRAC.NewBookkeepingSystem.Client.BookkeepingClient              import BookkeepingClient
from LHCbDIRAC.TransformationSystem.Client.TransformationClient        import TransformationClient
import time, re, types

from DIRAC.TransformationSystem.Agent.TransformationPlugin               import TransformationPlugin as DIRACTransformationPlugin

class TransformationPlugin( DIRACTransformationPlugin ):

  def __init__( self, plugin, transClient = None, replicaManager = None ):
    if not transClient:
      self.transClient = TransformationClient()
    DIRACTransformationPlugin.__init__( self, plugin, transClient = transClient, replicaManager = replicaManager )

  def _checkAncestors( self, filesReplicas, ancestorDepth ):
    """ Check ancestor availability on sites. Returns a list of SEs where all the ancestors are present
    """
    # Get the ancestors from the BK
    res = getAncestorFiles( filesReplicas.keys(), ancestorDepth )
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
    ancestorSites = []
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

  def _AtomicRun( self ):
    #possibleTargets = ['LCG.CERN.ch', 'LCG.CNAF.it', 'LCG.GRIDKA.de', 'LCG.IN2P3.fr', 'LCG.PIC.es', 'LCG.RAL.uk', 'LCG.SARA.nl']
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

    runSEsToUpdate = {}
    # For each of the runs determine the destination of any previous files
    runSEDict = {}
    if runFileDict:
      res = self.transClient.getTransformationRuns( {'TransformationID':transID, 'RunNumber':runFileDict.keys()} )
      if not res['OK']:
        gLogger.error( "Failed to obtain TransformationRuns", res['Message'] )
        return res
      for runDict in res['Value']:
        selectedSite = runDict['SelectedSite']
        runID = runDict['RunNumber']
        if selectedSite:
          runSEDict[runID] = selectedSite
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
              runSEDict[runID] = site
              runSEsToUpdate[runID] = site

    # Choose the destination site for new runs
    for runID in sortList( runFileDict.keys() ):
      if runID in runSEDict.keys():
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
      runSEDict[runID] = targetSite
      runSEsToUpdate[runID] = targetSite

    # Create the tasks
    tasks = []
    for runID in sortList( runSEDict.keys() ):
      targetSite = runSEDict[runID]
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
    for runID, targetSite in runSEsToUpdate.items():
      res = self.transClient.setTransformationRunsSite( transID, runID, targetSite )
      if not res['OK']:
        gLogger.error( "Failed to assign TransformationRun site", res['Message'] )
        continue
    return S_OK( tasks )

  def __groupByRun( self, files ):
    runFiles = {}
    for fileDict in files:
      runNumber = fileDict.get( 'RunNumber', 0 )
      if runNumber == None:
        runNumber = 0
      lfn = fileDict.get( 'LFN', '' )
      if runNumber not in runFiles.keys():
        runFiles[runNumber] = []
      if lfn:
        runFiles[runNumber].append( lfn )
    return S_OK( runFiles )

  def _RAWShares( self ):
    possibleTargets = ['CNAF-RAW', 'GRIDKA-RAW', 'IN2P3-RAW', 'SARA-RAW', 'PIC-RAW', 'RAL-RAW']
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
    runSEDict = {}
    if runFileDict:
      res = self.transClient.getTransformationRuns( {'TransformationID':transID, 'RunNumber':runFileDict.keys()} )
      if not res['OK']:
        gLogger.error( "Failed to obtain TransformationRuns", res['Message'] )
        return res
      for runDict in res['Value']:
        runSEDict[runDict['RunNumber']] = runDict['SelectedSite']

    # Choose the destination SE
    tasks = []
    for runID in sortList( runFileDict.keys() ):
      unusedLfns = runFileDict[runID][None]
      assignedSE = None
      if not runSEDict.has_key( runID ):
        continue
      if runSEDict[runID]:
        assignedSE = runSEDict[runID]
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

  def __getFilesStatForRun( self, transID, runID ):
    selectDict = {'TransformationID':transID}
    if runID:
      selectDict['RunNumber'] = runID
    res = self.transClient.getTransformationFiles( selectDict )
    files = 0
    processed = 0
    if res['OK']:
      for fileDict in res['Value']:
        files += 1
        if fileDict['Status'] == "Processed":
          processed += 1
    return ( files, processed )

  def __mergeByRun( self, requireFlush = False ):

    transID = self.params['TransformationID']
    if requireFlush:
      res = self.transClient.getBookkeepingQueryForTransformation( transID )
      queryProdID = None
      if res['OK']:
        queryProdID = res['Value'].get( 'ProductionID' )
      if not queryProdID:
        gLogger.warn( "Could not find ancestor production for transformation %d. Flush impossible" % transID )
        requireFlush = False

    res = self.__groupByRun( self.files )
    if not res['OK']:
      return res
    runFiles = res['Value']

    allReplicas = self.data.copy()
    allTasks = []
    res = self.transClient.getTransformationRuns( {'TransformationID':transID, 'RunNumber':runFiles.keys()} )
    if not res['OK']:
      return res
    transStatus = self.params['Status']
    for runDict in res['Value']:
      runID = runDict['RunNumber']
      runStatus = runDict['Status']
      runLfns = runFiles.get( runID, [] )
      if not runLfns:
      #  if requireFlush:
      #    self.transClient.setTransformationRunStatus( transID, runID, 'Flush' )
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
      status = runStatus
      if transStatus == 'Flush':
        status = 'Flush'
      elif requireFlush:
        # If all files in that run have been processed by the mother production, flush
        processingStats = [0, 0]
        for prod in queryProdID:
          ( files, processed ) = self.__getFilesStatForRun( prod, runID )
          processingStats[0] += files
          processingStats[1] += processed
        files = processingStats[0]
        processed = processingStats[1]
        if files and files == processed:
          status = 'Flush'
          gLogger.info( "All files processed for run %d in productions %s, flush in transformation %d" % ( runID, str( queryProdID ), transID ) )
      self.params['Status'] = status
      res = self._BySize()
      self.params['Status'] = transStatus
      if not res['OK']:
        return res
      allTasks.extend( res['Value'] )
      if requireFlush:
        self.transClient.setTransformationRunStatus( transID, runID, 'Flush' )
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
    archive1SEs = self.params.get( 'Archive1SEs', ['CERN-ARCHIVE'] )
    archive2SEs = self.params.get( 'Archive2SEs', ['CNAF-ARCHIVE', 'GRIDKA-ARCHIVE', 'IN2P3-ARCHIVE', 'SARA-ARCHIVE', 'PIC-ARCHIVE', 'RAL-ARCHIVE'] )
    mandatorySEs = self.params.get( 'MandatorySEs', ['CERN_M-DST'] )
    secondarySEs = self.params.get( 'SecondarySEs', ['CNAF-DST', 'GRIDKA-DST', 'IN2P3-DST', 'SARA-DST', 'PIC-DST', 'RAL-DST'] )
    numberOfCopies = int( self.params.get( 'NumberOfReplicas', 4 ) )
    return self._lhcbBroadcast( archive1SEs, archive2SEs, mandatorySEs, secondarySEs, numberOfCopies )

  def _LHCbMCDSTBroadcast( self ):
    archive1SEs = self.params.get( 'Archive1SEs', ['CERN-ARCHIVE'] )
    archive2SEs = self.params.get( 'Archive2SEs', ['CNAF-ARCHIVE', 'GRIDKA-ARCHIVE', 'IN2P3-ARCHIVE', 'SARA-ARCHIVE', 'PIC-ARCHIVE', 'RAL-ARCHIVE'] )
    mandatorySEs = self.params.get( 'MandatorySEs', ['CERN_MC_M-DST'] )
    secondarySEs = self.params.get( 'SecondarySEs', ['CNAF_MC-DST', 'GRIDKA_MC-DST', 'IN2P3_MC-DST', 'SARA_MC-DST', 'PIC_MC-DST', 'RAL_MC-DST'] )
    numberOfCopies = int( self.params.get( 'NumberOfReplicas', 3 ) )
    return self._lhcbBroadcast( archive1SEs, archive2SEs, mandatorySEs, secondarySEs, numberOfCopies )

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

  def _lhcbBroadcast( self, archive1SEs, archive2SEs, mandatorySEs, secondarySEs, numberOfCopies ):
    """ This plug-in broadcasts files to one archive1SE, one archive2SE and numberOfCopies secondarySEs"""
    archive1SEs = self.__getListFromString( archive1SEs )
    archive2SEs = self.__getListFromString( archive2SEs )
    mandatorySEs = self.__getListFromString( mandatorySEs )
    secondarySEs = self.__getListFromString( secondarySEs )
    # Select active SEs
    archive1ActiveSEs = self.__getActiveSEs( archive1SEs )
    if not archive1ActiveSEs:
      archive1ActiveSEs = archive1SEs
    archive2ActiveSEs = self.__getActiveSEs( archive2SEs )
    if not archive2ActiveSEs:
      archive2ActiveSEs = archive2SEs
    secondaryActiveSEs = self.__getActiveSEs( secondarySEs )

    # We need at least all mandatory copies + 2 (archive1 and archive2)
    numberOfCopies = max( numberOfCopies, len( mandatorySEs ) )
    numberOfCopies += 2

    transID = self.params['TransformationID']

    # Group the remaining data by run
    res = self.__groupByRun( self.files )
    if not res['OK']:
      return res
    runFileDict = res['Value']

    runSEsToUpdate = {}
    # For each of the runs determine the destination of any previous files
    runSEDict = {}
    if runFileDict:
      # Make a list of SEs already assigned to runs
      res = self.transClient.getTransformationRuns( {'TransformationID':transID, 'RunNumber':runFileDict.keys()} )
      if not res['OK']:
        gLogger.error( "Failed to obtain TransformationRuns", res['Message'] )
        return res
      for runDict in res['Value']:
        selectedSite = runDict['SelectedSite']
        runID = runDict['RunNumber']
        if selectedSite:
          runSEDict[runID] = selectedSite
          continue
        res = self.transClient.getTransformationFiles( condDict = {'TransformationID':transID, 'RunNumber':runID, 'Status':['Assigned', 'Processed']} )
        if not res['OK']:
          gLogger.error( "Failed to get transformation files for run", "%s %s" % ( runID, res['Message'] ) )
          continue
        if res['Value']:
          assignedSE = res['Value'][0]['UsedSE']
          if assignedSE:
            runSEDict[runID] = assignedSE
            runSEsToUpdate[runID] = assignedSE

    for runID in sortList( runFileDict.keys() ):
      if runSEDict.has_key( runID ):
        continue
      runLfns = runFileDict[runID]
      if not runLfns:
        continue
      # Select candidate SEs where most of the files are already
      SEFrequency = {}
      for lfn in runLfns:
        for se in self.data[lfn].keys():
          if se.endswith( "-FAILOVER" ):
            continue
          if not SEFrequency.has_key( se ):
            SEFrequency[se] = 0
          SEFrequency[se] += 1
      maxFreq = 0
      runLfnSEs = []
      for se in SEFrequency.keys():
        if SEFrequency[se] > maxFreq:
          maxFreq = SEFrequency[se]
          runLfnSEs = [se]
        elif SEFrequency[se] == maxFreq:
          runLfnSEs.append( se )
      #exampleRunLfn = randomize( runLfns )[0]
      # Get rid of anything that is only is failover
      #runLfnSEs = [se for se in sortList( self.data[exampleRunLfn].keys() ) if not re.search( "-FAILOVER", se )]
      if not runLfnSEs:
        continue

      selectedRunTargetSites = []
      # Ensure that we have a archive1 copy
      archive1SE = None
      for se in list( runLfnSEs ) + randomize( archive1ActiveSEs ):
        if se in archive1ActiveSEs:
          archive1SE = se
          if not archive1SE.endswith( '-ARCHIVE' ):
            selectedRunTargetSites.append( self._getSiteForSE( archive1SE )['Value'] )
          runTargetSEs = [archive1SE]
          break
      if not archive1SE:
        gLogger.error( 'Cannot select archive1SE in active SEs' )
        continue

      # If we know the second archive copy use it
      archive2SE = None
      for se in list( runLfnSEs ) + randomize( archive2ActiveSEs ):
        if se in archive2ActiveSEs and se not in runTargetSEs:
          archive2Site = self._getSiteForSE( se )['Value']
          if not archive2Site in selectedRunTargetSites:
            archive2SE = se
            if not archive2SE.endswith( '-ARCHIVE' ):
              selectedRunTargetSites.append( archive2Site )
            runTargetSEs.append( archive2SE )
            break
      # If we do not have a second archive copy then select one
      if not archive2SE:
        gLogger.error( 'Cannot select archive2SE in active SEs' )
        continue

      # Now select the secondary copies
      for se in list( mandatorySEs ) + randomize( runLfnSEs ) + randomize( secondaryActiveSEs ):
        if len( runTargetSEs ) >= numberOfCopies: break
        if se in mandatorySEs + secondaryActiveSEs and se not in runTargetSEs:
            secondarySite = self._getSiteForSE( se )['Value']
            if not secondarySite in selectedRunTargetSites:
                selectedRunTargetSites.append( secondarySite )
                runTargetSEs.append( se )
      if len( runTargetSEs ) < numberOfCopies:
        gLogger.error( 'Could not find enough active secondary SEs' )
        continue

      stringrunTargetSEs = ','.join( sortList( runTargetSEs ) )
      runSEDict[runID] = stringrunTargetSEs
      runSEsToUpdate[runID] = stringrunTargetSEs

    # Update the TransformationRuns table with the assigned (don't continue if it fails)
    for runID, targetSite in runSEsToUpdate.items():
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
      if runID == None:
        runID = 0
      if runID == 'IGNORE':
        continue
      if not runSEDict.has_key( runID ):
        continue
      runTargetSEs = sortList( runSEDict[runID].split( ',' ) )
      existingSEs = [se for se in self.data[lfn].keys() if not se.endswith( "-ARCHIVE" )]
      existingSites = self._getSitesForSEs( existingSEs )

      fileTargets[lfn] = []
      for runTargetSE in runTargetSEs:
        if runTargetSE.endswith( "-ARCHIVE" ) or not self._getSiteForSE( runTargetSE )['Value'] in existingSites:
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

  def _LHCbMCDSTBroadcastRandom( self ):
    """ This plug-in broadcasts files to archive1, to archive2 and to (NumberOfReplicas) secondary SEs  """

    transID = self.params['TransformationID']
    archive1SEs = self.params.get( 'Archive1SEs', ['CERN-ARCHIVE'] )
    archive2SEs = self.params.get( 'Archive2SEs', ['CNAF-ARCHIVE', 'GRIDKA-ARCHIVE', 'IN2P3-ARCHIVE', 'SARA-ARCHIVE', 'PIC-ARCHIVE', 'RAL-ARCHIVE'] )
    mandatorySEs = self.params.get( 'MandatorySEs', ['CERN_MC_M-DST'] )
    secondarySEs = self.params.get( 'SecondarySEs', ['CNAF_MC-DST', 'GRIDKA_MC-DST', 'IN2P3_MC-DST', 'SARA_MC-DST', 'PIC_MC-DST', 'RAL_MC-DST'] )
    numberOfCopies = int( self.params.get( 'NumberOfReplicas', 3 ) )

    archive1SEs = self.__getListFromString( archive1SEs )
    archive2SEs = self.__getListFromString( archive2SEs )
    mandatorySEs = self.__getListFromString( mandatorySEs )
    secondarySEs = self.__getListFromString( secondarySEs )
    # Select active SEs only
    archive1ActiveSEs = self.__getActiveSEs( archive1SEs )
    if not archive1ActiveSEs:
      archive1ActiveSEs = archive1SEs
    archive2ActiveSEs = self.__getActiveSEs( archive2SEs )
    if not archive2ActiveSEs:
      archive2ActiveSEs = archive2SEs
    secondaryActiveSEs = self.__getActiveSEs( secondarySEs )

    # We need at least all mandatory copies + 2 (archive1 and archive2)
    numberOfCopies = max( numberOfCopies, len( mandatorySEs ) )

    storageElementGroups = {}

    replicaGroups = self._getFileGroups( self.data )

    for replicaSE, lfns in replicaGroups.items():

      existingSEs = replicaSE.split( ',' )
      # Make a list of sites where the file already is in order to avoid duplicate copies
      targetSites = []
      for se in existingSEs:
        res = getSitesForSE( se )
        if res['OK']:
          targetSites += res['Value']

      archive1SE = []
      for se in existingSEs:
        if se in archive1SEs:
          archive1SE.append( se )
      for se in archive1SE:
        existingSEs.remove( se )

      archive2SE = []
      for se in existingSEs:
        if se in archive2ActiveSEs:
          archive2SE.append( se )
      for se in archive2SE:
        existingSEs.remove( se )

      missingses = []
      for se in mandatorySEs:
        if not se in existingSEs:
          missingses.append( se )

      secondaryses = []
      for se in existingSEs:
        if se in mandatorySEs + secondarySEs:
          secondaryses.append( se )

      if archive1SE and archive2SE and not missingses and len( secondaryses ) >= numberOfCopies:
        gLogger.info( "Found %s files that are already completed" % len( lfns ) )
        continue

      targetSEs = []

      needtocopy = 1
      if len( archive1SE ) < needtocopy:
        if archive1ActiveSEs:
          for se in randomize( archive1ActiveSEs ):
            if needtocopy <= 0:
              break
            targetSEs.append( se )
            needtocopy -= 1
            if not se.endswith( '-ARCHIVE' ):
              res = getSitesForSE( se )
              if res['OK']:
                targetSites += res['Value']
        if needtocopy > 0 :
          gLogger.error( "Can not select Active SE for archive1SE" )
          continue

      needtocopy = 1
      if len( archive2SE ) < needtocopy:
        if archive2ActiveSEs:
          for se in randomize( archive2ActiveSEs ):
            if needtocopy <= 0:
              break
            targetSEs.append( se )
            needtocopy -= 1
            if not se.endswith( '-ARCHIVE' ):
              res = getSitesForSE( se )
              if res['OK']:
                targetSites += res['Value']
        if needtocopy > 0:
          gLogger.error( "Can not select Active SE for archive2SE" )
          continue

      needtocopy = numberOfCopies
      if len( secondaryses ) < needtocopy:
        # Missing secondary copies, make a list of candidates
        candidateSEs = mandatorySEs + randomize( secondaryActiveSEs )
        #print secondaryses, candidateSEs, needtocopy
        for se in secondaryses:
          needtocopy -= 1
          if se in candidateSEs:
            candidateSEs.remove( se )
        #print candidateSEs, needtocopy

        if len( candidateSEs ) >= needtocopy:
          for se in candidateSEs:
            if needtocopy <= 0:
              break
            res = getSitesForSE( se )
            if res['OK' ]:
              sites = res['Value']
              site = None
              for site in sites:
                if site in targetSites:
                  site = None
                  break
              if site:
                targetSEs.append( se )
                targetSites += sites
                needtocopy -= 1
        if needtocopy > 0:
          gLogger.error( "Can not select enough Active SEs for SecondarySE" )
          continue

      if targetSEs:
        stringTargetSEs = ','.join( sortList( targetSEs ) )
        if not storageElementGroups.has_key( stringTargetSEs ):
          storageElementGroups[stringTargetSEs] = []
        storageElementGroups[stringTargetSEs].extend( lfns )
      else:
        gLogger.info( "Found %s files that are already completed" % len( lfns ) )
        self.transClient.setFileStatusForTransformation( transID, 'Processed', lfns )

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
    destSEs = self.params.get( 'DestinationSEs' )
    if not destSEs:
      gLogger.info( "_ReplicateDataset plugin: no destination SEs" )
      return S_OK( [] )
    numberOfCopies = int( self.params.get( 'NumberOfReplicas', 0 ) )
    return self.__simpleReplication( destSEs, [], numberOfCopies )

  def _ArchiveDataset( self ):
    archive1SEs = self.params.get( 'Archive1SEs', ['CERN-ARCHIVE'] )
    archive2SEs = self.params.get( 'Archive2SEs', ['CNAF-ARCHIVE', 'GRIDKA-ARCHIVE', 'IN2P3-ARCHIVE', 'SARA-ARCHIVE', 'PIC-ARCHIVE', 'RAL-ARCHIVE'] )
    archive1SEs = self.__getListFromString( archive1SEs )
    archive2SEs = self.__getListFromString( archive2SEs )
    archive1ActiveSEs = self.__getActiveSEs( archive1SEs )
    if not archive1ActiveSEs:
      archive1ActiveSEs = archive1SEs
    archive2ActiveSEs = self.__getActiveSEs( archive2SEs )
    if not archive2ActiveSEs:
      archive2ActiveSEs = archive2SEs
    archive1SE = randomize( archive1ActiveSEs )[0]
    return self.__simpleReplication( [archive1SE], archive2ActiveSEs, numberOfCopies = 2 )

  def __getListFromString( self, s ):
    # Avoid using eval()... painful
    if type( s ) == types.StringType:
      if s == "[]":
        return []
      if s[0] == '[' and s[-1] == ']':
        s = s[1:-1]
      l = s.split( ',' )
      ll = []
      for a in l:
        a = a.strip()
        if not a:
          ll.append( a )
        elif a[0] == "'" and a[-1] == "'":
          ll.append( a[1:-1] )
        elif a[0] == '"' and a[-1] == '"':
          ll.append( a[1:-1] )
        else:
          ll.append( a )
      return ll
    return s

  def __simpleReplication( self, mandatorySEs, destSEs, numberOfCopies = 0 ):
    transID = self.params['TransformationID']
    secondarySEs = self.__getListFromString( destSEs )
    activeSecondarySEs = self.__getActiveSEs( secondarySEs )
    if not numberOfCopies:
      numberOfCopies = len( secondarySEs ) + len( mandatorySEs )
    numberOfCopies = max( len( mandatorySEs ), numberOfCopies )

    replicaGroups = self._getFileGroups( self.data )

    storageElementGroups = {}

    for replicaSE, lfnGroup in replicaGroups.items():
      existingSEs = replicaSE.split( ',' )
      # If there is no choice on the SEs, send all files at once, otherwise make chunks
      if numberOfCopies >= len( mandatorySEs ) + len( activeSecondarySEs ):
        lfnChunks = [lfnGroup]
      else:
        lfnChunks = breakListIntoChunks( lfnGroup, 100 )

      for lfns in lfnChunks:
        targetSEs = []
        candidateSEs = mandatorySEs + randomize( activeSecondarySEs )
        # Remove existing SEs from list of candidates
        for se in existingSEs:
          if se in candidateSEs:
            numberOfCopies -= 1
            candidateSEs.remove( se )
        if numberOfCopies <= len( candidateSEs ):
          targetSEs = candidateSEs[0:numberOfCopies]
        else:
          targetSEs = candidateSEs
          needToCopy = numberOfCopies - len( targetSEs )
          # Try and replicate to non active SEs
          for se in destSEs:
            if needToCopy == 0: break
            if se not in targetSEs:
              targetSEs.append( se )
              needToCopy -= 1
        if targetSEs:
          stringTargetSEs = ','.join( sortList( targetSEs ) )
          if not storageElementGroups.has_key( stringTargetSEs ):
            storageElementGroups[stringTargetSEs] = []
          storageElementGroups[stringTargetSEs].extend( lfns )
        else:
          gLogger.info( "Found %s files that are already completed" % len( lfns ) )
          self.transClient.setFileStatusForTransformation( transID, 'Processed', lfns )

      # Now create reasonable size tasks
    tasks = []
    for stringTargetSEs in sortList( storageElementGroups.keys() ):
      stringTargetLFNs = storageElementGroups[stringTargetSEs]
      for lfnGroup in breakListIntoChunks( sortList( stringTargetLFNs ), 100 ):
        tasks.append( ( stringTargetSEs, lfnGroup ) )

    return S_OK( tasks )

  def _DestroyDataset( self ):
    return self.__removeReplicas( keepSEs = [], minKeep = 0 )

  def _DeleteDataset( self ):
    keepSEs = self.params.get( 'keepSEs', ['CERN-ARCHIVE', 'CNAF-ARCHIVE', 'GRIDKA-ARCHIVE', 'IN2P3-ARCHIVE', 'NIKHEF-ARCHIVE', 'SARA-ARCHIVE', 'PIC-ARCHIVE', 'RAL-ARCHIVE'] )
    return self.__removeReplicas( keepSEs = keepSEs, minKeep = 0 )

  def _DeleteReplicas( self ):
    listSEs = self.params.get( 'fromSEs', None )
    keepSEs = self.params.get( 'keepSEs', ['CERN-ARCHIVE', 'CNAF-ARCHIVE', 'GRIDKA-ARCHIVE', 'IN2P3-ARCHIVE', 'NIKHEF-ARCHIVE', 'SARA-ARCHIVE', 'PIC-ARCHIVE', 'RAL-ARCHIVE'] )
    keepSEs = self.__getListFromString( keepSEs )
    mandatorySEs = self.params.get( 'mandatorySEs', ['CERN_MC_M-DST', 'CERN_M-DST', 'CERN-DST', 'CERN_MC-DST'] )
    mandatorySEs = self.__getListFromString( mandatorySEs )
    # this is the number of replicas to be kept in addition to keepSEs and mandatorySEs
    minKeep = int( self.params.get( 'NumberOfReplicas', 1 ) )

    return self.__removeReplicas( listSEs = listSEs, keepSEs = keepSEs + mandatorySEs, minKeep = minKeep )

  def __removeReplicas( self, listSEs = [], keepSEs = [], minKeep = 999 ):
    transID = self.params['TransformationID']
    listSEs = self.__getListFromString( listSEs )
    keepSEs = self.__getListFromString( keepSEs )
    nKeep = min( 2, len( keepSEs ) )

    replicaGroups = self._getFileGroups( self.data )

    storageElementGroups = {}
    for replicaSE, lfns in replicaGroups.items():
      replicaSE = replicaSE.split( ',' )
      if minKeep == 0 and keepSEs:
        # Check that the dataset exists at least at 2 keepSE
        if len( [se for se in replicaSE if se in keepSEs] ) < nKeep:
          gLogger.info( "Found %d files that are not in %d keepSEs, no removal done" % ( len( lfns ), nKeep ) )
          self.transClient.setFileStatusForTransformation( transID, 'Processed', lfns )
          continue
      existingSEs = [se for se in replicaSE if not se in keepSEs]
      #print "Existing:",existingSEs,", minKeep", minKeep, ", keepSEs", keepSEs
      if minKeep == 0:
        targetSEs = existingSEs
      else:
        targetSEs = []
        if len( existingSEs ) > minKeep:
          # explicit deletion
          if listSEs:
            # check how  many replicas would be left if we remove from listSEs
            nLeft = len( [se for se in existingSEs if not se in listSEs] )
            if nLeft >= minKeep:
              # we can delete all replicas in listSEs
              targetSEs = listSEs
            else:
              # we should keep some in listSEs, too bad
              targetSEs = randomize( listSEs )[0:minKeep - nLeft]
              gLogger.info( "Found %d files that could only be deleted in %s of the requested SEs" % ( len( lfns ), minKeep - nLeft ) )
          else:
            # remove all replicas and keep only minKeep
            targetSEs = randomize( existingSEs )[0:-minKeep]

      if targetSEs:
        stringTargetSEs = ','.join( sortList( targetSEs ) )
        if not storageElementGroups.has_key( stringTargetSEs ):
          storageElementGroups[stringTargetSEs] = []
          storageElementGroups[stringTargetSEs].extend( lfns )
      else:
        gLogger.info( "Found %s files that don't need any replica deletion" % len( lfns ) )
        self.transClient.setFileStatusForTransformation( transID, 'Processed', lfns )

      # Now create reasonable size tasks
    tasks = []
    for stringTargetSEs in sortList( storageElementGroups.keys() ):
      stringTargetLFNs = storageElementGroups[stringTargetSEs]
      for lfnGroup in breakListIntoChunks( sortList( stringTargetLFNs ), 100 ):
        tasks.append( ( stringTargetSEs, lfnGroup ) )

    return S_OK( tasks )
