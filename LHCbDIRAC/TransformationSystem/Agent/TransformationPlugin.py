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
    res = self.__groupByRun()
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

  def __groupByRun( self, files = None ):
    if files == None:
      files = self.files
    runDict = {}
    for fileDict in files:
      runNumber = fileDict.get( 'RunNumber', 0 )
      if runNumber == None:
        runNumber = 0
      lfn = fileDict.get( 'LFN' )
      if not runDict.has_key( runNumber ):
        runDict[runNumber] = []
      if lfn:
        runDict[runNumber].append( lfn )
    return S_OK( runDict )

  def __groupByRunAndParam( self, lfns, param = '' ):
    runDict = {}
    if type( lfns ) == type( {} ):
      lfns = lfns.keys()
    if not param:
      # no need to query the BK as we have the answer from self.files
      files = [ fileDict for fileDict in self.files if fileDict['LFN'] in lfns]
      res = self.__groupByRun( files = files )
      for runNumber, lfns in res['Value'].items():
        runDict[runNumber] = {None:lfns}
      return S_OK( runDict )

    res = self.__getBookkeepingMetadata( lfns )
    if not res['OK']:
      return res
    for lfn, metadata in res['Value'].items():
      runNumber = metadata.get( "RunNumber", 0 )
      if runNumber == None:
        runNumber = 0
      if not runDict.has_key( runNumber ):
        runDict[runNumber] = {}
      paramValue = metadata.get( param )
      if not runDict[runNumber].has_key( paramValue ):
        runDict[runNumber][paramValue] = []
      runDict[runNumber][paramValue].append( lfn )
    return S_OK( runDict )

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
      res = self.__groupByRunAndParam( lfns )
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
    res = self.__groupByRunAndParam( self.data )
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
      else:
        if type( queryProdID ) != type( [] ):
          queryProdID = [queryProdID]

    res = self.__groupByRun()
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

  def _ByRun( self, param = '', plugin = 'Standard', requireFlush = False ):
    transID = self.params['TransformationID']
    if requireFlush:
      res = self.transClient.getBookkeepingQueryForTransformation( transID )
      queryProdID = None
      if res['OK']:
        queryProdID = res['Value'].get( 'ProductionID' )
      if not queryProdID:
        gLogger.warn( "Could not find ancestor production for transformation %d. Flush impossible" % transID )
        requireFlush = False
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

    # We need at least all mandatory copies
    numberOfCopies = max( numberOfCopies, len( mandatorySEs ) )

    transID = self.params['TransformationID']

    # Group the remaining data by run
    res = self.__groupByRun()
    if not res['OK']:
      return res
    runFileDict = res['Value']
    if not runFileDict:
      # No files, no tasks!
      return S_OK( [] )

    # For each of the runs determine the destination of any previous files
    runSEDict = {}
    runSEsToUpdate = {}
    # Make a list of SEs already assigned to runs
    res = self.transClient.getTransformationRuns( {'TransformationID':transID, 'RunNumber':runFileDict.keys()} )
    if not res['OK']:
      gLogger.error( "Failed to obtain TransformationRuns", res['Message'] )
      return res
    for runDict in res['Value']:
      runID = runDict['RunNumber']
      selectedSite = runDict['SelectedSite']
      # If the run already has a selected site, use it for that run
      if selectedSite:
        runSEDict[runID] = selectedSite
        continue
      # Check if some files are assigned to an SE in this run
      res = self.transClient.getTransformationFiles( condDict = {'TransformationID':transID, 'RunNumber':runID, 'Status':['Assigned', 'Processed']} )
      if not res['OK']:
        gLogger.error( "Failed to get transformation files for run", "%s %s" % ( runID, res['Message'] ) )
        continue
      if res['Value']:
        assignedSE = res['Value'][0]['UsedSE']
        if assignedSE:
          runSEDict[runID] = assignedSE
          runSEsToUpdate[runID] = True

    fileTargetSEs = {}
    alreadyCompleted = []
    # Consider all runs in turn
    for runID in runFileDict.keys():
      runLfns = runFileDict[runID]
      # Check if the run is already assigned
      runUpdate = runSEsToUpdate.has_key( runID )
      stringTargetSEs = runSEDict.get( runID, '' )
      targetSEs = stringTargetSEs.split( ',' )
      # No SE assigned yet, determine them
      if not targetSEs:
        # Select candidate SEs where most of the files are already
        SEFrequency = {}
        for lfn in runLfns:
          for se in [se for se in self.data[lfn].keys() if not se.endswith( "-FAILOVER" ) and not se.endswith( "-ARCHIVE" )]:
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
        # this may happen when all files are in FAILOVER
        if not runLfnSEs:
          continue

        targetSites = []
        targetSEs = []
        # Ensure that we have a archive1 copy
        ( ses, targetSites ) = self.__selectSE( randomize( archive1ActiveSEs ), 1, targetSites )
        if len( ses ) < 1 :
          gLogger.error( 'Cannot select archive1SE in active SEs' )
          continue
        targetSEs += ses

        # If we know the second archive copy use it
        ( ses, targetSites ) = self.__selectSE( randomize( archive2ActiveSEs ), 1, targetSites )
        if len( ses ) < 1 :
          gLogger.error( 'Cannot select archive2SE in active SEs' )
          continue
        targetSEs += ses

        # Now select the secondary copies
        # Missing secondary copies, make a list of candidates, without already existing SEs
        candidateSEs = mandatorySEs
        candidateSEs += [se for se in runLfnSEs if se not in candidateSEs]
        candidateSEs += [se for se in randomize( secondaryActiveSEs ) if se not in candidateSEs]
        #print candidateSEs, numberOfCopies
        ( ses, targetSites ) = self.__selectSEs( candidateSEs, numberOfCopies, targetSites )
        if len( ses ) < numberOfCopies:
          gLogger.error( "Can not select enough Active SEs for SecondarySE" )
          continue
        targetSEs += ses

        stringTargetSEs = ','.join( sortList( targetSEs ) )
        runUpdate = True

      # Update the TransformationRuns table with the assigned SEs (don't continue if it fails)
      if runUpdate:
        res = self.transClient.setTransformationRunsSite( transID, runID, stringTargetSEs )
        if not res['OK']:
          gLogger.error( "Failed to assign TransformationRun site", res['Message'] )
          return S_ERROR( "Failed to assign TransformationRun site" )

      #Now assign the individual files to their targets
      for lfn in runLfns:
        existingSEs = self.data[lfn].keys()
        existingSites = self._getSitesForSEs( [se for se in existingSEs if not se.endswith( '-ARCHIVE' )] )
        # Discard existing SEs
        neededSEs = [se for se in targetSEs if se not in existingSEs ]
        # discard SEs at sites where already normal replica
        neededSEs = [se for se in neededSEs if se.endswith( "-ARCHIVE" ) or self.getSiteForSE( se )['Value'] not in existingSites]
        if not neededSEs:
          alreadyCompleted.append( lfn )
        else:
          fileTargetSEs[lfn] = neededSEs

    # Update the status of the already done files
    if alreadyCompleted:
      gLogger.info( "Found %s files that are already completed" % len( alreadyCompleted ) )
      self.transClient.setFileStatusForTransformation( transID, 'Processed', alreadyCompleted )

    # Now group all of the files by their target SEs
    storageElementGroups = {}
    for lfn, targetSEs in fileTargetSEs.items():
      stringTargetSEs = ','.join( sortList( targetSEs ) )
      if not storageElementGroups.has_key( stringTargetSEs ):
        storageElementGroups[stringTargetSEs] = []
      storageElementGroups[stringTargetSEs].append( lfn )

    return S_OK( self.__createTasks( storageElementGroups ) )

  def __selectSEs( self, candSEs, needToCopy, existingSites ):
    targetSites = existingSites
    targetSEs = []
    for se in candSEs:
      if needToCopy <= 0: break
      site = True
      sites = []
      # Don't take into account ARCHIVE SEs for duplicate replicas at sites
      if not se.endswith( '-ARCHIVE' ):
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
        needToCopy -= 1
    return ( targetSEs, targetSites )

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
    numberOfArchive1 = 1
    numberOfArchive2 = 1
    numberOfCopies = max( numberOfCopies, len( mandatorySEs ) )

    storageElementGroups = {}

    replicaGroups = self._getFileGroups( self.data )

    for replicaSE, lfns in replicaGroups.items():
      existingSEs = replicaSE.split( ',' )
      # Make a list of sites where the file already is in order to avoid duplicate copies
      targetSEs = []
      targetSites = []
      for se in existingSEs:
        res = getSitesForSE( se )
        if res['OK']:
          targetSites += res['Value']

      archive1ExistSEs = [se for se in existingSEs if se in archive1SEs]
      archive2ExistSEs = [se for se in existingSEs if se in archive2SEs]
      secondaryExistSEs = [se for se in existingSEs if se in mandatorySEs + secondarySEs]
      missingSEs = [ se for se in mandatorySEs if se not in existingSEs]

      if len( archive1ExistSEs ) >= numberOfArchive1 and len( archive2ExistSEs ) >= numberOfArchive2 and len( secondaryExistSEs ) >= numberOfCopies and not missingSEs :
        gLogger.info( "Found %s files that are already completed" % len( lfns ) )
        continue

      if len( archive1ExistSEs ) < numberOfArchive1:
        ( ses, targetSites ) = self.__selectSEs( randomize( archive1ActiveSEs ), numberOfArchive1, targetSites )
        if len( ses ) < numberOfArchive1 :
          gLogger.error( "Can not select Active SE for archive1SE" )
          continue
        targetSEs += ses

      if len( archive2ExistSEs ) < numberOfArchive2:
        ( ses, targetSites ) = self.__selectSEs( randomize( archive2ActiveSEs ), numberOfArchive2, targetSites )
        if len( ses ) < numberOfArchive2 :
          gLogger.error( "Can not select Active SE for archive2SE" )
          continue
        targetSEs += ses

      needToCopy = numberOfCopies
      if len( secondaryExistSEs ) < needToCopy:
        # Missing secondary copies, make a list of candidates, without already existing SEs
        candidateSEs = [se for se in missingSEs + randomize( secondaryActiveSEs ) if se not in secondaryExistSEs]
        needToCopy -= len( missingSEs ) + len( secondaryActiveSEs ) - len( candidateSEs )
        #print candidateSEs, needToCopy

        if len( candidateSEs ) >= needToCopy:
          ( ses, targetSites ) = self.__selectSEs( candidateSEs, needToCopy, existingSites = targetSites )
        if len( ses ) < needToCopy:
          gLogger.error( "Can not select enough Active SEs for SecondarySE" )
          continue
        targetSEs += ses

      if targetSEs:
        stringTargetSEs = ','.join( sortList( targetSEs ) )
        if not storageElementGroups.has_key( stringTargetSEs ):
          storageElementGroups[stringTargetSEs] = []
        storageElementGroups[stringTargetSEs].extend( lfns )
      else:
        gLogger.info( "Found %s files that are already completed" % len( lfns ) )
        self.transClient.setFileStatusForTransformation( transID, 'Processed', lfns )

    return S_OK( self.__createTasks( storageElementGroups ) )

  def __getActiveSEs( self, selist ):
    activeSE = []
    for se in selist:
      res = gConfig.getOption( '/Resources/StorageElements/%s/WriteAccess' % se, 'Unknown' )
      if res['OK'] and res['Value'] == 'Active':
        activeSE.append( se )
    return activeSE

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

  def __closerSEs( self, existingSEs, targetSEs ):
    """
    Order the targetSEs such that the first ones are closer to existingSEs. Keep all elements in targetSEs
    """
    sameSEs = [se for se in targetSEs if se in existingSEs]
    targetSEs = [ se for se in targetSEs if se not in existingSEs]
    if targetSEs:
      # Some SEs are left, look for sites
      existingSites = [self._getSiteForSE( se )['Value'] for se in existingSEs]
      closeSEs = [se for se in targetSEs if self._getSiteForSE( se )['Value'] in existingSites]
      otherSEs = [se for se in targetSEs if se not in closeSEs]
      targetSEs = randomize( closeSEs ) + randomize( otherSEs )
    return targetSEs + sameSEs

  def _ReplicateDataset( self ):
    destSEs = self.params.get( 'DestinationSEs' )
    if not destSEs:
      gLogger.info( "_ReplicateDataset plugin: no destination SEs" )
      return S_OK( [] )
    numberOfCopies = int( self.params.get( 'NumberOfReplicas', 0 ) )
    return self.__simpleReplication( destSEs, [], numberOfCopies )

  def _ArchiveDataset( self ):
    archive1SEs = self.params.get( 'Archive1SEs', [] )
    archive2SEs = self.params.get( 'Archive2SEs', ['CERN-ARCHIVE', 'CNAF-ARCHIVE', 'GRIDKA-ARCHIVE', 'IN2P3-ARCHIVE', 'SARA-ARCHIVE', 'PIC-ARCHIVE', 'RAL-ARCHIVE'] )
    archive1SEs = self.__getListFromString( archive1SEs )
    archive2SEs = self.__getListFromString( archive2SEs )
    archive1ActiveSEs = self.__getActiveSEs( archive1SEs )
    if not archive1ActiveSEs:
      archive1ActiveSEs = archive1SEs
    archive2ActiveSEs = self.__getActiveSEs( archive2SEs )
    if not archive2ActiveSEs:
      archive2ActiveSEs = archive2SEs
    if archive1ActiveSEs:
      archive1SE = [randomize( archive1ActiveSEs )[0]]
    else:
      archive1SE = []
    return self.__simpleReplication( archive1SE, archive2ActiveSEs, numberOfCopies = 2 )

  def __simpleReplication( self, mandatorySEs, secondarySEs, numberOfCopies = 0 ):
    transID = self.params['TransformationID']
    secondarySEs = self.__getListFromString( secondarySEs )
    if not numberOfCopies:
      numberOfCopies = len( secondarySEs ) + len( mandatorySEs )
      activeSecondarySEs = secondarySEs
    else:
      activeSecondarySEs = self.__getActiveSEs( secondarySEs )
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
        candidateSEs = mandatorySEs + self.__closerSEs( existingSEs, activeSecondarySEs )
        # Remove existing SEs from list of candidates
        ncand = len( candidateSEs )
        candidateSEs = [se for se in candidateSEs if se not in existingSEs]
        needToCopy = numberOfCopies - ( ncand - len( candidateSEs ) )
        targetSEs = []
        if needToCopy > 0:
          if needToCopy <= len( candidateSEs ):
            targetSEs = candidateSEs[0:needToCopy]
          else:
            targetSEs = candidateSEs
            needToCopy -= len( targetSEs )
            # Try and replicate to non active SEs
            otherSEs = [se for se in secondarySEs if se not in targetSEs]
            if otherSEs:
              targetSEs += otherSEs[0:min( needToCopy, len( otherSEs ) )]
        if targetSEs:
          stringTargetSEs = ','.join( sortList( targetSEs ) )
          if not storageElementGroups.has_key( stringTargetSEs ):
            storageElementGroups[stringTargetSEs] = []
          storageElementGroups[stringTargetSEs] += lfns
        else:
          gLogger.info( "Found %s files that are already completed" % len( lfns ) )
          self.transClient.setFileStatusForTransformation( transID, 'Processed', lfns )

    return S_OK( self.__createTasks( storageElementGroups ) )

  def __createTasks( self, storageElementGroups, chunkSize = 100 ):
    #  create reasonable size tasks
    tasks = []
    for stringTargetSEs in sortList( storageElementGroups.keys() ):
      stringTargetLFNs = storageElementGroups[stringTargetSEs]
      for lfnGroup in breakListIntoChunks( sortList( stringTargetLFNs ), 100 ):
        tasks.append( ( stringTargetSEs, lfnGroup ) )
    return tasks

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
          storageElementGroups[stringTargetSEs] += lfns
      else:
        gLogger.info( "Found %s files that don't need any replica deletion" % len( lfns ) )
        self.transClient.setFileStatusForTransformation( transID, 'Processed', lfns )

    return S_OK( self.__createTasks( storageElementGroups ) )
