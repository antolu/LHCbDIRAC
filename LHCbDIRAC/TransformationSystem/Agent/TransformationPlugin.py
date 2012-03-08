"""  TransformationPlugin is a class wrapping the supported LHCb transformation plugins
"""

__RCSID__ = "$Id$"


from DIRAC                                                             import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.Core.Utilities.SiteSEMapping                                import getSitesForSE, getSEsForSite
from DIRAC.Core.Utilities.List                                         import breakListIntoChunks, sortList, randomize
from DIRAC.DataManagementSystem.Client.ReplicaManager                  import ReplicaManager
from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient              import BookkeepingClient
from LHCbDIRAC.TransformationSystem.Client.TransformationClient        import TransformationClient
import time, types

from DIRAC.TransformationSystem.Agent.TransformationPlugin               import TransformationPlugin as DIRACTransformationPlugin

class TransformationPlugin( DIRACTransformationPlugin ):

  def __init__( self, plugin, transClient = None, replicaManager = None, debug = False ):
    if not transClient:
      self.transClient = TransformationClient()
    self.bk = BookkeepingClient()
    DIRACTransformationPlugin.__init__( self, plugin, transClient = transClient, replicaManager = replicaManager )
    self.transformationRunStats = {}
    self.debug = debug
    self.workDirectory = None
    self.pluginCallback = None
    self.cachedLFNAncestors = {}
    self.cachedLFNSize = {}
    self.cachedNbRAWFiles = {}
    self.cachedRunLfns = {}
    self.cacheFile = ''
    self.startTime = time.time()

  def __del__( self ):
    self.__logInfo( "Execution finished, timing: %.3f seconds" % ( time.time() - self.startTime ) )

  def setDirectory( self, directory ):
    self.workDirectory = directory

  def setCallback( self, callback ):
    self.pluginCallback = callback

  def setDebug( self, val = True ):
    self.debug = val

  def __logVerbose( self, message, param = '' ):
    if self.debug:
      gLogger.info( self.plugin + ": [%s] " % str( self.params['TransformationID'] ) + message, param )
    else:
      gLogger.verbose( self.plugin + ": [%s] " % str( self.params['TransformationID'] ) + message, param )

  def __logDebug( self, message, param = '' ):
    gLogger.debug( self.plugin + ": [%s] " % str( self.params['TransformationID'] ) + message, param )

  def __logInfo( self, message, param = '' ):
    gLogger.info( self.plugin + ": [%s] " % str( self.params['TransformationID'] ) + message, param )

  def __logWarn( self, message, param = '' ):
    gLogger.warn( self.plugin + ": [%s] " % str( self.params['TransformationID'] ) + message, param )

  def __logError( self, message, param = '' ):
    gLogger.error( self.plugin + ": [%s] " % str( self.params['TransformationID'] ) + message, param )

  def __removeProcessedFiles( self ):
    """
    Checks if the LFNs have descendants in the same transformation. Removes them from self.data
    and sets them 'Processed'
    """
    transID = self.params['TransformationID']
    descendants = {}
    startTime = time.time()
    for lfns in breakListIntoChunks( self.data.keys(), 500 ):
      # WARNING: this is in principle not sufficient as one should check also whether descendants without replica
      #          may have themselves descendants with replicas
      res = self.bk.getAllDescendents( lfns, production = int( transID ), depth = 1, checkreplica=True )
      if not res['OK']:
        self.__logError( "Cannot get descendants of files: %s", res['Message'] )
      else:
        descendants.update( res['Value']['Successful'] )
    self.__logVerbose( "Got Descendents of %d files in %.3f seconds" % ( len( self.data ), time.time() - startTime ) )
    if descendants:
      processedLfns = [lfn for lfn in descendants if descendants[lfn]]
      if processedLfns:
        res = self.transClient.setFileStatusForTransformation( transID, 'Processed', processedLfns )
        if res['OK']:
          self.__logInfo( "Found %d input files that have already been processed (status set)" % len( processedLfns ) )
          for lfn in processedLfns:
            if lfn in self.data:
              self.data.pop( lfn )
          for fileDict in [fileDict for fileDict in self.files]:
            if fileDict['LFN'] in processedLfns:
              self.files.remove( fileDict )
      else:
        # Here one should check descendants of children
        self.__logVerbose( "No input files have already been processed" )

  def _RAWShares( self ):
    """
    Plugin for replicating RAW data to Tier1s according to shares, excluding CERN which is the source of transfers...
    """
    self.__logInfo( "Starting execution of plugin" )
    possibleTargets = ['CNAF-RAW', 'GRIDKA-RAW', 'IN2P3-RAW', 'PIC-RAW', 'RAL-RAW', 'SARA-RAW']
    sourceSE = 'CERN-RAW'
    transID = self.params['TransformationID']

    # Get the requested shares from the CS
    res = self._getShares( 'RAW' )
    shareOnSE = True
    if not res['OK']:
      res = self._getShares( 'CPU' )
      shareOnSE = False
      if not res['OK']:
        return res
    targetShares = res['Value']
    # Remove the CERN component and renormalise
    if 'LCG.CERN.ch' in targetShares:
      targetShares.pop( 'LCG.CERN.ch' )
    targetShares = self._normaliseShares( targetShares )
    self.__logInfo( "Obtained the following target shares (%):" )
    for site in sortList( targetShares.keys() ):
      self.__logInfo( "%s: %.1f" % ( site.ljust( 15 ), targetShares[site] ) )

    # Ensure that our files only have one existing replica at CERN
    replicaGroups = self._getFileGroups( self.data )
    alreadyReplicated = {}
    for replicaSE, lfns in replicaGroups.items():
      existingSEs = replicaSE.split( ',' )
      if len( existingSEs ) > 1:
        for lfn in lfns:
          self.data.pop( lfn )
        if sourceSE in existingSEs:
          existingSEs.remove( sourceSE )
        targetSE = existingSEs[0]
        alreadyReplicated.setdefault( targetSE, [] ).extend( lfns )
    for se, lfns in alreadyReplicated.items():
      self.__logInfo( "Attempting to update %s files to Processed at %s" % ( len( lfns ), se ) )
      res = self.__groupByRunAndParam( lfns, param = 'Standard' )
      if not res['OK']:
        return res
      runFileDict = res['Value']
      for runID in sortList( runFileDict.keys() ):
        res = self.transClient.setTransformationRunsSite( transID, runID, se )
        if not res['OK']:
          self.__logError( "Failed to assign TransformationRun site", res['Message'] )
          return res
      self.transClient.setFileUsedSEForTransformation( self.params['TransformationID'], se, lfns )

    # Get the existing destinations from the transformationDB
    res = self._getExistingCounters( requestedSites = targetShares.keys(), useSE = shareOnSE )
    if not res['OK']:
      self.__logError( "Failed to get existing file share", res['Message'] )
      return res
    existingCount = res['Value']
    if existingCount:
      self.__logInfo( "Existing storage element utilization (%):" )
      normalisedExistingCount = self._normaliseShares( existingCount )
      for site in sortList( normalisedExistingCount.keys() ):
        self.__logInfo( "%s: %.1f" % ( site.ljust( 15 ), normalisedExistingCount[site] ) )

    # Group the remaining data by run
    res = self.__groupByRunAndParam( self.data, param = 'Standard' )
    if not res['OK']:
      return res
    runFileDict = res['Value']
    if not runFileDict:
      # No files, no tasks!
      return S_OK( [] )

    # For each of the runs determine the destination of any previous files
    runSEDict = {}
    res = self.transClient.getTransformationRuns( {'TransformationID':transID, 'RunNumber':runFileDict.keys()} )
    if not res['OK']:
      self.__logError( "Failed to obtain TransformationRuns", res['Message'] )
      return res
    for runDict in res['Value']:
      runSEDict[runDict['RunNumber']] = runDict['SelectedSite']

    # Choose the destination SE
    tasks = []
    for runID in [run for run in sortList( runFileDict.keys() ) if run in runSEDict]:
      runLfns = runFileDict[runID][None]
      assignedSE = None
      if runSEDict[runID]:
        assignedSE = runSEDict[runID]
        if shareOnSE:
          targetSite = assignedSE
        else:
          res = getSitesForSE( assignedSE, gridName = 'LCG' )
          if  res['OK']:
            targetSites = [site for site in res['Value'] if site in targetShares]
            if targetSites:
              targetSite = targetSites[0]
            else:
              assignedSE = None
      else:
        res = self._getNextSite( existingCount, targetShares )
        if not res['OK']:
          self.__logError( "Failed to get next destination SE", res['Message'] )
        else:
          targetSite = res['Value']
          if shareOnSE:
            assignedSE = targetSite
          else:
            res = getSEsForSite( targetSite )
            if res['OK']:
              targetSEs = [se for se in res['Value'] if se in possibleTargets]
              if targetSEs:
                assignedSE = targetSEs[0]
          if assignedSE:
            self.__logVerbose( "Run %d (%d files) assigned to %s" % ( runID, len( runLfns ), assignedSE ) )

      if assignedSE and assignedSE in possibleTargets:
      # Update the TransformationRuns table with the assigned (if this fails do not create the tasks)
        res = self.transClient.setTransformationRunsSite( transID, runID, assignedSE )
        if not res['OK']:
          self.__logError( "Failed to assign TransformationRun site", res['Message'] )
        else:
          #Create the tasks
          tasks.append( ( assignedSE, runLfns ) )
          existingCount[targetSite] = existingCount.setdefault( targetSite, 0 ) + len( runLfns )
    return S_OK( tasks )

  def _getExistingCounters( self, normalise = False, requestedSites = [], useSE = False ):
    res = self.transClient.getCounters( 'TransformationFiles', ['UsedSE'], {'TransformationID':self.params['TransformationID']} )
    if not res['OK']:
      return res
    usageDict = {}
    for usedDict, count in res['Value']:
      usedSE = usedDict['UsedSE']
      if usedSE != 'Unknown':
        usageDict[usedSE] = count
    if 'NIKHEF-RAW' in usageDict:
      usageDict['SARA-RAW'] = usageDict.setdefault( 'SARA-RAW', 0 ) + usageDict['NIKHEF-RAW']
      usageDict.pop( 'NIKHEF-RAW' )
    if requestedSites:
      siteDict = {}
      for se, count in usageDict.items():
        if not useSE:
          res = getSitesForSE( se, gridName = 'LCG' )
          if not res['OK']:
            return res
          sites = [site for site in res['Value'] if site in requestedSites]
        elif se in requestedSites:
          sites = [se]
        else:
          self.__logWarn( "%s is in counters but not in required list" % se )
          sites = []
        # Don't double count sites if they share the same SE
        for site in sites:
          siteDict[site] = siteDict.setdefault( site, 0 ) + count / float( len( sites ) )
      usageDict = siteDict.copy()
    if normalise:
      usageDict = self._normaliseShares( usageDict )
    return S_OK( usageDict )

  def _AtomicRun( self ):
    self.__logInfo( "Starting execution of plugin" )
    transID = self.params['TransformationID']
    self.__removeProcessedFiles()
    # Get the requested shares from the CS
    backupSE = 'CERN-RAW'
    res = self.transClient.getTransformation( transID )
    if not res['OK']:
      self.__logError( "Cannot get information on transformation" )
      return {}
    else:
      transType = res['Value']['Type']
    sharesSections = { 'DataReconstruction': 'CPUforRAW', 'DataReprocessing' : 'CPUforReprocessing'}
    if transType in sharesSections:
      section = sharesSections[transType]
    else:
      section = 'CPUforRAW'
    res = self._getShares( section )
    if not res['OK']:
      cpuForRaw = False
      res = self._getShares( 'CPU', normalise = True )
      if not res['OK']:
        return res
      cpuShares = res['Value']
      targetSites = sortList( cpuShares.keys() )
    else:
      cpuForRaw = True
      rawFraction = res['Value']
      targetSites = sortList( rawFraction.keys() )
      result = self._getShares( 'RAW', normalise = True )
      if result['OK']:
        rawShares = result['Value']
        tier1Fraction = 0.
        cpuShares = {}
        for se in [se for se in rawShares if se in rawFraction]:
          cpuShares[se] = rawShares[se] * rawFraction[se]
          tier1Fraction += cpuShares[se]
        cpuShares[backupSE] = 100. - tier1Fraction
      else:
        rawShares = None
    if cpuForRaw:
      self.__logInfo( "Fraction of RAW (%s) to be processed at each SE (%%):" % section )
      for site in targetSites:
        self.__logInfo( "%s: %.1f" % ( site.ljust( 15 ), 100. * rawFraction[site] ) )

    # Get the existing destinations from the transformationDB
    res = self._getExistingCounters( requestedSites = targetSites + [backupSE], useSE = cpuForRaw )
    if not res['OK']:
      self.__logError( "Failed to get executed share", res['Message'] )
      return res
    existingCount = res['Value']
    if existingCount:
      normalisedExistingCount = self._normaliseShares( existingCount )
    else:
      normalisedExistingCount = {}
    self.__logInfo( "Target shares and utilisation for production (%%):" )
    for se in sortList( cpuShares.keys() ):
      infoStr = "%s: %4.1f |" % ( se.ljust( 15 ), cpuShares[se] )
      if se in normalisedExistingCount:
        infoStr += " %4.1f" % normalisedExistingCount[se]
      self.__logInfo( infoStr )

    # Group the remaining data by run
    res = self.__groupByRun()
    if not res['OK']:
      return res
    runFileDict = res['Value']
    if not runFileDict:
      return S_OK()

    # For each of the runs determine the destination of any previous files
    runUpdate = {}
    runSEDict = {}
    res = self.transClient.getTransformationRuns( {'TransformationID':transID, 'RunNumber':runFileDict.keys()} )
    if not res['OK']:
      self.__logError( "Failed to obtain TransformationRuns", res['Message'] )
      return res
    for runDict in res['Value']:
      runID = runDict['RunNumber']
      if runDict['SelectedSite']:
        runSEDict[runID] = runDict['SelectedSite']
        runUpdate[runID] = False
      else:
        res = self.transClient.getTransformationFiles( condDict = {'TransformationID':transID, 'RunNumber':runID, 'Status':['Assigned', 'Processed']} )
        if not res['OK']:
          self.__logError( "Failed to get transformation files for run", "%s %s" % ( runID, res['Message'] ) )
        else:
          if res['Value']:
            lfnSEs = {}
            for dict in res['Value']:
              lfnSEs[dict['LFN']] = [dict['UsedSE']]
            sortedSEs = self.__sortExistingSEs( lfnSEs.keys(), lfnSEs )
            res = getSitesForSE( sortedSEs[0], gridName = 'LCG' )
            if  res['OK']:
              for site in [site for site in res['Value'] if site in targetSites]:
                  runSEDict[runID] = site
                  runUpdate[runID] = True

    # Choose the destination site for new runs
    for runID in [run for run in sortList( runFileDict.keys() ) if run not in runSEDict]:
      runLfns = runFileDict[runID]
      distinctSEs = []
      candidates = []
      for lfn in runLfns:
        for se in [se for se in self.data[lfn].keys() if se not in distinctSEs]:
          if se == "CERN-RDST":
            se = "CERN-RAW"
          if se in distinctSEs: continue
          if cpuForRaw:
            distinctSEs.append( se )
          else:
            res = getSitesForSE( se, gridName = 'LCG' )
            if res['OK']:
              distinctSEs.append( se )
              for site in [site for site in res['Value'] if site in targetSites and site not in candidates]:
                candidates.append( site )
      if len( distinctSEs ) < 2:
        self.__logInfo( "Not found two candidate SEs for run %d, skipped" % runID )
      else:
        if cpuForRaw:
          seProbs = {}
          prob = 0.
          if backupSE not in distinctSEs:
            self.__logWarn( " %s not in the SEs for run %d" % ( backupSE, runID ) )
            backupSE = None
          distinctSEs = sortList( [se for se in distinctSEs if se in rawFraction and se != backupSE] )
          for se in distinctSEs:
            prob += rawFraction[se] / len( distinctSEs )
            seProbs[se] = prob
          if backupSE:
            seProbs[backupSE] = 1.
            distinctSEs.append( backupSE )
          # get a random number between 0 and 1
          import random
          rand = random.uniform( 0., 1. )
          strProbs = ','.join( [' %s:%.3f' % ( se, seProbs[se] ) for se in distinctSEs] )
          self.__logInfo( "For run %d, SE integrated fraction =%s, random number = %.3f" % ( runID, strProbs, rand ) )
          for se in distinctSEs:
            if rand <= seProbs[se]:
              selectedSE = se
              break
          self.__logVerbose( "Selected SE for reconstruction is %s", selectedSE )
          targetSite = selectedSE
        else:
          res = self._getNextSite( existingCount, cpuShares, randomize( candidates ) )
          if not res['OK']:
            self.__logError( "Failed to get next destination SE", res['Message'] )
            targetSite = None
          else:
            targetSite = res['Value']
        if targetSite:
          existingCount[targetSite] = existingCount.setdefault( targetSite, 0 ) + len( runLfns )
          runSEDict[runID] = targetSite
          runUpdate[runID] = True
          self.__logInfo( "Run %d (%d files) assigned to %s" % ( runID, len( runLfns ), targetSite ) )

    # Create the tasks
    tasks = []
    for runID in sortList( runSEDict.keys() ):
      targetSite = runSEDict[runID]
      if runUpdate[runID]:
        # Update the TransformationRuns table with the assigned (if this fails do not create the tasks)
        res = self.transClient.setTransformationRunsSite( transID, runID, targetSite )
        if not res['OK']:
          self.__logError( "Failed to assign TransformationRun site", res['Message'] )
          continue
      if cpuForRaw and not targetSite.startswith( 'LCG.' ):
        possibleSEs = [targetSite]
      else:
        res = getSEsForSite( targetSite )
        if  res['OK']:
          possibleSEs = res['Value']
        else:
          possibleSEs = None
      if possibleSEs:
        for lfn in sortList( runFileDict[runID] ):
          if len( self.data[lfn] ) >= 2:
            for se in self.data[lfn].keys():
              if se == "CERN-RDST":
                se = "CERN-RAW"
              if se in possibleSEs:
                tasks.append( ( se, [lfn] ) )
                break

    if self.pluginCallback:
      self.pluginCallback( transID, invalidateCache = True )
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
      if lfn:
        runDict.setdefault( runNumber, [] ).append( lfn )
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
      runDict.setdefault( runNumber, {} ).setdefault( metadata.get( param ), [] ).append( lfn )
    return S_OK( runDict )

  def __getRAWAncestorsForRun( self, transID, runID, param = '', paramValue = '' ):
    startTime1 = time.time()
    res = self.transClient.getTransformationFiles( { 'TransformationID' : transID, 'RunNumber': runID } )
    self.__logVerbose( "Timing for getting transformation files: %.3f s" % ( time.time() - startTime1 ) )
    if not res['OK']:
      self.__logError( "Cannot get files for run %s" % str( runID ) )
      return []
    ancestors = 0
    lfns = [f['LFN'] for f in res['Value']]
    cachedLFNs = self.cachedLFNAncestors.get( runID )
    if cachedLFNs:
      self.__logVerbose( "Cache hit for run %d: %d files cached" % ( runID, len( cachedLFNs ) ) )
      for lfn in cachedLFNs:
        ancestors += cachedLFNs[lfn]
      lfns = [lfn for lfn in lfns if lfn not in cachedLFNs]
    if param:
      res = self.__getBookkeepingMetadata( lfns )
      if not res['OK']:
        return []
      metadata = res['Value']
      lfns = [f for f in metadata if metadata[f][param] == paramValue]
    if lfns:
      startTime = time.time()
      res = self.bk.getAllAncestorsWithFileMetaData( lfns, depth = 10 )
      self.__logVerbose( "Timing for getting all ancestors with metadata of %d files: %.3f s" % ( len( lfns ), time.time() - startTime ) )
      ancestorDict = res['Value']['Successful']
      for lfn in ancestorDict:
        n = len( [f for f in ancestorDict[lfn] if f['FileType'] == 'RAW'] )
        self.cachedLFNAncestors.setdefault( runID, {} )[lfn] = n
        ancestors += n
    self.__logVerbose( "Full timing for getRAWAncestors: %.3f seconds" % ( time.time() - startTime1 ) )
    return ancestors

  def __readCacheFile( self, transID ):
    import os, pickle
    # Now try and get the cached information
    tmpDir = os.environ.get( 'TMPDIR', '/tmp' )
    cacheFiles = ( ( self.workDirectory, ( 'TransPluginCache' ) ),
                   ( tmpDir, ( 'dirac', 'TransPluginCache' ) ) )
    for ( cacheFile, prefixes ) in cacheFiles:
      if not cacheFile: continue
      if type( prefixes ) == type( '' ):
        prefixes = [prefixes]
      for node in prefixes:
        cacheFile = os.path.join( cacheFile, node )
        if not os.path.exists( cacheFile ):
          os.mkdir( cacheFile )
      cacheFile = os.path.join( cacheFile, "Transformation_%s.pkl" % ( str( transID ) ) )
      if not self.cacheFile:
        self.cacheFile = cacheFile
      try:
        f = open( cacheFile, 'r' )
        self.cachedLFNAncestors = pickle.load( f )
        self.cachedNbRAWFiles = pickle.load( f )
        self.cachedLFNSize = pickle.load( f )
        try:
          self.cachedRunLfns = pickle.load( f )
        except:
          pass
        f.close()
        self.__logVerbose( "Cache file %s successfully loaded" % cacheFile )
        break
      except:
        self.__logVerbose( "Cache file %s could not be loaded" % cacheFile )

  def __writeCacheFile( self ):
    import pickle
    if self.cacheFile:
      try:
        f = open( self.cacheFile, 'w' )
        pickle.dump( self.cachedLFNAncestors, f )
        pickle.dump( self.cachedNbRAWFiles, f )
        pickle.dump( self.cachedLFNSize, f )
        pickle.dump( self.cachedRunLfns, f )
        f.close()
        self.__logVerbose( "Cache file %s successfully written" % self.cacheFile )
      except:
        self.__logError( "Could not write cache file %s" % self.cacheFile )

  def __getFileSize( self, lfns ):
    fileSizes = {}
    startTime1 = time.time()
    for lfn in [lfn for lfn in lfns if lfn in self.cachedLFNSize]:
        fileSizes[lfn] = self.cachedLFNSize[lfn]
    if fileSizes:
      self.__logVerbose( "Cache hit for File size for %d files" % len( fileSizes ) )
    lfns = [lfn for lfn in lfns if lfn not in self.cachedLFNSize]
    if lfns:
      startTime = time.time()
      res = self.rm.getCatalogFileSize( lfns )
      if not res['OK']:
        return S_ERROR( "Failed to get sizes for files" )
      if res['Value']['Failed']:
        return S_ERROR( "Failed to get sizes for all files" )
      fileSizes.update( res['Value']['Successful'] )
      self.cachedLFNSize.update( ( res['Value']['Successful'] ) )
      self.__logVerbose( "Timing for getting size of %d files from catalog: %.3f seconds" % ( len( lfns ), ( time.time() - startTime ) ) )
    self.__logVerbose( "Timing for getting size of files: %.3f seconds" % ( time.time() - startTime1 ) )
    return fileSizes

  def __clearCachedFileSize( self, lfns ):
    for lfn in [lfn for lfn in lfns if lfn in self.cachedLFNSize]:
      self.cachedLFNSize.pop( lfn )

  def _groupBySize( self ):
    """ Generate a task for a given amount of data """
    if not self.params:
      return S_ERROR( "TransformationPlugin._BySize: The 'BySize' plug-in requires parameters." )
    status = self.params['Status']
    requestedSize = float( self.params['GroupSize'] ) * 1000 * 1000 * 1000 # input size in GB converted to bytes
    maxFiles = self.__getPluginParam( 'MaxFiles', 100 )
    # Group files by SE
    fileGroups = self._getFileGroups( self.data )
    # Get the file sizes
    fileSizes = self.__getFileSize( self.data.keys() )
    tasks = []
    for replicaSE, lfns in fileGroups.items():
      taskLfns = []
      taskSize = 0
      for lfn in lfns:
        taskSize += fileSizes[lfn]
        taskLfns.append( lfn )
        if ( taskSize > requestedSize ) or ( len( taskLfns ) >= maxFiles ):
          tasks.append( ( replicaSE, taskLfns ) )
          self.__clearCachedFileSize( taskLfns )
          taskLfns = []
          taskSize = 0
      if ( status == 'Flush' ) and taskLfns:
        tasks.append( ( replicaSE, taskLfns ) )
        self.__clearCachedFileSize( taskLfns )
    return S_OK( tasks )

  def _LHCbStandard( self ):
    return self._groupByReplicas()

  def __getNbRAWInRun( self, runID, evtType ):
    rawFiles = self.cachedNbRAWFiles.get( runID, {} ).get( evtType )
    if not rawFiles:
      startTime = time.time()
      res = self.bk.getRunNbFiles( {'RunNumber':runID, 'EventTypeId':evtType} )
      if not res['OK']:
        rawFiles = 0
        self.__logError( "Cannot get number of RAW files for run %d, evttype %d" % ( runID, evtType ) )
      else:
        rawFiles = res['Value']
        self.cachedNbRAWFiles.setdefault( runID, {} )[evtType] = rawFiles
        self.__logVerbose( "Run %d has %d RAW files (timing: %3f s)" % ( runID, rawFiles, time.time() - startTime ) )
    return rawFiles

  def _ByRun( self, param = '', plugin = 'LHCbStandard', requireFlush = False ):
    self.__logInfo( "Starting execution of plugin" )
    transID = self.params['TransformationID']
    self.__removeProcessedFiles()
    res = self.__groupByRunAndParam( self.data, param = param )
    if not res['OK']:
      return res
    runDict = res['Value']
    transStatus = self.params['Status']
    allTasks = []
    res = self.transClient.getTransformationRuns( {'TransformationID':transID, 'RunNumber':runDict.keys()} )
    if not res['OK']:
      return res
    # Loop on all runs that have new files
    inputData = self.data.copy()
    self.__readCacheFile( transID )
    runEvtType = {}
    for run in res['Value']:
      runID = run['RunNumber']
      runStatus = run['Status']
      if transStatus == 'Flush':
        runStatus = 'Flush'
      paramDict = runDict.get( runID, {} )
      runRAWFiles = {}
      for paramValue in sortList( paramDict.keys() ):
        if paramValue:
          paramStr = " (%s : %s) " % ( param, paramValue )
        else:
          paramStr = " "
        runParamLfns = paramDict[paramValue]
        # Check if something was new since last time...
        cachedLfns = self.cachedRunLfns.setdefault( runID, {} ).setdefault( paramValue, [] )
        newLfns = [lfn for lfn in runParamLfns if lfn not in cachedLfns]
        if len( newLfns ) == 0 and transID > 0 and runStatus != 'Flush':
          self.__logVerbose( "No new files since last time for run %d%s: skip..." % ( runID, paramStr ) )
          continue
        else:
          self.__logVerbose( "Of %d files, %d are new for %d%s" % ( len( runParamLfns ), len( newLfns ), runID, paramStr ) )
        runFlush = requireFlush
        if runFlush:
          if paramValue not in runEvtType:
            lfn = runParamLfns[0]
            res = self.__getBookkeepingMetadata( [lfn] )
            if res['OK']:
              runEvtType[paramValue] = res['Value'][lfn].get( 'EventTypeId', 90000000 )
              self.__logVerbose( 'Event type %s: %s' % ( paramStr, str( runEvtType[paramValue] ) ) )
            else:
              self.__logWarn( "Can't determine event type for transformation %s, can't flush" % paramStr )
              runFlush = False
              runEvtType[paramValue] = None
          evtType = runEvtType[paramValue]
        runParamReplicas = {}
        for lfn in [lfn for lfn in runParamLfns if lfn in inputData]:
            runParamReplicas[lfn ] = {}
            for se in inputData[lfn]:
              if not self.__isArchive( se ):
                runParamReplicas[lfn][se] = inputData[lfn][se]
        self.data = runParamReplicas
        status = runStatus
        if status != 'Flush' and runFlush:
          # If all files in that run have been processed and received, flush
          # Get the number of RAW files in that run
          rawFiles = self.__getNbRAWInRun( runID, evtType )
          ancestorRawFiles = self.__getRAWAncestorsForRun( transID, runID, param, paramValue )
          self.__logVerbose( "Obtained %d ancestor RAW files" % ancestorRawFiles )
          runProcessed = ( ancestorRawFiles == rawFiles )
          if runProcessed:
            # The whole run was processed by the parent production and we received all files
            self.__logInfo( "All RAW files (%d) ready for run %d%s- Flushing %d files" % ( rawFiles, runID, paramStr, len( runParamReplicas ) ) )
            status = 'Flush'
            self.transClient.setTransformationRunStatus( transID, runID, 'Flush' )
          else:
            self.__logVerbose( "Only %d files (of %d) available for run %d" % ( ancestorRawFiles, rawFiles, runID ) )
        self.params['Status'] = status
        #print "Calling",plugin,"with",self.data
        res = eval( 'self._%s()' % plugin )
        self.params['Status'] = transStatus
        if not res['OK']:
          return res
        allTasks.extend( res['Value'] )
        # Cache the left-over LFNs
        taskLfns = []
        for task in res['Value']:
          targetSite = task[0]
          taskLfns += task[1]
          res = self.transClient.setTransformationRunsSite( transID, runID, targetSite )
          if not res['OK']:
            self.__logError( "Failed to set target SE for run %s as %s: %s" % ( str( runID ), targetSite, res['Message'] ) )
        self.cachedRunLfns[runID][paramValue] = [lfn for lfn in runParamLfns if lfn not in taskLfns]
    self.data = inputData
    self.__writeCacheFile()
    return S_OK( allTasks )

  def _ByRunWithFlush( self ):
    return self._ByRun( requireFlush = True )

  def _ByRunBySize( self ):
    return self._ByRun( plugin = 'BySize' )

  def _ByRunBySizeWithFlush( self ):
    return self._ByRun( plugin = 'BySize', requireFlush = True )

  def _ByRunSize( self ):
    return self._ByRun( plugin = 'BySize' )

  def _MergeByRun( self ):
    return self._ByRunSize()

  def _ByRunSizeWithFlush( self ):
    return self._ByRun( plugin = 'BySize', requireFlush = True )

  def _MergeByRunWithFlush( self ):
    return self._ByRunSizeWithFlush()

  def _ByRunFileType( self ):
    return self._ByRun( param = 'FileType' )

  def _ByRunFileTypeWithFlush( self ):
    return self._ByRun( param = 'FileType', requireFlush = True )

  def _ByRunFileTypeSize( self ):
    return self._ByRun( param = 'FileType', plugin = 'BySize' )

  def _ByRunFileTypeSizeWithFlush( self ):
    return self._ByRun( param = 'FileType', plugin = 'BySize', requireFlush = True )

  def _ByRunEventType( self ):
    return self._ByRun( param = 'EventTypeId' )

  def _ByRunEventTypeWithFlush( self ):
    return self._ByRun( param = 'EventTypeId', requireFlush = True )

  def _ByRunEventTypeSize( self ):
    return self._ByRun( param = 'EventTypeId', plugin = 'BySize' )

  def _ByRunEventTypeSizeWithFlush( self ):
    return self._ByRun( param = 'EventTypeId', plugin = 'BySize', requireFlush = True )

  def __getBookkeepingMetadata( self, lfns ):
    start = time.time()
    res = self.bk.getFileMetadata( lfns )
    self.__logVerbose( "Obtained BK metadata of %d files in %.3f seconds" % ( len( lfns ), time.time() - start ) )
    return res

  def __isArchive( self, se ):
    return se.endswith( "-ARCHIVE" )

  def __isFailover( self, se ):
    return se.endswith( "-FAILOVER" )

  def __isFreezer( self, se ):
    return se.endswith( "-FREEZER" )

  def __sortExistingSEs( self, lfns, lfnSEs ):
    SEFrequency = {}
    archiveSEs = []
    for lfn in lfns:
      if lfn in lfnSEs:
        existingSEs = lfnSEs[lfn]
        archiveSEs += [se for se in existingSEs if self.__isArchive( se ) and se not in archiveSEs]
        for se in [se for se in existingSEs if not self.__isFailover( se ) and not self.__isArchive( se )]:
          SEFrequency[se] = SEFrequency.setdefault( se, 0 ) + 1
    sortedSEs = SEFrequency.keys()
    # sort SEs in reverse order of frequency
    sortedSEs.sort( key = SEFrequency.get, reverse = True )
    # add the archive SEs at the end
    return sortedSEs + archiveSEs

  def __selectSEs( self, candSEs, needToCopy, existingSites ):
    targetSites = existingSites
    targetSEs = []
    if needToCopy < len( candSEs ):
      # Use a weight for each SE
      pass
    for se in candSEs:
      if needToCopy <= 0: break
      site = True
      sites = []
      # Don't take into account ARCHIVE SEs for duplicate replicas at sites
      if not self.__isArchive( se ):
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

  def __setTargetSEs( self, numberOfCopies, archive1SEs, archive2SEs, mandatorySEs, secondarySEs, existingSEs, exclusiveSEs = False ):
    # Select active SEs
    nbArchive1 = min(1, len(archive1SEs))
    nbArchive2 = min(1, len(archive2SEs))
    archive1ActiveSEs = self.__getActiveSEs( archive1SEs )
    if not archive1ActiveSEs:
      archive1ActiveSEs = archive1SEs
    archive2ActiveSEs = self.__getActiveSEs( archive2SEs )
    if not archive2ActiveSEs:
      archive2ActiveSEs = archive2SEs
    secondaryActiveSEs = self.__getActiveSEs( secondarySEs )

    targetSites = []
    targetSEs = []
    self.__logVerbose( "Selecting SEs from %s, %s, %s, %s (%d copies) for files in %s" % ( archive1ActiveSEs, archive2ActiveSEs, mandatorySEs, secondaryActiveSEs, numberOfCopies, existingSEs ) )
    # Ensure that we have a archive1 copy
    archive1Existing = [se for se in archive1SEs if se in existingSEs]
    ( ses, targetSites ) = self.__selectSEs( archive1Existing + randomize( archive1ActiveSEs ), nbArchive1, targetSites )
    self.__logVerbose( "Archive1SEs: %s" % ses )
    if len( ses ) < nbArchive1 :
      self.__logError( 'Cannot select archive1SE in active SEs' )
      return None
    targetSEs += ses

    # ... and an Archive2 copy
    archive2Existing = [se for se in archive2SEs if se in existingSEs]
    ( ses, targetSites ) = self.__selectSEs( archive2Existing + randomize( archive2ActiveSEs ), nbArchive2, targetSites )
    self.__logVerbose( "Archive2SEs: %s" % ses )
    if len( ses ) < nbArchive2 :
      self.__logError( 'Cannot select archive2SE in active SEs' )
      return None
    targetSEs += ses

    # Now select the secondary copies
    # Missing secondary copies, make a list of candidates, without already existing SEs
    #candidateSEs = [se for se in mandatorySEs]
    #candidateSEs += [se for se in existingSEs if se not in candidateSEs]
    candidateSEs = [se for se in existingSEs if se not in targetSEs+archive1SEs+archive2SEs]
    candidateSEs += [se for se in mandatorySEs if se not in candidateSEs]
    candidateSEs += [se for se in randomize( secondaryActiveSEs ) if se not in candidateSEs]
    ( ses, targetSites ) = self.__selectSEs( candidateSEs, numberOfCopies, targetSites )
    self.__logVerbose( "SecondarySEs: %s" % ses )
    if len( ses ) < numberOfCopies:
      self.__logError( "Can not select enough Active SEs for SecondarySE" )
      return None
    targetSEs += ses

    if exclusiveSEs:
      targetSEs = [se for se in targetSEs if se not in existingSEs]
    self.__logVerbose( "Selected target SEs: %s" % targetSEs )
    return ','.join( sortList( targetSEs ) )

  def __assignTargetToLfns( self, lfns, stringTargetSEs ):
    targetSEs = [se for se in stringTargetSEs.split( ',' ) if se]
    alreadyCompleted = []
    fileTargetSEs = {}
    for lfn in lfns:
      existingSEs = self.data[lfn].keys()
      existingSites = self._getSitesForSEs( [se for se in existingSEs if not se.endswith( '-ARCHIVE' )] )
      # Discard existing SEs
      ses = [se for se in targetSEs if se not in existingSEs ]
      # discard SEs at sites where already normal replica
      neededSEs = [se for se in ses if self.__isArchive( se ) or self.__isFreezer( se ) or self._getSiteForSE( se )['Value'] not in existingSites]
      stringTargetSEs = ','.join( sortList( neededSEs ) )
      if not neededSEs:
        alreadyCompleted.append( lfn )
      else:
        fileTargetSEs[lfn] = stringTargetSEs
    return ( fileTargetSEs, alreadyCompleted )

  def __getPluginParam(self, name, default=None):
    # get the value of a parameter looking 1st in the CS
    if default != None:
      valueType = type(default)
    else:
      valueType = None
    # First look at a generic value...
    from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
    optionPath = "TransformationPlugins/%s" %( name)
    value = Operations().getValue( optionPath, None)
    self.__logVerbose("Default plugin param %s: '%s'" %(optionPath, value))
    # Then look at a plugin-specific value
    optionPath = "TransformationPlugins/%s/%s" %(self.plugin, name)
    value = Operations().getValue( optionPath, value)
    self.__logVerbose("Speficic plugin param %s: '%s'" %(optionPath, value))
    if value != None:
      default = value
    # Finally look at a transformation-specific parameter
    value = self.params.get( name, default)
    self.__logVerbose("Transformation plugin param %s: '%s'" %(name, value))
    if valueType and type(value) != valueType:
      if valueType == type([]):
        value = self.__getListFromString(value)
      elif valueType == type(0):
        value = int(value)
      elif valueType != type(''):
        self.__logWarning("Unknown parameter value type %s, passed as string" %str(valueType))
    self.__logVerbose("Final plugin param %s: '%s'" %(name, value))
    return value

  def _LHCbDSTBroadcast( self ):
    archive1SEs = self.__getPluginParam( 'Archive1SEs', ['CERN-ARCHIVE'] )
    archive2SEs = self.__getPluginParam( 'Archive2SEs', ['CNAF-ARCHIVE', 'GRIDKA-ARCHIVE', 'IN2P3-ARCHIVE', 'SARA-ARCHIVE', 'PIC-ARCHIVE', 'RAL-ARCHIVE'] )
    mandatorySEs = self.__getPluginParam( 'MandatorySEs', ['CERN-DST'] )
    secondarySEs = self.__getPluginParam( 'SecondarySEs', ['CNAF-DST', 'GRIDKA-DST', 'IN2P3-DST', 'SARA-DST', 'PIC-DST', 'RAL-DST'] )
    numberOfCopies = self.__getPluginParam( 'NumberOfReplicas', 4 )
    return self._lhcbBroadcast( archive1SEs, archive2SEs, mandatorySEs, secondarySEs, numberOfCopies )

  def _LHCbMCDSTBroadcast( self ):
    archive1SEs = self.__getPluginParam( 'Archive1SEs', ['CERN-ARCHIVE'] )
    archive2SEs = self.__getPluginParam( 'Archive2SEs', ['CNAF-ARCHIVE', 'GRIDKA-ARCHIVE', 'IN2P3-ARCHIVE', 'SARA-ARCHIVE', 'PIC-ARCHIVE', 'RAL-ARCHIVE'] )
    mandatorySEs = self.__getPluginParam( 'MandatorySEs', ['CERN_MC_M-DST'] )
    secondarySEs = self.__getPluginParam( 'SecondarySEs', ['CNAF_MC-DST', 'GRIDKA_MC-DST', 'IN2P3_MC-DST', 'SARA_MC-DST', 'PIC_MC-DST', 'RAL_MC-DST'] )
    numberOfCopies = self.__getPluginParam( 'NumberOfReplicas', 3 )
    return self._lhcbBroadcast( archive1SEs, archive2SEs, mandatorySEs, secondarySEs, numberOfCopies )

  def _lhcbBroadcast( self, archive1SEs, archive2SEs, mandatorySEs, secondarySEs, numberOfCopies ):
    """ This plug-in broadcasts files to one archive1SE, one archive2SE and numberOfCopies secondarySEs"""
    self.__logInfo( "Starting execution of plugin" )
    transID = self.params['TransformationID']

    # We need at least all mandatory copies
    numberOfCopies = max( numberOfCopies, len( mandatorySEs ) )

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
    runUpdate = {}
    # Make a list of SEs already assigned to runs
    res = self.transClient.getTransformationRuns( {'TransformationID':transID, 'RunNumber':runFileDict.keys()} )
    if not res['OK']:
      self.__logError( "Failed to obtain TransformationRuns", res['Message'] )
      return res
    for runDict in res['Value']:
      runID = runDict['RunNumber']
      # If the run already has a selected site, use it for that run
      if runDict['SelectedSite']:
        runSEDict[runID] = runDict['SelectedSite']
        runUpdate[runID] = False
      else:
        # Check if some files are assigned to an SE in this run
        res = self.transClient.getTransformationFiles( condDict = {'TransformationID':transID, 'RunNumber':runID, 'Status':['Assigned', 'Processed']} )
        if not res['OK']:
          self.__logError( "Failed to get transformation files for run", "%s %s" % ( runID, res['Message'] ) )
        else:
          if res['Value'] and res['Value'][0]['UsedSE']:
              runSEDict[runID] = res['Value'][0]['UsedSE']
              runUpdate[runID] = True

    fileTargetSEs = {}
    alreadyCompleted = []
    # Consider all runs in turn
    for runID in runFileDict.keys():
      runLfns = runFileDict[runID]
      # Check if the run is already assigned
      stringTargetSEs = runSEDict.get( runID, None )
      # No SE assigned yet, determine them
      if not stringTargetSEs:
        # Sort existing SEs where most of the files are already
        existingSEs = self.__sortExistingSEs( runLfns, self.data )
        # this may happen when all files are in FAILOVER
        if  existingSEs:
          # Now select the target SEs
          self.__logVerbose( "Selecting SEs for %d files: %s" % ( len( runLfns ), str( runLfns ) ) )
          stringTargetSEs = self.__setTargetSEs( numberOfCopies, archive1SEs, archive2SEs, mandatorySEs, secondarySEs, existingSEs, exclusiveSEs = False )
          runUpdate[runID] = True

      # Update the TransformationRuns table with the assigned SEs (don't continue if it fails)
      if stringTargetSEs:
        if runUpdate[runID]:
          res = self.transClient.setTransformationRunsSite( transID, runID, stringTargetSEs )
          if not res['OK']:
            self.__logError( "Failed to assign TransformationRun site", res['Message'] )
            return S_ERROR( "Failed to assign TransformationRun site" )

        #Now assign the individual files to their targets
        ( runFileTargetSEs, runCompleted ) = self.__assignTargetToLfns( runLfns, stringTargetSEs )
        alreadyCompleted += runCompleted
        fileTargetSEs.update( runFileTargetSEs )

    # Update the status of the already done files
    if alreadyCompleted:
      self.__logInfo( "Found %s files that are already completed" % len( alreadyCompleted ) )
      self.transClient.setFileStatusForTransformation( transID, 'Processed', alreadyCompleted )

    # Now group all of the files by their target SEs
    storageElementGroups = {}
    for lfn, stringTargetSEs in fileTargetSEs.items():
      storageElementGroups.setdefault( stringTargetSEs, [] ).append( lfn )

    return S_OK( self.__createTasks( storageElementGroups ) )

  def _LHCbMCDSTBroadcastRandom( self ):
    """ This plug-in broadcasts files to archive1, to archive2 and to (NumberOfReplicas) secondary SEs  """

    self.__logInfo( "Starting execution of plugin" )
    transID = self.params['TransformationID']
    archive1SEs = self.__getPluginParam( 'Archive1SEs', ['CERN-ARCHIVE'] )
    archive2SEs = self.__getPluginParam( 'Archive2SEs', ['CNAF-ARCHIVE', 'GRIDKA-ARCHIVE', 'IN2P3-ARCHIVE', 'SARA-ARCHIVE', 'PIC-ARCHIVE', 'RAL-ARCHIVE'] )
    mandatorySEs = self.__getPluginParam( 'MandatorySEs', ['CERN_MC_M-DST'] )
    secondarySEs = self.__getPluginParam( 'SecondarySEs', ['CNAF_MC-DST', 'GRIDKA_MC-DST', 'IN2P3_MC-DST', 'SARA_MC-DST', 'PIC_MC-DST', 'RAL_MC-DST'] )
    numberOfCopies = self.__getPluginParam( 'NumberOfReplicas', 3 )

    # We need at least all mandatory copies
    numberOfCopies = max( numberOfCopies, len( mandatorySEs ) )

    storageElementGroups = {}
    for replicaSE, lfnGroup in self._getFileGroups( self.data ).items():
      existingSEs = replicaSE.split( ',' )
      for lfns in breakListIntoChunks( lfnGroup, 100 ):

        stringTargetSEs = self.__setTargetSEs( numberOfCopies, archive1SEs, archive2SEs, mandatorySEs, secondarySEs, existingSEs, exclusiveSEs = True )
        if stringTargetSEs:
          storageElementGroups.setdefault( stringTargetSEs, [] ).extend( lfns )
        else:
          self.__logInfo( "Found %d files that are already completed" % len( lfns ) )
          self.transClient.setFileStatusForTransformation( transID, 'Processed', lfns )

    return S_OK( self.__createTasks( storageElementGroups ) )

  def __getActiveSEs( self, selist ):
    activeSE = []
    try:
      from DIRAC.ResourceStatusSystem.Client                                 import ResourceStatus
      res = ResourceStatus.getStorageElementStatus( selist, statusType = 'Write', default = 'Unknown' )
      if res[ 'OK' ]:
        for k,v in res[ 'Value' ].items():
          if v.has_key( 'Write' ) and v[ 'Write' ] in [ 'Active', 'Bad' ]:
            activeSE.append( k )
    except:
      # To be removed when using Dirac v6r2
      for se in selist:
        res = gConfig.getOption( '/Resources/StorageElements/%s/WriteAccess' % se, 'Unknown' )
        if res['OK'] and res['Value'] == 'Active':
          activeSE.append( se )
    return activeSE

  def __getListFromString( self, s ):
    # Avoid using eval()... painful
    if type( s ) == types.StringType:
      if s == "[]" or s == '':
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
    destSEs = self.__getPluginParam( 'DestinationSEs', [] )
    if not destSEs:
      destSEs = self.__getPluginParam( 'MandatorySEs', [] )
    secondarySEs = self.__getPluginParam( 'SecondarySEs', [] )
    numberOfCopies = self.__getPluginParam( 'NumberOfReplicas', 0 )
    return self.__simpleReplication( destSEs, secondarySEs, numberOfCopies )

  def _ArchiveDataset( self ):
    archive1SEs = self.__getPluginParam( 'Archive1SEs', ['CERN-ARCHIVE'] )
    archive2SEs = self.__getPluginParam( 'Archive2SEs', ['CNAF-ARCHIVE', 'GRIDKA-ARCHIVE', 'IN2P3-ARCHIVE', 'SARA-ARCHIVE', 'PIC-ARCHIVE', 'RAL-ARCHIVE'] )
    archive1ActiveSEs = self.__getActiveSEs( archive1SEs )
    numberOfCopies = self.__getPluginParam( 'NumberOfReplicas', 2 )
    if not archive1ActiveSEs:
      archive1ActiveSEs = archive1SEs
    archive2ActiveSEs = self.__getActiveSEs( archive2SEs )
    if not archive2ActiveSEs:
      archive2ActiveSEs = archive2SEs
    if archive1ActiveSEs:
      archive1SE = [randomize( archive1ActiveSEs )[0]]
    else:
      archive1SE = []
    return self.__simpleReplication( archive1SE, archive2ActiveSEs, numberOfCopies = numberOfCopies )

  def __simpleReplication( self, mandatorySEs, secondarySEs, numberOfCopies = 0 ):
    self.__logInfo( "Starting execution of plugin" )
    transID = self.params['TransformationID']
    if not numberOfCopies:
      numberOfCopies = len( secondarySEs ) + len( mandatorySEs )
      activeSecondarySEs = secondarySEs
    else:
      activeSecondarySEs = self.__getActiveSEs( secondarySEs )
      numberOfCopies = max( len( mandatorySEs ), numberOfCopies )

    replicaGroups = self._getFileGroups( self.data )

    alreadyCompleted = []
    fileTargetSEs = {}
    for replicaSE, lfnGroup in replicaGroups.items():
      existingSEs = replicaSE.split( ',' )
      # If there is no choice on the SEs, send all files at once, otherwise make chunks
      if numberOfCopies >= len( mandatorySEs ) + len( activeSecondarySEs ):
        lfnChunks = [lfnGroup]
      else:
        lfnChunks = breakListIntoChunks( lfnGroup, 100 )

      for lfns in lfnChunks:
        candidateSEs = self.__closerSEs( existingSEs, activeSecondarySEs )
        # Remove existing SEs from list of candidates
        ncand = len( candidateSEs )
        candidateSEs = [se for se in candidateSEs if se not in existingSEs]
        needToCopy = numberOfCopies - ( ncand - len( candidateSEs ) )
        stillMandatory = [se for se in mandatorySEs if se not in candidateSEs]
        candidateSEs = stillMandatory + candidateSEs
        needToCopy = max( needToCopy, len( stillMandatory ) )
        targetSEs = []
        if needToCopy > 0:
          if needToCopy <= len( candidateSEs ):
            targetSEs = candidateSEs[0:needToCopy]
          else:
            targetSEs = [se for se in candidateSEs]
            needToCopy -= len( targetSEs )
            # Try and replicate to non active SEs
            otherSEs = [se for se in secondarySEs if se not in targetSEs]
            if otherSEs:
              targetSEs += otherSEs[0:min( needToCopy, len( otherSEs ) )]
        else:
          alreadyCompleted += lfns
        if targetSEs:
          stringTargetSEs = ','.join( sortList( targetSEs ) )
          #Now assign the individual files to their targets
          ( chunkFileTargetSEs, completed ) = self.__assignTargetToLfns( lfns, stringTargetSEs )
          alreadyCompleted += completed
          fileTargetSEs.update( chunkFileTargetSEs )

    # Update the status of the already done files
    if alreadyCompleted:
      self.__logInfo( "Found %s files that are already completed" % len( alreadyCompleted ) )
      self.transClient.setFileStatusForTransformation( transID, 'Processed', alreadyCompleted )

    # Now group all of the files by their target SEs
    storageElementGroups = {}
    for lfn, stringTargetSEs in fileTargetSEs.items():
      storageElementGroups.setdefault( stringTargetSEs, [] ).append( lfn )

    return S_OK( self.__createTasks( storageElementGroups ) )

  def _FakeReplication( self ):
    storageElementGroups = {}
    for replicaSE, lfnGroup in self._getFileGroups( self.data ).items():
      existingSEs = replicaSE.split( ',' )
      for lfns in breakListIntoChunks( lfnGroup, 100 ):
        stringTargetSEs = existingSEs[0]
        storageElementGroups.setdefault( stringTargetSEs, [] ).extend( lfns )
    if self.pluginCallback:
      self.pluginCallback( self.params['TransformationID'], invalidateCache = True )
    return S_OK( self.__createTasks( storageElementGroups ) )

  def __createTasks( self, storageElementGroups, chunkSize = 100 ):
    #  create reasonable size tasks
    tasks = []
    for stringTargetSEs in sortList( storageElementGroups.keys() ):
      stringTargetLFNs = storageElementGroups[stringTargetSEs]
      for lfnGroup in breakListIntoChunks( sortList( stringTargetLFNs ), 100 ):
        tasks.append( ( stringTargetSEs, lfnGroup ) )
    self.__logVerbose( "%d tasks created" % len( tasks ) )
    return tasks

  def _DestroyDataset( self ):
    return self.__removeReplicas( keepSEs = [], minKeep = 0 )

  def _DeleteDataset( self ):
    keepSEs = self.__getPluginParam( 'KeepSEs', ['CERN-ARCHIVE', 'CNAF-ARCHIVE', 'GRIDKA-ARCHIVE', 'IN2P3-ARCHIVE', 'NIKHEF-ARCHIVE', 'SARA-ARCHIVE', 'PIC-ARCHIVE', 'RAL-ARCHIVE'] )
    return self.__removeReplicas( keepSEs = keepSEs, minKeep = 0 )

  def _DeleteReplicas( self ):
    listSEs = self.__getPluginParam( 'FromSEs', None )
    keepSEs = self.__getPluginParam( 'KeepSEs', ['CERN-ARCHIVE', 'CNAF-ARCHIVE', 'GRIDKA-ARCHIVE', 'IN2P3-ARCHIVE', 'NIKHEF-ARCHIVE', 'SARA-ARCHIVE', 'PIC-ARCHIVE', 'RAL-ARCHIVE'] )
    mandatorySEs = self.__getPluginParam( 'MandatorySEs', ['CERN_MC_M-DST', 'CERN_M-DST', 'CERN-DST', 'CERN_MC-DST'] )
    # this is the number of replicas to be kept in addition to keepSEs and mandatorySEs
    minKeep = self.__getPluginParam( 'NumberOfReplicas', 1 )

    return self.__removeReplicas( listSEs = listSEs, keepSEs = keepSEs + mandatorySEs, minKeep = minKeep )

  def __removeReplicas( self, listSEs = [], keepSEs = [], minKeep = 999 ):
    self.__logInfo( "Starting execution of plugin" )
    transID = self.params['TransformationID']
    nKeep = min( 2, len( keepSEs ) )
    #print nKeep, listSEs, keepSEs

    replicaGroups = self._getFileGroups( self.data )

    storageElementGroups = {}
    for replicaSE, lfns in replicaGroups.items():
      replicaSE = replicaSE.split( ',' )
      if minKeep == 0 and keepSEs:
        # Check that the dataset exists at least at 2 keepSE
        if len( [se for se in replicaSE if se in keepSEs] ) < nKeep:
          self.__logInfo( "Found %d files that are not in %d keepSEs, no removal done" % ( len( lfns ), nKeep ) )
          self.transClient.setFileStatusForTransformation( transID, 'Processed', lfns )
          continue
      existingSEs = [se for se in replicaSE if not se in keepSEs]
      #print "Existing:",existingSEs,", minKeep", minKeep, ", keepSEs", keepSEs
      if minKeep == 0:
        targetSEs = [se for se in existingSEs]
      else:
        targetSEs = []
        if len( existingSEs ) > minKeep:
          # explicit deletion
          if listSEs:
            # check how  many replicas would be left if we remove from listSEs
            nLeft = len( [se for se in existingSEs if not se in listSEs] )
            if nLeft >= minKeep:
              # we can delete all replicas in listSEs
              targetSEs = [se for se in listSEs if se in existingSEs]
            else:
              # we should keep some in listSEs, too bad
              targetSEs = randomize( [se for se in listSEs if se in existingSEs] )[0:minKeep - nLeft]
              self.__logInfo( "Found %d files that could only be deleted in %s of the requested SEs" % ( len( lfns ), minKeep - nLeft ) )
          else:
            # remove all replicas and keep only minKeep
            targetSEs = randomize( existingSEs )[0:-minKeep]

      if targetSEs:
        stringTargetSEs = ','.join( sortList( targetSEs ) )
        storageElementGroups.setdefault( stringTargetSEs, [] ).extend( lfns )
      else:
        self.__logInfo( "Found %s files that don't need any replica deletion" % len( lfns ) )
        self.transClient.setFileStatusForTransformation( transID, 'Processed', lfns )

    return S_OK( self.__createTasks( storageElementGroups ) )

