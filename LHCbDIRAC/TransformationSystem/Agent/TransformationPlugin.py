"""  TransformationPlugin is a class wrapping the supported LHCb transformation plugins
"""

__RCSID__ = "$Id$"

import time, datetime, os, random

from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Utilities.List import breakListIntoChunks, randomize, uniqueElements
from DIRAC.TransformationSystem.Agent.TransformationPlugin import TransformationPlugin as DIRACTransformationPlugin
from LHCbDIRAC.TransformationSystem.Client.Utilities \
     import PluginUtilities, getFileGroups, groupByRun, isArchive, isFailover, \
     sortExistingSEs, getRemovalPlugins, closerSEs
from LHCbDIRAC.BookkeepingSystem.Client.BKQuery import BKQuery
from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
from LHCbDIRAC.ResourceStatusSystem.Client.ResourceManagementClient import ResourceManagementClient
from DIRAC.ResourceStatusSystem.Client.ResourceStatus import ResourceStatus

class TransformationPlugin( DIRACTransformationPlugin ):
  """ Extension of DIRAC TransformationPlugin - instantiated by the TransformationAgent
  """

  def __init__( self, plugin,
                transClient = None, dataManager = None,
                bkkClient = None, rmClient = None, rss = None,
                debug = False, transInThread = None ):
    """ The clients can be passed in.
    """
    DIRACTransformationPlugin.__init__( self, plugin, transClient = transClient, dataManager = dataManager )

    if not bkkClient:
      self.bkClient = BookkeepingClient()
    else:
      self.bkClient = bkkClient

    if not rmClient:
      self.rmClient = ResourceManagementClient()
    else:
      self.rmClient = rmClient

    if not rss:
      self.resourceStatus = ResourceStatus()
    else:
      self.resourceStatus = rss

    self.params = {}
    self.workDirectory = None
    self.pluginCallback = self.voidMethod
    self.startTime = time.time()
    self.transReplicas = {}
    self.transFiles = []
    self.transID = None
    self.debug = debug
    self.util = PluginUtilities( plugin, self.transClient, self.dm, self.bkClient,
                                 self.rmClient, self.resourceStatus, debug,
                                 transInThread if transInThread else {} )
    self.setDebug( self.util.getPluginParam( 'Debug', False ) )

  def voidMethod( self, id, invalidateCache = False ):
    return

  def setInputData( self, data ):
    """
    self.transReplicas are the replica location of the transformation files.
    However if some don't have a replica, it is not in this dictionary
    self.transReplicas[lfn] == [ SE1, SE2...]
    """
    # data is a synonym as used in DIRAC
    self.transReplicas = data.copy()
    self.data = self.transReplicas

  def setTransformationFiles( self, files ):
    """
    self.transFiles are all the Unused files for that transformation
    It is a list of dictionaries, of which lfn = fileDict['LFN']
    Keys are: ['ErrorCount', 'FileID', 'InsertedTime', 'LFN', 'LastUpdate',
              'RunNumber', 'Status', 'TargetSE', 'TaskID', 'TransformationID', 'UsedSE']
    """
    # files is a synonym, as used in DIRAC
    self.transFiles = [fileDict for fileDict in files]
    self.files = self.transFiles

  def setParameters( self, params ):
    self.params = params
    self.transID = params['TransformationID']
    self.util.setParameters( params )
    self.setDebug( self.util.getPluginParam( 'Debug', False ) )

  def setDebug( self, val = True ):
    self.debug = val or self.debug
    self.util.setDebug( val )

  def __cleanFiles( self, status = None ):
    """
    Remove from transFiles all files without a replica and set their status
    """
    noReplicaFiles = []
    for fileDict in [fileDict for fileDict in self.transFiles]:
      if fileDict['LFN'] not in self.transReplicas:
        noReplicaFiles.append( fileDict['LFN'] )
        self.transFiles.remove( fileDict )
    if noReplicaFiles:
      if not status:
        status = 'Problematic' if self.plugin not in getRemovalPlugins() else 'Processed'
      info = 'without replicas' if status == 'Problematic' else 'already processed'
      res = self.transClient.setFileStatusForTransformation( self.transID, status, noReplicaFiles )
      if not res['OK']:
        self.util.logError( 'Error setting file status for %d files' % len( noReplicaFiles ), res['Message'] )
      else:
        self.util.logInfo( 'Found %d files %s, status set to %s' % ( len( noReplicaFiles ), info, status ) )

  def __del__( self ):
    self.util.logInfo( "Execution finished, timing: %.3f seconds" % ( time.time() - self.startTime ) )

  def _removeProcessedFiles( self ):
    """
    Checks if the LFNs have descendants in the same transformation. Removes them from self.transReplicas
    and sets them 'Processed'
    """
    startTime = time.time()
    descendants = self.util.getProcessedFiles( self.transReplicas.keys() )
    self.util.logVerbose( "Got descendants of %d files in %.3f seconds" % ( len( self.transReplicas ),
                                                                            time.time() - startTime ) )
    if descendants:
      processedLfns = [lfn for lfn in descendants if descendants[lfn]]
      if processedLfns:
        res = self.transClient.setFileStatusForTransformation( self.transID, 'Processed', processedLfns )
        if res['OK']:
          self.util.logInfo( "Found %d input files that have already been processed (status set)" % len( processedLfns ) )
          for lfn in processedLfns:
            self.transReplicas.pop( lfn, None )
          for fileDict in [fileDict for fileDict in self.transFiles if fileDict['LFN'] in processedLfns]:
            self.transFiles.remove( fileDict )
      else:
        # Here one should check descendants of children
        self.util.logVerbose( "No input files have already been processed" )

  def _RAWShares( self ):
    """
    Plugin for replicating RAW data to Tier1s according to shares, excluding CERN which is the source of transfers...
    """
    self.util.logInfo( "Starting execution of plugin" )
    possibleTargets = ['CNAF-RAW', 'GRIDKA-RAW', 'IN2P3-RAW', 'PIC-RAW', 'RAL-RAW', 'SARA-RAW']
    sourceSE = 'CERN-RAW'

    # Get the requested shares from the CS
    res = self.util.getShares( section = 'RAW' )
    if not res['OK']:
      self.util.logError( "Section RAW in Shares not available" )
      return res
    existingCount, targetShares = res['Value']

    # Ensure that our files only have one existing replica at CERN
    alreadyReplicated = {}
    for replicaSE, lfns in getFileGroups( self.transReplicas ).items():
      existingSEs = replicaSE.split( ',' )
      if len( existingSEs ) > 1:
        for lfn in lfns:
          self.transReplicas.pop( lfn )
        alreadyReplicated.setdefault( [se for se in existingSEs if se != sourceSE][0], [] ).extend( lfns )
    if alreadyReplicated:
      # Remove files form the transFiles
      self.__cleanFiles( status = 'Processed' )

    # Group the remaining data by run
    res = groupByRun( self.transFiles )
    if not res['OK']:
      return res
    runFileDict = res['Value']
    if not runFileDict:
      # No files, no tasks!
      return S_OK( [] )

    # For each of the runs determine the destination of any previous files
    res = self.transClient.getTransformationRuns( {'TransformationID':self.transID, 'RunNumber':runFileDict.keys()} )
    if not res['OK']:
      self.util.logError( "Failed to obtain TransformationRuns", res['Message'] )
      return res
    runSEDict = dict( [( runDict['RunNumber'], runDict['SelectedSite'] ) for runDict in res['Value']] )

    # Choose the destination SE
    tasks = []
    for runID in [run for run in runFileDict if run in runSEDict]:
      runLfns = runFileDict[runID]
      assignedSE = runSEDict[runID]
      if assignedSE:
        update = False
      else:
        update = True
        # No SE assigned yet
        res = self._getNextSite( existingCount, targetShares )
        if not res['OK']:
          self.util.logError( "Failed to get next destination SE", res['Message'] )
        else:
          assignedSE = res['Value']
          if assignedSE:
            self.util.logVerbose( "Run %d (%d files) assigned to %s" % ( runID, len( runLfns ), assignedSE ) )

      if assignedSE in possibleTargets:
        # Update the TransformationRuns table with the assigned (if this fails do not create the tasks)
        if update:
          res = self.transClient.setTransformationRunsSite( self.transID, runID, assignedSE )
          if not res['OK']:
            self.util.logError( "Failed to assign TransformationRun SE", res['Message'] )
            return res
        # Create the tasks
        tasks.append( ( assignedSE, runLfns ) )
    return S_OK( tasks )

  def _AtomicRun( self ):
    """
    Plugin for Reconstruction and reprocessing
    It uses the assigned shares per site and waits for files to be replicated
    """
    self.util.logInfo( "Starting execution of plugin" )
    delay = self.util.getPluginParam( 'RunDelay', 1 )
    minNbReplicas = 2
    self._removeProcessedFiles()
    # Get the requested shares from the CS
    backupSE = 'CERN-RAW'
    res = self.util.getCPUShares( self.transID, backupSE )
    if not res['OK']:
      return res
    else:
      rawFraction, cpuShares = res['Value']
      outsideFraction = 0.
      for frac in rawFraction.values():
        outsideFraction += frac
      if outsideFraction == 0.:
        minNbReplicas = 1

    transType = self.util.getPluginParam( 'Type' )

    if transType == 'DataReconstruction':
      fractionToProcess = self.util.getPluginParam( 'FractionToProcess', 1. )
      if fractionToProcess != 1.:
        minFilesToProcess = self.util.getPluginParam( 'MinFilesToProcess', 100 )
    else:
      fractionToProcess = 1.

    activeRAWSEs = self.util.getActiveSEs( cpuShares.keys() )
    inactiveRAWSEs = [se for se in cpuShares if se not in activeRAWSEs]
    self.util.logVerbose( "Active RAW SEs: %s" % activeRAWSEs )
    if inactiveRAWSEs:
      self.util.logInfo( "Some RAW SEs are not active: %s" % inactiveRAWSEs )

    # Group the remaining data by run
    res = groupByRun( self.transFiles )
    if not res['OK']:
      return res
    runFileDict = res['Value']
    if not runFileDict:
      return S_OK()
    zeroRun = runFileDict.pop( 0, None )
    if zeroRun:
      self.util.logInfo( "Setting run number for files with run #0, which means it was not set yet" )
      newRuns = self.util.setRunForFiles( zeroRun )
      for newRun, runLFNs in newRuns.items():
        runFileDict.setdefault( newRun, [] ).extend( runLFNs )


    # For each of the runs determine the destination of any previous files
    runUpdate = {}
    runSEDict = {}
    res = self.transClient.getTransformationRuns( {'TransformationID':self.transID, 'RunNumber':runFileDict.keys()} )
    if not res['OK']:
      self.util.logError( "Failed to obtain TransformationRuns", res['Message'] )
      return res
    for runDict in res['Value']:
      runID = runDict['RunNumber']
      if transType == 'DataReconstruction':
        # Wait for 'delay' hours before starting the task
        res = self.bkClient.getRunInformations( int( runID ) )
        if res['OK']:
          endDate = res['Value']['RunEnd']
          if datetime.datetime.now() - endDate < datetime.timedelta( hours = delay ):
            self.util.logInfo( 'Run %d was taken less than %d hours ago, skip...' % ( runID, delay ) )
            if runID in runFileDict:
              runFileDict.pop( runID )
            continue
          else:
            self.util.logVerbose( 'Run %d was taken more than %d hours ago, we take!' % ( runID, delay ) )
        else:
          self.util.logError( "Error getting run information for run %d (skipped):" % runID, res['Message'] )
          continue
      if runDict['SelectedSite']:
        runSEDict[runID] = runDict['SelectedSite']
        runUpdate[runID] = False
      else:
        res = self.transClient.getTransformationFiles( condDict = {'TransformationID':self.transID,
                                                                 'RunNumber':runID,
                                                                 'Status':['Assigned', 'Processed']} )
        if not res['OK']:
          self.util.logError( "Failed to get transformation files for run", "%s %s" % ( runID, res['Message'] ) )
        else:
          if res['Value']:
            lfnSEs = dict( [( tDict['LFN'], tDict['UsedSE'] ) for tDict in res['Value']] )
            sortedSEs = sortExistingSEs( lfnSEs )
            if len( sortedSEs ) > 1:
              self.util.logWarn( 'For run %d, files are assigned to more than one site: %s' % ( runID, sortedSEs ) )
            runSEDict[runID] = sortedSEs[0]
            runUpdate[runID] = True

    # Choose the destination site for new runs
    for runID in [run for run in sorted( runFileDict ) if run not in runSEDict]:
      runLfns = runFileDict[runID]
      distinctSEs = []
      for lfn in runLfns:
        distinctSEs += [se for se in self.transReplicas.get( lfn, {} ) if se not in distinctSEs and se in activeRAWSEs]
      if len( distinctSEs ) < minNbReplicas:
        self.util.logInfo( "Not found %d active candidate SEs for run %d, skipped" \
                           % ( minNbReplicas, runID ) )
        continue
      seProbs = {}
      prob = 0.
      if backupSE not in distinctSEs:
        self.util.logWarn( " %s not in the SEs for run %d" % ( backupSE, runID ) )
        backupSE = None
      distinctSEs = sorted( [se for se in distinctSEs if se in rawFraction and se != backupSE] )
      selectedSE = None
      if not distinctSEs:
        # If the file is at a single SE, and OK, it must be backupSE
        selectedSE = backupSE
      else:
        for se in distinctSEs:
          prob += rawFraction[se] / len( distinctSEs )
          seProbs[se] = prob
        if backupSE:
          seProbs[backupSE] = 1.
          distinctSEs.append( backupSE )
        # get a random number between 0 and 1
        rand = random.uniform( 0., 1. )
        strProbs = ','.join( [' %s:%.3f' % ( se, seProbs[se] ) for se in distinctSEs] )
        self.util.logInfo( "For run %d, SE integrated fraction =%s, random number = %.3f" % ( runID, strProbs, rand ) )
        for se in distinctSEs:
          prob = seProbs[se]
          if rand <= prob:
            selectedSE = se
            break
        self.util.logVerbose( "Selected SE for reconstruction is %s" % selectedSE )
      if selectedSE:
        runSEDict[runID] = selectedSE
        runUpdate[runID] = True
        self.util.logInfo( "Run %d (%d files) assigned to %s" % ( runID, len( runLfns ), selectedSE ) )

    # Create the tasks
    tasks = []
    for runID in sorted( runSEDict ):
      selectedSE = runSEDict[runID]
      nbNew = len( runFileDict[runID] )
      self.util.logInfo( "Creating tasks for run %d, targetSE %s (%d files)" % ( runID, selectedSE, nbNew ) )
      if not selectedSE:
        self.util.logWarn( "Run %d has no targetSE, skipped..." % runID )
        continue
      if runUpdate[runID]:
        self.util.logVerbose( "Assign run site for run %d: %s" % ( runID, selectedSE ) )
        # Update the TransformationRuns table with the assigned (if this fails do not create the tasks)
        res = self.transClient.setTransformationRunsSite( self.transID, runID, selectedSE )
        if not res['OK']:
          self.util.logError( "Failed to assign TransformationRun site", res['Message'] )
          continue
      status = self.params['Status']
      self.params['Status'] = 'Flush'
      lfns = [lfn for lfn in runFileDict[runID] \
              if len( self.transReplicas.get( lfn, [] ) ) >= minNbReplicas]
      notConsidered = [lfn for lfn in runFileDict[runID] if lfn not in lfns]
      if notConsidered:
        self.util.logVerbose( "Run %s: %d files are not considered (not %d replicas)" % ( runID, len( notConsidered ),
                                                                                          minNbReplicas ) )
      if not lfns:
        continue
      if fractionToProcess == 1.:
        runFraction = 1.
      else:
        res = self.transClient.getTransformationFiles( condDict = {'TransformationID':self.transID, 'RunNumber':runID} )
        if not res['OK']:
          self.util.logError( "Failed to get transformation files for run", "%s %s" % ( runID, res['Message'] ) )
          continue
        else:
          nbRaw = len( res['Value'] )
        res = self.transClient.getTransformationFiles( condDict = {'TransformationID':self.transID,
                                                                   'RunNumber':runID,
                                                                   'Status':['Assigned', 'Processed']} )
        if not res['OK']:
          self.util.logError( "Failed to get transformation files for run", "%s %s" % ( runID, res['Message'] ) )
          continue
        else:
          nbSubmitted = len( res['Value'] )
        # Let's compute which fraction should be processed
        if nbRaw <= minFilesToProcess:
          runFraction = 1.
        elif nbRaw < minFilesToProcess / fractionToProcess:
          runFraction = float( minFilesToProcess ) / nbRaw
        else:
          runFraction = fractionToProcess
        # Now adjust taking into account the files already submitted
        runFraction = min( max( 0., ( runFraction * nbRaw - nbSubmitted ) / nbNew ), 1. )
        self.util.logInfo( 'Run %s: %d RAW files, %d submitted, will process %.1f%% of %d new files'
                        % ( runID, nbRaw, nbSubmitted, 100. * runFraction, nbNew ) )
      if runFraction == 0.:
        # No need to group, just create a fake task for all files
        res = { 'OK': True, 'Value':[( '', lfns )]}
      else:
        res = self._groupBySize( lfns )
      self.params['Status'] = status
      if res['OK']:
        notProcessed = 0
        total = 0
        self.util.logVerbose( "groupBySize returned %d tasks for %d files" % ( len( res['Value'] ), len( lfns ) ) )
        for task in res['Value']:
          total += len( task[1] )
          if runFraction != 1.:
            # Decide whether the files should be processed or not
            rand = random.uniform( 0., 1. )
            if rand > runFraction:
              # Don't process this/these file
              notProcessed += len( task[1] )
              res = self.transClient.setFileStatusForTransformation( self.transID, 'NotProcessed', task[1] )
              if not res['OK']:
                self.util.logError( "Error setting file status NotProcessed for %d files" % len( task[1] ),
                                    res['Message'] )
              else:
                res = self.bkClient.setFilesInvisible( task[1] )
                if not res['OK']:
                  self.util.logError( "Error setting %d files invisible in BK" % len( task[1] ), res['Message'] )
              continue
          finalSE = selectedSE if selectedSE in task[0].split( ',' ) else backupSE
          if finalSE in task[0].split( ',' ):
            if finalSE != selectedSE:
              self.util.logVerbose( "Using backup SE %s as target as file not available at %s" % ( backupSE, selectedSE ) )
            # Set the RAW files visible in the BK (only needed for reprocessing)
            if transType == 'DataReprocessing':
              res = self.bkClient.setFilesVisible( task[1] )
              if not res['OK']:
                self.util.logError( "Error setting %d files visible in BK" % len( task[1] ), res['Message'] )
            else:
              res = {'OK': True}
            if res['OK']:
              tasks.append( ( finalSE, task[1] ) )
          else:
            self.util.logVerbose( 'Task not created: %s' % str( task ) )
        if notProcessed:
          self.util.logVerbose( 'Run %s: of %d files, only %d will be processed (requested %.1f%%, min %d)' \
                             % ( runID, total, total - notProcessed, 100. * runFraction, minFilesToProcess ) )
      else:
        self.util.logError( 'Error grouping files by size', res['Message'] )

    if self.pluginCallback:
      self.pluginCallback( self.transID, invalidateCache = True )
    return S_OK( tasks )


  def _groupBySize( self, files = None ):
    """
    Generate a task for a given amount of data at a (set of) SE
    """
    if not files:
      files = self.transReplicas
    else:
      files = dict( zip( files, [self.transReplicas[lfn] for lfn in files] ) )
    return self.util.groupBySize( files, self.params['Status'] )

  def _LHCbStandard( self ):
    """ Plugin grouping files at same sites based on number of files,
        used for example for stripping or WG productions
    """
    return self.util.groupByReplicas( self.transReplicas, self.params['Status'] )

  def _ByRun( self, param = '', plugin = 'LHCbStandard', requireFlush = False ):
    try:
      return self.__byRun( param, plugin, requireFlush )
    except Exception:
      self.util.logException( "Exception in _ByRun plugin:" )
      return S_ERROR( [] )

  def __byRun( self, param = '', plugin = 'LHCbStandard', requireFlush = False ):
    """ Basic plugin for when you want to group files by run
    """
    self.util.logInfo( "Starting execution of plugin" )
    allTasks = []
    typesWithNoCheck = self.util.getPluginParam( 'NoCheckTypes', ['Merge'] )
    if self.params['Type'] not in typesWithNoCheck:
      self._removeProcessedFiles()
    self.util.readCacheFile( self.workDirectory )
    if not self.transReplicas:
      self.util.logVerbose( "No data to be processed by plugin" )
      return S_OK( allTasks )
    res = self.util.groupByRunAndParam( self.transReplicas, self.transFiles, param = param )
    if not res['OK']:
      self.util.logError( "Error when grouping %d files by run for param %s" % ( len( self.transReplicas ), param ) )
      return res
    runDict = res['Value']
    zeroRunDict = runDict.pop( 0, None )
    if zeroRunDict:
      self.util.logInfo( "Setting run number for files with run #0, which means it was not set yet" )
      for paramValue, zeroRun in zeroRunDict.items():
        newRuns = self.util.setRunForFiles( zeroRun )
        for newRun, runLFNs in newRuns.items():
          runDict.setdefault( newRun, {} ).setdefault( paramValue, [] ).extend( runLFNs )
    transStatus = self.params['Status']
    startTime = time.time()
    res = self.transClient.getTransformationRuns( {'TransformationID':self.transID, 'RunNumber':runDict.keys()} )
    if not res['OK']:
      self.util.logError( "Error when getting transformation runs for runs %s" % str( runDict.keys() ) )
      return res
    self.util.logVerbose( "Obtained %d runs from transDB in %.1f seconds" % ( len( res['Value'] ),
                                                                              time.time() - startTime ) )
    runSites = dict( [ ( run['RunNumber'], run['SelectedSite'] ) for run in res['Value'] if run['SelectedSite'] ] )
    # Loop on all runs that have new files
    inputData = self.transReplicas.copy()
    setInputData = set( inputData )
    runEvtType = {}
    nRunsLeft = len( res['Value'] )
    for run in sorted( res['Value'], cmp = ( lambda d1, d2: int( d1['RunNumber'] - d2['RunNumber'] ) ) ):
      runID = run['RunNumber']
      self.util.logDebug( "Processing run %d, still %d runs left" % ( runID, nRunsLeft ) )
      nRunsLeft -= 1
      if transStatus == 'Flush':
        runStatus = 'Flush'
      else:
        runStatus = run['Status']
      paramDict = runDict.get( runID, {} )
      targetSEs = [se for se in runSites.get( runID, '' ).split( ',' ) if se]
      for paramValue in sorted( paramDict ):
        if paramValue:
          paramStr = " (%s : %s) " % ( param, paramValue )
        else:
          paramStr = " "
        runParamLfns = set( paramDict[paramValue] )
        # Check if something was new since last time...
        cachedLfns = self.util.getCachedRunLFNs( runID, paramValue )
        newLfns = runParamLfns - cachedLfns
        if len( newLfns ) == 0 and self.transID > 0 and runStatus != 'Flush' and not self.util.cacheExpired( runID ):
          self.util.logVerbose( "No new files since last time for run %d%s: skip..." % ( runID, paramStr ) )
          continue
        self.util.logVerbose( "Of %d files, %d are new for %d%s" % ( len( runParamLfns ),
                                                                  len( newLfns ), runID, paramStr ) )
        runFlush = requireFlush
        if runFlush:
          if not runEvtType.get( paramValue ):
            lfn = paramDict[paramValue][0]
            res = self.util.getBookkeepingMetadata( [lfn], 'EventType' )
            if res['OK']:
              runEvtType[paramValue] = res['Value'].get( lfn, 90000000 )
              self.util.logDebug( 'Event type%s: %s' % ( paramStr, str( runEvtType[paramValue] ) ) )
            else:
              self.util.logWarn( "Can't determine event type for transformation%s, can't flush" % paramStr,
                              res['Message'] )
              runEvtType[paramValue] = None
          evtType = runEvtType[paramValue]
          if not evtType:
            runFlush = False
        runParamReplicas = {}
        for lfn in runParamLfns & setInputData:
          runParamReplicas[lfn ] = [se for se in inputData[lfn] if not isArchive( se )]
        # We need to replace the input replicas by those of this run before calling the helper plugin
        # As it may use self.data, set both transReplicas and data members
        self.data = self.transReplicas = runParamReplicas
        status = runStatus
        if status != 'Flush' and runFlush:
          # If all files in that run have been processed and received, flush
          # Get the number of RAW files in that run
          rawFiles = self.util.getNbRAWInRun( runID, evtType )
          ancestorRawFiles = self.util.getRAWAncestorsForRun( runID, param, paramValue )
          self.util.logVerbose( "Obtained %d ancestor RAW files" % ancestorRawFiles )
          runProcessed = ( ancestorRawFiles == rawFiles )
          if runProcessed:
            # The whole run was processed by the parent production and we received all files
            self.util.logInfo( "All RAW files (%d) ready for run %d%s- Flushing %d files" % ( rawFiles,
                                                                                            runID, paramStr,
                                                                                            len( runParamReplicas ) ) )
            status = 'Flush'
            self.transClient.setTransformationRunStatus( self.transID, runID, 'Flush' )
          else:
            self.util.logVerbose( "Only %d ancestor RAW files (of %d) available for run %d" % ( ancestorRawFiles, rawFiles, runID ) )
        self.params['Status'] = status
        # Now calling the helper plugin
        startTime = time.time()
        res = eval( 'self._%s()' % plugin )
        self.util.logVerbose( "Executed helper plugin %s for %d files (Run %d%s) in %.1f seconds" % ( plugin,
                                                                                                      len( self.data ), runID,
                                                                                                      paramStr,
                                                                                                      time.time() - startTime ) )
        self.params['Status'] = transStatus
        if not res['OK']:
          return res
        tasks = res['Value']
        self.util.logVerbose( "Created %d tasks for run %d%s" % ( len( tasks ), runID, paramStr ) )
        allTasks.extend( tasks )
        # Cache the left-over LFNs
        taskLfns = []
        newSEs = []
        for task in tasks:
          newSEs += [se for se in task[0].split( ',' ) if se not in targetSEs and se not in newSEs ]
          taskLfns += task[1]
        if newSEs:
          targetSEs += newSEs
          self.util.logVerbose( "Set target SEs for run %s as %s" % ( str( runID ), str( targetSEs ) ) )
          res = self.transClient.setTransformationRunsSite( self.transID, runID, ','.join( targetSEs ) )
          if not res['OK']:
            self.util.logError( "Failed to set target SEs to run %s as %s" % ( str( runID ), str( targetSEs ) ),
                             res['Message'] )
        self.util.setCachedRunLfns( runID, paramValue, [lfn for lfn in runParamLfns if lfn not in taskLfns] )
    # reset the input data as it was when calling the plugin
    self.setInputData( inputData )
    self.util.writeCacheFile()
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

  def _LHCbDSTBroadcast( self ):
    """ Usually for replication of real data (4 copies)
    """
    archive1SEs = self.util.getPluginParam( 'Archive1SEs', ['CERN-ARCHIVE'] )
    archive2SEs = self.util.getPluginParam( 'Archive2SEs', ['CNAF-ARCHIVE', 'GRIDKA-ARCHIVE', 'IN2P3-ARCHIVE',
                                                            'SARA-ARCHIVE', 'PIC-ARCHIVE', 'RAL-ARCHIVE'] )
    mandatorySEs = self.util.getPluginParam( 'MandatorySEs', ['CERN-DST'] )
    secondarySEs = self.util.getPluginParam( 'SecondarySEs', ['CNAF-DST', 'GRIDKA-DST', 'IN2P3-DST', 'SARA-DST',
                                                              'PIC-DST', 'RAL-DST'] )
    numberOfCopies = self.util.getPluginParam( 'NumberOfReplicas', 4 )
    return self._lhcbBroadcast( archive1SEs, archive2SEs, mandatorySEs, secondarySEs, numberOfCopies, forceRun = True )

  def _LHCbMCDSTBroadcast( self ):
    """ For replication of MC data (3 copies)
    """
    archive1SEs = self.util.getPluginParam( 'Archive1SEs', [] )
    archive2SEs = self.util.getPluginParam( 'Archive2SEs', ['CERN-ARCHIVE', 'CNAF-ARCHIVE', 'GRIDKA-ARCHIVE',
                                                            'IN2P3-ARCHIVE', 'SARA-ARCHIVE',
                                                            'PIC-ARCHIVE', 'RAL-ARCHIVE'] )
    mandatorySEs = self.util.getPluginParam( 'MandatorySEs', ['CERN_MC_M-DST'] )
    secondarySEs = self.util.getPluginParam( 'SecondarySEs', ['CNAF_MC-DST', 'GRIDKA_MC-DST', 'IN2P3_MC-DST',
                                                              'SARA_MC-DST', 'PIC_MC-DST', 'RAL_MC-DST'] )
    numberOfCopies = self.util.getPluginParam( 'NumberOfReplicas', 3 )
    return self._lhcbBroadcast( archive1SEs, archive2SEs, mandatorySEs, secondarySEs, numberOfCopies )

  def _lhcbBroadcast( self, archive1SEs, archive2SEs, mandatorySEs, secondarySEs, numberOfCopies, forceRun = False ):
    """ This plug-in broadcasts files to one archive1SE, one archive2SE and numberOfCopies secondarySEs
        All files for the same run have the same target
    """
    self.util.logInfo( "Starting execution of plugin" )

    # We need at least all mandatory copies
    numberOfCopies = max( numberOfCopies, len( mandatorySEs ) )

    # Group the remaining data by run
    res = groupByRun( self.transFiles )
    if not res['OK']:
      return res
    runFileDict = res['Value']
    if not runFileDict:
      # No files, no tasks!
      return S_OK( [] )
    if forceRun:
      zeroRun = runFileDict.pop( 0, None )
      if zeroRun:
        self.util.logInfo( "Setting run number for files with run #0, which means it was not set yet" )
        newRuns = self.util.setRunForFiles( zeroRun )
        for newRun, runLFNs in newRuns.items():
          runFileDict.setdefault( newRun, [] ).extend( runLFNs )
    if not runFileDict:
      return S_OK( [] )

    # For each of the runs determine the destination of any previous files
    runSEDict = {}
    runUpdate = {}
    # Make a list of SEs already assigned to runs
    res = self.transClient.getTransformationRuns( {'TransformationID':self.transID, 'RunNumber':runFileDict.keys()} )
    if not res['OK']:
      self.util.logError( "Failed to obtain TransformationRuns", res['Message'] )
      return res
    for runDict in res['Value']:
      runID = runDict['RunNumber']
      # If the run already has a selected site, use it for that run
      if runDict['SelectedSite']:
        runSEDict[runID] = runDict['SelectedSite']
        runUpdate[runID] = False
      else:
        # Check if some files are assigned to an SE in this run
        res = self.transClient.getTransformationFiles( condDict = {'TransformationID':self.transID,
                                                                   'RunNumber':runID,
                                                                   'Status':['Assigned', 'Processed']} )
        if not res['OK']:
          self.util.logError( "Failed to get transformation files for run", "%s %s" % ( runID, res['Message'] ) )
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
        existingSEs = sortExistingSEs( self.transReplicas, runLfns )
        # this may happen when all files are in FAILOVER
        if  existingSEs:
          # Now select the target SEs
          self.util.logVerbose( "Selecting SEs for %d files" % len( runLfns ) )
          self.util.logDebug( "Files: %s" % str( runLfns ) )
          stringTargetSEs = self.util.setTargetSEs( numberOfCopies, archive1SEs, archive2SEs,
                                                    mandatorySEs, secondarySEs, existingSEs, exclusiveSEs = False )
          runUpdate[runID] = True

      # Update the TransformationRuns table with the assigned SEs (don't continue if it fails)
      if stringTargetSEs:
        if runUpdate[runID]:
          res = self.transClient.setTransformationRunsSite( self.transID, runID, stringTargetSEs )
          if not res['OK']:
            self.util.logError( "Failed to assign TransformationRun site", res['Message'] )
            return S_ERROR( "Failed to assign TransformationRun site" )

        # Now assign the individual files to their targets
        targetSEs = stringTargetSEs.split( ',' )
        if 'CNAF-DST' in targetSEs and 'CNAF_M-DST' in targetSEs:
          targetSEs.remove( 'CNAF-DST' )
          stringTargetSEs = ','.join( targetSEs )
        ( runFileTargetSEs, runCompleted ) = self.util.assignTargetToLfns( runLfns, self.transReplicas, stringTargetSEs )
        alreadyCompleted += runCompleted
        fileTargetSEs.update( runFileTargetSEs )

    # Update the status of the already done files
    if alreadyCompleted:
      self.util.logInfo( "Found %s files that are already completed" % len( alreadyCompleted ) )
      self.transClient.setFileStatusForTransformation( self.transID, 'Processed', alreadyCompleted )

    # Now group all of the files by their target SEs
    storageElementGroups = {}
    for lfn, stringTargetSEs in fileTargetSEs.items():
      storageElementGroups.setdefault( stringTargetSEs, [] ).append( lfn )

    return S_OK( self.util.createTasks( storageElementGroups ) )

  def _LHCbMCDSTBroadcastRandom( self ):
    """ This plug-in broadcasts files to archive1, to archive2 and to random (NumberOfReplicas) secondary SEs
    """

    self.util.logInfo( "Starting execution of plugin" )
    archive1SEs = self.util.getPluginParam( 'Archive1SEs', [] )
    archive2SEs = self.util.getPluginParam( 'Archive2SEs', ['CERN-ARCHIVE', 'CNAF-ARCHIVE', 'GRIDKA-ARCHIVE',
                                                            'IN2P3-ARCHIVE', 'SARA-ARCHIVE',
                                                            'PIC-ARCHIVE', 'RAL-ARCHIVE'] )
    mandatorySEs = self.util.getPluginParam( 'MandatorySEs', ['CERN_MC_M-DST'] )
    secondarySEs = self.util.getPluginParam( 'SecondarySEs', ['CNAF_MC-DST', 'GRIDKA_MC-DST', 'IN2P3_MC-DST',
                                                              'SARA_MC-DST', 'PIC_MC-DST', 'RAL_MC-DST'] )
    numberOfCopies = self.util.getPluginParam( 'NumberOfReplicas', 3 )

    # We need at least all mandatory copies
    numberOfCopies = max( numberOfCopies, len( mandatorySEs ) )

    storageElementGroups = {}
    for replicaSE, lfnGroup in getFileGroups( self.transReplicas ).items():
      existingSEs = [se for se in replicaSE.split( ',' ) if not isFailover( se )]
      for lfns in breakListIntoChunks( lfnGroup, 100 ):

        stringTargetSEs = self.util.setTargetSEs( numberOfCopies, archive1SEs, archive2SEs,
                                               mandatorySEs, secondarySEs, existingSEs, exclusiveSEs = True )
        if stringTargetSEs:
          storageElementGroups.setdefault( stringTargetSEs, [] ).extend( lfns )
        else:
          self.util.logInfo( "Found %d files that are already completed" % len( lfns ) )
          self.transClient.setFileStatusForTransformation( self.transID, 'Processed', lfns )

    return S_OK( self.util.createTasks( storageElementGroups ) )


  def _ReplicateDataset( self ):
    """ Plugin for replicating files to specified SEs
    """
    destSEs = self.util.getPluginParam( 'DestinationSEs', [] )
    if not destSEs:
      destSEs = self.util.getPluginParam( 'MandatorySEs', [] )
    secondarySEs = self.util.getPluginParam( 'SecondarySEs', [] )
    fromSEs = self.util.getPluginParam( 'FromSEs', [] )
    numberOfCopies = self.util.getPluginParam( 'NumberOfReplicas', 0 )
    return self._simpleReplication( destSEs, secondarySEs, numberOfCopies, fromSEs = fromSEs )

  def _ArchiveDataset( self ):
    """ Plugin for archiving datasets (normally 2 archives, unless one of the lists is empty)
    """
    archive1SEs = self.util.getPluginParam( 'Archive1SEs', [] )
    archive2SEs = self.util.getPluginParam( 'Archive2SEs', ['CERN-ARCHIVE', 'CNAF-ARCHIVE', 'GRIDKA-ARCHIVE',
                                                            'IN2P3-ARCHIVE', 'SARA-ARCHIVE',
                                                            'PIC-ARCHIVE', 'RAL-ARCHIVE'] )
    archive1ActiveSEs = self.util.getActiveSEs( archive1SEs )
    numberOfCopies = self.util.getPluginParam( 'NumberOfReplicas', 1 )
    if not archive1ActiveSEs:
      archive1ActiveSEs = archive1SEs
    archive2ActiveSEs = self.util.getActiveSEs( archive2SEs )
    if not archive2ActiveSEs:
      archive2ActiveSEs = archive2SEs
    if archive1ActiveSEs:
      archive1SE = [randomize( archive1ActiveSEs )[0]]
    else:
      archive1SE = []
    return self._simpleReplication( archive1SE, archive2ActiveSEs, numberOfCopies = numberOfCopies )

  def _simpleReplication( self, mandatorySEs, secondarySEs, numberOfCopies = 0, fromSEs = [] ):
    """ Actually creates the replication tasks for replication plugins
    """
    self.util.logInfo( "Starting execution of plugin" )
    mandatorySEs = uniqueElements( mandatorySEs )
    secondarySEs = [se for se in uniqueElements( secondarySEs ) if se not in mandatorySEs]
    if not numberOfCopies:
      numberOfCopies = len( secondarySEs ) + len( mandatorySEs )
      activeSecondarySEs = secondarySEs
    else:
      activeSecondarySEs = self.util.getActiveSEs( secondarySEs )
      numberOfCopies = max( len( mandatorySEs ), numberOfCopies )

    self.util.logVerbose( "%d replicas, mandatory at %s, optional at %s" % ( numberOfCopies,
                                                                             mandatorySEs, activeSecondarySEs ) )

    alreadyCompleted = []
    fileTargetSEs = {}
    for replicaSE, lfnGroup in getFileGroups( self.transReplicas ).items():
      existingSEs = [se for se in replicaSE.split( ',' ) if not isFailover( se )]
      # If a FromSEs parameter is given, only keep the files that are at one of those SEs, mark the others NotProcessed
      if fromSEs:
        if not [se for se in existingSEs if se in fromSEs]:
          res = self.transClient.setFileStatusForTransformation( self.transID, 'NotProcessed', lfnGroup )
          if not res['OK']:
            self.util.logError( 'Error setting files NotProcessed', res['Message'] )
          else:
            self.util.logVerbose( 'Found %d files that at not in %s, set NotProcessed' % ( len( lfnGroup ), fromSEs ) )
          continue

      # If there is no choice on the SEs, send all files at once, otherwise make chunks
      if numberOfCopies >= len( mandatorySEs ) + len( activeSecondarySEs ):
        lfnChunks = [lfnGroup]
      else:
        lfnChunks = breakListIntoChunks( lfnGroup, 100 )

      for lfns in lfnChunks:
        candidateSEs = closerSEs( existingSEs, secondarySEs )
        # Remove duplicated SEs (those that are indeed the same), but keep existing ones
        for se1 in [se for se in candidateSEs if se not in existingSEs]:
          if self.util.isSameSEInList( se1, [se for se in candidateSEs if se != se1] ):
            candidateSEs.remove( se1 )
        # Remove existing SEs from list of candidates
        ncand = len( candidateSEs )
        candidateSEs = [se for se in candidateSEs if se not in existingSEs]
        needToCopy = numberOfCopies - ( ncand - len( candidateSEs ) )
        stillMandatory = [se for se in mandatorySEs if se not in candidateSEs]
        candidateSEs = self.util.uniqueSEs( stillMandatory + [se for se in candidateSEs if se in activeSecondarySEs] )
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
          stringTargetSEs = ','.join( sorted( targetSEs ) )
          # Now assign the individual files to their targets
          ( chunkFileTargetSEs, completed ) = self.util.assignTargetToLfns( lfns, self.transReplicas, stringTargetSEs )
          alreadyCompleted += completed
          fileTargetSEs.update( chunkFileTargetSEs )

    # Update the status of the already done files
    if alreadyCompleted:
      self.util.logInfo( "Found %s files that are already completed" % len( alreadyCompleted ) )
      self.transClient.setFileStatusForTransformation( self.transID, 'Processed', alreadyCompleted )

    # Now group all of the files by their target SEs
    storageElementGroups = {}
    for lfn, stringTargetSEs in fileTargetSEs.items():
      storageElementGroups.setdefault( stringTargetSEs, [] ).append( lfn )

    return S_OK( self.util.createTasks( storageElementGroups ) )

  def _FakeReplication( self ):
    """ Creates replication tasks for to the existing SEs. Used only for tests!
    """
    storageElementGroups = {}
    for replicaSE, lfnGroup in getFileGroups( self.transReplicas ).items():
      existingSEs = replicaSE.split( ',' )
      for lfns in breakListIntoChunks( lfnGroup, 100 ):
        stringTargetSEs = existingSEs[0]
        storageElementGroups.setdefault( stringTargetSEs, [] ).extend( lfns )
    if self.pluginCallback:
      self.pluginCallback( self.transID, invalidateCache = True )
    return S_OK( self.util.createTasks( storageElementGroups ) )

  def _DestroyDataset( self ):
    """ Plugin setting all existing SEs as targets
    """
    res = self._removeReplicas( keepSEs = [], minKeep = 0 )
    if not res['OK'] or not res['Value']:
      return res
    tasks = res['Value']
    if self.util.getPluginParam( 'CleanTransformations', False ):
      lfns = []
      for task in tasks:
        lfns += task[1]
      # Check if some of these files are used by transformations
      selectDict = { 'LFN':lfns }
      res = self.transClient.getTransformationFiles( selectDict )
      if not res['OK']:
        self.util.logError( "Error getting transformation files for %d files" % len( lfns ), res['Message'] )
      else:
        processedFiles = []
        self.util.logVerbose( "Out of %d files, %d occurrences were found in transformations" % ( len( lfns ),
                                                                                                  len( res['Value'] ) ) )
        transDict = {}
        runList = []
        for fileDict in res['Value']:
          # Processed files are immutable, and don't kill yourself!
          if fileDict['TransformationID'] != self.transID:
            if fileDict['Status'] not in ( 'Processed', 'Removed' ):
              transDict.setdefault( fileDict['TransformationID'], [] ).append( fileDict['LFN'] )
              run = fileDict['RunNumber']
              if run not in runList:
                runList.append( run )
            else:
              processedFiles.append( fileDict['LFN'] )
        if runList:
          self.util.logVerbose( "Files to be set Removed in other transformations for runs %s" % str( sorted( runList ) ) )
        else:
          self.util.logVerbose( "No files to be set Removed in other transformations" )
        if processedFiles:
          self.util.logInfo( "%d files are being removed but were already Processed or Removed" % len( processedFiles ) )
        for trans, lfns in transDict.items():
          if self.transID > 0:
            # Do not actually take action for a fake transformation (dirac-test-plugin)
            res = self.transClient.setFileStatusForTransformation( trans, 'Removed', lfns )
            action = 'set'
          else:
            res = {'OK':True}
            action = 'to be set'
          if not res['OK']:
            self.util.logError( "Error setting %d files in transformation %d to status Removed" % ( len( lfns ),
                                                                                                    trans ),
                                res['Message'] )
          else:
            self.util.logInfo( "%d files %s as 'Removed' in transformation %d" % ( len( lfns ), action, trans ) )

    return S_OK( tasks )

  def _DeleteDataset( self ):
    """ Plugin used to remove disk replicas, keeping some (e.g. archives)
    """
    keepSEs = self.util.getPluginParam( 'KeepSEs', ['CERN-ARCHIVE', 'CNAF-ARCHIVE', 'GRIDKA-ARCHIVE',
                                                 'IN2P3-ARCHIVE', 'NIKHEF-ARCHIVE', 'SARA-ARCHIVE',
                                                 'PIC-ARCHIVE', 'RAL-ARCHIVE'] )
    return self._removeReplicas( keepSEs = keepSEs, minKeep = 0 )

  def _DeleteReplicas( self ):
    """ Plugin for removing replicas from specific SEs or reduce the number of replicas
    """
    fromSEs = self.util.getPluginParam( 'FromSEs', [] )
    keepSEs = self.util.getPluginParam( 'KeepSEs', ['CERN-ARCHIVE', 'CNAF-ARCHIVE', 'GRIDKA-ARCHIVE',
                                                    'IN2P3-ARCHIVE', 'NIKHEF-ARCHIVE', 'SARA-ARCHIVE',
                                                    'PIC-ARCHIVE', 'RAL-ARCHIVE'] )
    mandatorySEs = self.util.getPluginParam( 'MandatorySEs', ['CERN_MC_M-DST', 'CERN_M-DST', 'CERN-DST', 'CERN_MC-DST'] )
    # Allow removing explicitly from SEs in mandatorySEs
    mandatorySEs = [se for se in mandatorySEs if se not in fromSEs]
    # this is the number of replicas to be kept in addition to keepSEs and mandatorySEs
    minKeep = self.util.getPluginParam( 'NumberOfReplicas', 1 )

    return self._removeReplicas( fromSEs = fromSEs, keepSEs = keepSEs, mandatorySEs = mandatorySEs, minKeep = minKeep )

  def _removeReplicas( self, fromSEs = None, keepSEs = None, mandatorySEs = None, minKeep = 999 ):
    """ Utility acutally implementing the logic to remove replicas or files
    """
    if not fromSEs:
      fromSEs = []
    if not keepSEs:
      keepSEs = []
    if not mandatorySEs:
      mandatorySEs = []
    self.util.logInfo( "Starting execution of plugin" )
    reduceSEs = minKeep < 0
    minKeep = abs( minKeep )

    storageElementGroups = {}
    notInKeepSEs = []
    for replicaSE, lfns in getFileGroups( self.transReplicas ).items():
      replicaSE = replicaSE.split( ',' )
      if minKeep == 0 and keepSEs:
        # Check that the dataset exists at least at 1 keepSE
        if not [se for se in replicaSE if se in keepSEs]:
          notInKeepSEs.extend( lfns )
          continue
      existingSEs = [se for se in replicaSE if se not in keepSEs and not isFailover( se )]
      if minKeep == 0:
        # We only keep the replicas in keepSEs
        targetSEs = [se for se in existingSEs]
      else:
        targetSEs = []
        # Take into account the mandatory SEs
        existingSEs = [se for se in existingSEs if se not in mandatorySEs]
        self.util.logVerbose( "%d files, non-keep SEs: %s, removal from %s, keep %d" % ( len( lfns ), existingSEs,
                                                                                         fromSEs, minKeep ) )
        # print existingSEs, fromSEs, minKeep
        if len( existingSEs ) > minKeep:
          # explicit deletion
          if fromSEs and not reduceSEs:
            # check how  many replicas would be left if we remove from fromSEs
            nLeft = len( [se for se in existingSEs if se not in fromSEs] )
            # we can delete all replicas in fromSEs
            targetSEs = [se for se in fromSEs if se in existingSEs]
            self.util.logVerbose( "Target SEs, 1st level: %s, number of left replicas: %d" % ( targetSEs, nLeft ) )
            if nLeft < minKeep:
              # we should keep some in fromSEs, too bad
              targetSEs = randomize( targetSEs )[0:minKeep - nLeft]
              self.util.logInfo( "Found %d files that could only be deleted in %d of the requested SEs" % ( len( lfns ),
                                                                                                    minKeep - nLeft ) )
              self.util.logVerbose( "Target SEs, 2nd level: %s" % targetSEs )
          else:
            # Here the fromSEs are only a preference
            if fromSEs:
              targetSEs = [se for se in fromSEs if se in existingSEs] + randomize( [se for se in existingSEs if se not in fromSEs] )
            else:
              # remove all replicas and keep only minKeep
              targetSEs = randomize( existingSEs )
            targetSEs = targetSEs[0:-minKeep]
        elif [se for se in fromSEs if se in existingSEs]:
          nLeft = len( [se for se in existingSEs if se not in fromSEs] )
          self.util.logInfo( "Found %d files at requested SEs with not enough replicas (%d left, %d requested)" % ( len( lfns ), nLeft, minKeep ) )
          self.util.logVerbose( "First file at %s are: %s" % ( str( existingSEs ), lfns[0] ) )
          self.transClient.setFileStatusForTransformation( self.transID, 'Problematic', lfns )
          continue

      if targetSEs:
        stringTargetSEs = ','.join( sorted( targetSEs ) )
        storageElementGroups.setdefault( stringTargetSEs, [] ).extend( lfns )
        self.util.logVerbose( "%d files to be removed at %s" % ( len( lfns ), stringTargetSEs ) )
      else:
        self.util.logInfo( "Found %s files that don't need any replica deletion" % len( lfns ) )
        self.transClient.setFileStatusForTransformation( self.transID, 'Processed', lfns )

    if notInKeepSEs:
      self.util.logInfo( "Found %d files not in at least one keepSE, no removal done, set Problematic" % len( notInKeepSEs ) )
      self.transClient.setFileStatusForTransformation( self.transID, 'Problematic', notInKeepSEs )

    if self.pluginCallback:
      self.pluginCallback( self.transID, invalidateCache = True )
    return S_OK( self.util.createTasks( storageElementGroups ) )

  def _DeleteReplicasWhenProcessed( self ):
    """ This plugin considers files and checks whether they were processed for a list of processing passes
        For files that were processed, it sets replica removal tasks from a set of SEs
    """
    keepSEs = self.util.getPluginParam( 'KeepSEs', [] )
    listSEs = set( self.util.getPluginParam( 'FromSEs', [] ) ) - set( keepSEs )
    if not listSEs:
      self.util.logError( 'No SEs where to delete from, check overlap with %s' % keepSEs )
      return S_OK( [] )
    processingPasses = self.util.getPluginParam( 'ProcessingPasses', [] )
    period = self.util.getPluginParam( 'Period', 6 )
    cacheLifeTime = self.util.getPluginParam( 'CacheLifeTime', 24 )

    transStatus = self.params['Status']
    self.util.readCacheFile( self.workDirectory )
    onlyAtList = False

    if not processingPasses:
      self.util.logWarn( "No processing pass(es)" )
      return S_OK( [] )
    skip = False
    try:
      res = self.transClient.getBookkeepingQuery( self.transID )
      if not res['OK']:
        self.util.logError( "Failed to get BK query for transformation", res['Message'] )
        return S_OK( [] )
      bkQuery = BKQuery( res['Value'], visible = 'All' )
      self.util.logVerbose( "BKQuery: %s" % bkQuery )
      transProcPass = bkQuery.getProcessingPass()
      if not transProcPass:
        lfn = self.transReplicas.keys()[0]
        res = self.bkClient.getDirectoryMetadata( lfn )
        if not res['OK']:
          self.util.logError( "Error getting directory metadata", res['Message'] )
          return res
        transQuery = res['Value']['Successful'].get( lfn, [{}] )[0]
        for key in ( 'ConditionDescription', 'FileType', 'Production', 'EventType' ):
          transQuery.pop( key, None )
        bkQuery = BKQuery( transQuery, visible = 'All' )
        transProcPass = bkQuery.getProcessingPass()
        if not transProcPass:
          self.util.logError( 'Unable to find processing pass for transformation nor for %s' % lfn )
        self.util.logVerbose( "BKQuery reconstructed: %s" % bkQuery )
      processingPasses = set( [os.path.join( transProcPass, procPass ) if not transProcPass.endswith( procPass ) else transProcPass for procPass in processingPasses] )
      acceptMerge = transProcPass in processingPasses

      now = datetime.datetime.utcnow()
      cacheOK = False
      productions = self.util.getCachedProductions()
      activeProds = productions.get( 'Active', {} )
      archivedProds = productions.get( 'Archived', {} )
      if productions and ( now - productions['CacheTime'] ) < datetime.timedelta( hours = cacheLifeTime ):
        if 'Active' not in productions:
          cacheOK = False
        else:
          # If we haven't found productions for one of the processing passes, retry
          activeProds = productions['Active']
          archivedProds = productions['Archived']
          cacheOK = True
          for procPass in processingPasses:
            if not activeProds.get( procPass ) and not archivedProds.get( procPass ):
              cacheOK = False
              break
      if cacheOK:
        if transStatus != 'Flush' and ( now - productions['LastCall_%s' % self.transID] ) < datetime.timedelta( hours = period ):
          self.util.logInfo( "Skip this loop (less than %s hours since last call)" % period )
          skip = True
          return S_OK( [] )
      else:
        self.util.logVerbose( "Cache is being refreshed (lifetime %d hours)" % cacheLifeTime )
        bkQuery.setFileType( None )
        bkQuery.setEventType( None )
        for procPass in processingPasses:
          bkQuery.setProcessingPass( procPass )
          prods = bkQuery.getBKProductions()
          if not prods:
            self.util.logVerbose( "For procPass %s, found no productions, wait next time" % ( procPass ) )
            return S_OK( [] )
          activeProds[procPass] = []
          archivedProds[procPass] = []
          for prod in prods:
            res = self.transClient.getTransformation( prod )
            if not res['OK']:
              self.util.logError( "Error getting transformation %s" % prod, res['Message'] )
            elif ( acceptMerge and res['Value']['Type'] == 'Merge' ) or ( not acceptMerge and res['Value']['Type'] != 'Merge' ):
              status = res['Value']['Status']
              if status == 'Archived':
                archivedProds[procPass].append( int( prod ) )
              elif status != "Cleaned":
                activeProds[procPass].append( int( prod ) )
          self.util.logInfo( "For procPass %s, found productions: %s, archived %s"
                          % ( procPass, activeProds[procPass], archivedProds[procPass] ) )
        productions = {'Active':activeProds, 'Archived':archivedProds}
        productions['CacheTime'] = now

      productions['LastCall_%s' % self.transID] = now
      self.util.setCachedProductions( productions )
      replicaGroups = getFileGroups( self.data )
      self.util.logVerbose( "Using %d input files, in %d groups" % ( len( self.data ), len( replicaGroups ) ) )
      storageElementGroups = {}
      newGroups = {}
      for stringSEs, lfns in replicaGroups.items():
        replicaSEs = set( stringSEs.split( ',' ) )
        targetSEs = listSEs & replicaSEs
        if not targetSEs:
          self.util.logInfo( "%d files are not in required list (only at %s)" % ( len( lfns ), sorted( replicaSEs ) ) )
        elif not replicaSEs - listSEs :
          self.util.logInfo( "%d files are only in required list (only at %s), don't remove yet" % ( len( lfns ), sorted( replicaSEs ) ) )
          onlyAtList = True
        else:
          newGroups.setdefault( ','.join( list( targetSEs ) ), [] ).extend( lfns )
      # Restrict the query to the TS to the interesting productions
      prodList = [pr for prods in activeProds.values() for pr in prods ]
      self.util.logVerbose( "Using following list of productions: %s" % str( prodList ) )
      for stringTargetSEs, lfns in newGroups.items():
        res = self.transClient.getTransformationFiles( {'TransformationID': prodList, 'LFN': lfns, 'Status':['Processed', 'Problematic']} )
        if not res['OK']:
          self.util.logError( "Failed to get transformation files for %d files" % len( lfns ) )
          continue
        lfnsNotProcessed = {}
        for trDict in res['Value']:
          trans = int( trDict['TransformationID'] )
          lfn = trDict['LFN']
          lfnsNotProcessed.setdefault( lfn, list( processingPasses ) )
          for procPass in lfnsNotProcessed[lfn]:
            if trans in activeProds[procPass]:
              lfnsNotProcessed[lfn].remove( procPass )
              break
        for lfn in [lfn for lfn in lfns if lfnsNotProcessed.get( lfn, True )]:
          lfnsNotProcessed.setdefault( lfn, list( processingPasses ) )
        for procPass, prods in archivedProds.items():
          lfnsToCheck = [lfn for lfn in lfnsNotProcessed if procPass in lfnsNotProcessed[lfn]]
          for prod in sorted( prods ):
            if not lfnsToCheck:
              break
            res = self.bkClient.getFileDescendants( lfnsToCheck, production = prod, depth = 1 )
            if res['OK']:
              for lfn in [lfn for lfn in lfnsToCheck if lfn in res['Value']['Successful']]:
                lfnsNotProcessed[lfn].remove( procPass )
                lfnsToCheck.remove( lfn )

        lfnsProcessed = [lfn for lfn in lfnsNotProcessed if not lfnsNotProcessed[lfn]]
        lfnsNotProcessed = [lfn for lfn in lfns if lfnsNotProcessed.get( lfn, True )]
        # print lfnsProcessed, lfnsNotProcessed
        self.util.logInfo( "Found %d / %d files that are processed (/ not) at %s" % ( len( lfnsProcessed ),
                                                                                      len( lfnsNotProcessed ),
                                                                                      stringTargetSEs ) )
        if lfnsProcessed:
          storageElementGroups.setdefault( stringTargetSEs, [] ).extend( lfnsProcessed )
      if not storageElementGroups:
        return S_OK( [] )
    except Exception, e:
      self.util.logException( 'Exception while executing the plugin' )
      return S_ERROR( e )
    finally:
      self.util.writeCacheFile()
      if not skip and onlyAtList and self.pluginCallback:
        self.pluginCallback( self.transID, invalidateCache = True )
    return S_OK( self.util.createTasks( storageElementGroups ) )

  def _ReplicateToLocalSE( self ):
    """ Used for example to replicate from a buffer to a tape SE on the same site
    """
    destSEs = self.util.getPluginParam( 'DestinationSEs', [] )
    watermark = self.util.getPluginParam( 'MinFreeSpace', 30 )

    storageElementGroups = {}

    for replicaSE, lfns in getFileGroups( self.transReplicas ).items():
      replicaSE = [se for se in replicaSE.split( ',' ) if not isFailover( se ) and not isArchive( se )]
      if not replicaSE:
        continue
      if [se for se in replicaSE if se in destSEs]:
        self.util.logInfo( "Found %d files that are already present in the destination SEs (status set)" % len( lfns ) )
        res = self.transClient.setFileStatusForTransformation( self.transID, 'Processed', lfns )
        if not res['OK']:
          self.util.logError( "Can't set %d files of transformation %s to 'Processed: %s'" % ( len( lfns ),
                                                                                            str( self.transID ),
                                                                                            res['Message'] ) )
          return res
        continue
      targetSEs = [se for se in destSEs if se not in replicaSE]
      candidateSEs = closerSEs( replicaSE, targetSEs, local = True )
      if candidateSEs:
        freeSpace = self.util.getStorageFreeSpace( candidateSEs )
        shortSEs = [se for se in candidateSEs if freeSpace[se] < watermark]
        if shortSEs:
          self.util.logVerbose( "No enough space (%s TB) found at %s" % ( watermark, ','.join( shortSEs ) ) )
        candidateSEs = [se for se in candidateSEs if se not in shortSEs]
        if candidateSEs:
          storageElementGroups.setdefault( candidateSEs[0], [] ).extend( lfns )
      else:
        self.util.logWarn( "Could not find a close SE for %d files" % len( lfns ) )

    return S_OK( self.util.createTasks( storageElementGroups ) )

  def _Healing( self ):
    """ Plugin that creates task for replicating files to the same SE where they are declared problematic
    """
    self.__cleanFiles()
    storageElementGroups = {}

    for replicaSE, lfns in getFileGroups( self.transReplicas ).items():
      replicaSE = set( [se for se in replicaSE.split( ',' ) if not isFailover( se ) and not isArchive( se )] )
      if not replicaSE:
        self.util.logInfo( "Found %d files that don't have a suitable source replica. Set Problematic" % len( lfns ) )
        res = self.transClient.setFileStatusForTransformation( self.transID, 'Problematic', lfns )
        continue
      # get no problematic replicas only
      res = self.fc.getReplicas( lfns, allStatus = False )
      if not res['OK']:
        self.util.logError( 'Error getting catalog replicas', res['Message'] )
        continue
      replicas = res['Value']['Successful']
      noMissingSE = []
      noOtherReplica = []
      for lfn in lfns:
        if lfn not in replicas:
          # This file has no active replicas, problematic
          noOtherReplica.append( lfn )
        else:
          targetSEs = replicaSE - set( replicas[lfn] )
          if targetSEs:
            storageElementGroups.setdefault( ','.join( sorted( targetSEs ) ), [] ).append( lfn )
          else:
            # print lfn, sorted( replicas[lfn] ), sorted( replicaSE )
            noMissingSE.append( lfn )
      if noOtherReplica:
        self.util.logInfo( "Found %d files that have no other active replica (set Problematic)" % len( noOtherReplica ) )
        res = self.transClient.setFileStatusForTransformation( self.transID, 'Problematic', noOtherReplica )
        if not res['OK']:
          self.util.logError( "Can't set %d files of transformation %s to 'Problematic': %s" % ( len( noOtherReplica ),
                                                                                               str( self.transID ),
                                                                                               res['Message'] ) )
      if noMissingSE:
        self.util.logInfo( "Found %d files that are already present in the destination SEs (set Processed)" % len( noMissingSE ) )
        res = self.transClient.setFileStatusForTransformation( self.transID, 'Processed', noMissingSE )
        if not res['OK']:
          self.util.logError( "Can't set %d files of transformation %s to 'Processed': %s" % ( len( noMissingSE ),
                                                                                               str( self.transID ),
                                                                                               res['Message'] ) )
          return res


    return S_OK( self.util.createTasks( storageElementGroups ) )
