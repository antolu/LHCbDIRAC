"""  TransformationPlugin is a class wrapping the supported LHCb transformation plugins
"""

__RCSID__ = "$Id$"

import time, datetime, os

from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Utilities.List import breakListIntoChunks, randomize
from DIRAC.TransformationSystem.Agent.TransformationPlugin import TransformationPlugin as DIRACTransformationPlugin
from LHCbDIRAC.TransformationSystem.Client.Utilities \
     import PluginUtilities, getFileGroups, groupByRun, isArchive, isFailover, \
     sortExistingSEs, getRemovalPlugins, closerSEs

class TransformationPlugin( DIRACTransformationPlugin ):
  """ Extension of DIRAC TransformationPlugin - instantiated by the TransformationAgent
  """

  def __init__( self, plugin,
                transClient=None, replicaManager=None,
                bkkClient=None, rmClient=None, rss=None,
                debug=False ):
    """ The clients can be passed in.
    """
    DIRACTransformationPlugin.__init__( self, plugin, transClient=transClient, replicaManager=replicaManager )

    if not bkkClient:
      from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
      self.bkClient = BookkeepingClient()
    else:
      self.bkClient = bkkClient

    if not rmClient:
      from LHCbDIRAC.ResourceStatusSystem.Client.ResourceManagementClient import ResourceManagementClient
      self.rmClient = ResourceManagementClient()
    else:
      self.rmClient = rmClient

    if not rss:
      from DIRAC.ResourceStatusSystem.Client.ResourceStatus import ResourceStatus
      self.resourceStatus = ResourceStatus()
    else:
      self.resourceStatus = rss

    self.params = {}
    self.workDirectory = None
    self.pluginCallback = None
    self.startTime = time.time()
    self.transReplicas = {}
    self.transFiles = []
    self.transID = None
    self.util = PluginUtilities( plugin, self.transClient, self.rm, self.bkClient, self.rmClient, self.resourceStatus, debug )

  def setInputData( self, data ):
    """
    self.transReplicas are the replicas of the transformation files.
    However if some don't have a replica, it is not in this dictionary
    self.transReplicas[lfn] == { SE1:PFN1, SE2:PFN2...}
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

  def __cleanFiles( self, status=None ):
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

  def setDirectory( self, directory ):
    self.workDirectory = directory

  def setCallback( self, callback ):
    self.pluginCallback = callback

  def setDebug( self, val=True ):
    self.util.setDebug( val )

  def _removeProcessedFiles( self ):
    """
    Checks if the LFNs have descendants in the same transformation. Removes them from self.transReplicas
    and sets them 'Processed'
    """
    descendants = {}
    startTime = time.time()
    for lfns in breakListIntoChunks( self.transReplicas.keys(), 500 ):
      # WARNING: this is in principle not sufficient as one should check also whether descendants without replica
      #          may have themselves descendants with replicas
      res = self.bkClient.getFileDescendents( lfns, production=int( self.transID ), depth=1, checkreplica=True )
      if not res['OK']:
        self.util.logError( "Cannot get descendants of files:", res['Message'] )
      else:
        descendants.update( res['Value']['Successful'] )
    self.util.logVerbose( "Got descendants of %d files in %.3f seconds" % ( len( self.transReplicas ), time.time() - startTime ) )
    if descendants:
      processedLfns = [lfn for lfn in descendants if descendants[lfn]]
      if processedLfns:
        res = self.transClient.setFileStatusForTransformation( self.transID, 'Processed', processedLfns )
        if res['OK']:
          self.util.logInfo( "Found %d input files that have already been processed (status set)" % len( processedLfns ) )
          for lfn in processedLfns:
            if lfn in self.transReplicas:
              self.transReplicas.pop( lfn )
          for fileDict in [fileDict for fileDict in self.transFiles]:
            if fileDict['LFN'] in processedLfns:
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
    res = self.util.getShares( section='RAW' )
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
      self.__cleanFiles( status='Processed' )

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
        #Create the tasks
        tasks.append( ( assignedSE, runLfns ) )
    return S_OK( tasks )

  def _AtomicRun( self ):
    """
    Plugin for Reconstruction and reprocessing
    It uses the assigned shares per site and waits for files to be replicated
    """
    self.util.logInfo( "Starting execution of plugin" )
    delay = self.util.getPluginParam( 'RunDelay', 1 )
    self._removeProcessedFiles()
    # Get the requested shares from the CS
    backupSE = 'CERN-RAW'
    res = self.util.getCPUShares( self.transID, backupSE )
    if not res['OK']:
      return res
    else:
      ( rawFraction, cpuShares ) = res['Value']

    res = self.transClient.getTransformation( self.transID )
    if not res['OK']:
      self.util.logError( "Cannot get information on transformation" )
      return res
    else:
      transType = res['Value']['Type']

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
    if runFileDict.pop( 0, None ):
      self.util.logInfo( "Removing run #0, which means it was not set yet" )

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
          if datetime.datetime.now() - endDate < datetime.timedelta( hours=delay ):
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
        res = self.transClient.getTransformationFiles( condDict={'TransformationID':self.transID,
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
        for se in [se for se in self.transReplicas[lfn].keys() if se not in distinctSEs and se in activeRAWSEs]:
          if se not in distinctSEs:
            distinctSEs.append( se )
      if len( distinctSEs ) < 2:
        self.util.logInfo( "Not found two active candidate SEs for run %d, skipped" % runID )
        continue
      seProbs = {}
      prob = 0.
      if backupSE not in distinctSEs:
        self.util.logWarn( " %s not in the SEs for run %d" % ( backupSE, runID ) )
        backupSE = None
      distinctSEs = sorted( [se for se in distinctSEs if se in rawFraction and se != backupSE] )
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
      self.util.logInfo( "For run %d, SE integrated fraction =%s, random number = %.3f" % ( runID, strProbs, rand ) )
      for se in distinctSEs:
        if rand <= seProbs[se]:
          selectedSE = se
          break
      self.util.logVerbose( "Selected SE for reconstruction is %s", selectedSE )
      if selectedSE:
        runSEDict[runID] = selectedSE
        runUpdate[runID] = True
        self.util.logInfo( "Run %d (%d files) assigned to %s" % ( runID, len( runLfns ), selectedSE ) )

    # Create the tasks
    tasks = []
    for runID in sorted( runSEDict ):
      selectedSE = runSEDict[runID]
      self.util.logInfo( "Creating tasks for run %d, targetSE %s (%d files)" % ( runID, selectedSE,
                                                                              len( runFileDict[runID] ) ) )
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
      lfns = [lfn for lfn in runFileDict[runID] if len( self.transReplicas.get( lfn, [] ) ) >= 2]
      if not lfns:
        continue
      res = self._groupBySize( lfns )
      self.params['Status'] = status
      if res['OK']:
        for task in res['Value']:
          if selectedSE in task[0].split( ',' ):
            tasks.append( ( selectedSE, task[1] ) )
      else:
        self.util.logError( 'Error grouping files by size', res['Message'] )

    if self.pluginCallback:
      self.pluginCallback( self.transID, invalidateCache=True )
    return S_OK( tasks )


  def _groupBySize( self, files=None ):
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
        used for example for WG productions
    """
    return self._groupByReplicas()

  def _ByRun( self, param='', plugin='LHCbStandard', requireFlush=False ):
    """ Basic plugin for when you want to group files by run
    """
    self.util.logInfo( "Starting execution of plugin" )
    allTasks = []
    self._removeProcessedFiles()
    if not self.transReplicas:
      self.util.logVerbose( "No data to be processed by plugin" )
      return S_OK( allTasks )
    res = self.util.groupByRunAndParam( self.transReplicas, self.transFiles, param=param )
    if not res['OK']:
      self.util.logError( "Error when grouping %d files by run for param %s" % ( len( self.transReplicas ), param ) )
      return res
    runDict = res['Value']
    transStatus = self.params['Status']
    res = self.transClient.getTransformationRuns( {'TransformationID':self.transID, 'RunNumber':runDict.keys()} )
    if not res['OK']:
      self.util.logError( "Error when getting transformation runs for runs %s" % str( runDict.keys() ) )
      return res
    runSites = dict( [ ( r['RunNumber'], r['SelectedSite'] ) for r in res['Value'] if r['SelectedSite'] ] )
    # Loop on all runs that have new files
    inputData = self.transReplicas.copy()
    self.util.readCacheFile( self.workDirectory )
    runEvtType = {}
    for run in res['Value']:
      runID = run['RunNumber']
      runStatus = run['Status']
      if transStatus == 'Flush':
        runStatus = 'Flush'
      paramDict = runDict.get( runID, {} )
      targetSEs = [se for se in runSites.get( runID, '' ).split( ',' ) if se]
      for paramValue in sorted( paramDict.keys() ):
        if paramValue:
          paramStr = " (%s : %s) " % ( param, paramValue )
        else:
          paramStr = " "
        runParamLfns = paramDict[paramValue]
        # Check if something was new since last time...
        cachedLfns = self.util.getCachedRunLFNs( runID, paramValue )
        newLfns = [lfn for lfn in runParamLfns if lfn not in cachedLfns]
        if len( newLfns ) == 0 and self.transID > 0 and runStatus != 'Flush':
          self.util.logVerbose( "No new files since last time for run %d%s: skip..." % ( runID, paramStr ) )
          continue
        self.util.logVerbose( "Of %d files, %d are new for %d%s" % ( len( runParamLfns ),
                                                                  len( newLfns ), runID, paramStr ) )
        runFlush = requireFlush
        if runFlush:
          if not runEvtType.get( paramValue ):
            lfn = runParamLfns[0]
            res = self.util.getBookkeepingMetadata( [lfn] )
            if res['OK']:
              runEvtType[paramValue] = res['Value'][lfn].get( 'EventTypeId', 90000000 )
              self.util.logVerbose( 'Event type%s: %s' % ( paramStr, str( runEvtType[paramValue] ) ) )
            else:
              self.util.logWarn( "Can't determine event type for transformation%s, can't flush" % paramStr,
                              res['Message'] )
              runEvtType[paramValue] = None
          evtType = runEvtType[paramValue]
          if not evtType:
            runFlush = False
        runParamReplicas = {}
        for lfn in [lfn for lfn in runParamLfns if lfn in inputData]:
          runParamReplicas[lfn ] = {}
          for se in inputData[lfn]:
            if not isArchive( se ):
              runParamReplicas[lfn][se] = inputData[lfn][se]
        # We need to replace the input replicas by those of this run before calling the helper plugin
        # As it may use self.data, set both transReplicas and data members
        self.transReplicas = runParamReplicas
        self.data = self.transReplicas
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
                                                                                           runID,
                                                                                           paramStr,
                                                                                           len( runParamReplicas ) ) )
            status = 'Flush'
            self.transClient.setTransformationRunStatus( self.transID, runID, 'Flush' )
          else:
            self.util.logVerbose( "Only %d files (of %d) available for run %d" % ( ancestorRawFiles, rawFiles, runID ) )
        self.params['Status'] = status
        # Now calling the helper plugin
        res = eval( 'self._%s()' % plugin )
        self.params['Status'] = transStatus
        if not res['OK']:
          return res
        tasks = res['Value']
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
    return self._ByRun( requireFlush=True )

  def _ByRunBySize( self ):
    return self._ByRun( plugin='BySize' )

  def _ByRunBySizeWithFlush( self ):
    return self._ByRun( plugin='BySize', requireFlush=True )

  def _ByRunSize( self ):
    return self._ByRun( plugin='BySize' )

  def _MergeByRun( self ):
    return self._ByRunSize()

  def _ByRunSizeWithFlush( self ):
    return self._ByRun( plugin='BySize', requireFlush=True )

  def _MergeByRunWithFlush( self ):
    return self._ByRunSizeWithFlush()

  def _ByRunFileType( self ):
    return self._ByRun( param='FileType' )

  def _ByRunFileTypeWithFlush( self ):
    return self._ByRun( param='FileType', requireFlush=True )

  def _ByRunFileTypeSize( self ):
    return self._ByRun( param='FileType', plugin='BySize' )

  def _ByRunFileTypeSizeWithFlush( self ):
    return self._ByRun( param='FileType', plugin='BySize', requireFlush=True )

  def _ByRunEventType( self ):
    return self._ByRun( param='EventTypeId' )

  def _ByRunEventTypeWithFlush( self ):
    return self._ByRun( param='EventTypeId', requireFlush=True )

  def _ByRunEventTypeSize( self ):
    return self._ByRun( param='EventTypeId', plugin='BySize' )

  def _ByRunEventTypeSizeWithFlush( self ):
    return self._ByRun( param='EventTypeId', plugin='BySize', requireFlush=True )

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
    return self._lhcbBroadcast( archive1SEs, archive2SEs, mandatorySEs, secondarySEs, numberOfCopies )

  def _LHCbMCDSTBroadcast( self ):
    """ For replication of MC data (3 copies)
    """
    archive1SEs = self.util.getPluginParam( 'Archive1SEs', [] )
    archive2SEs = self.util.getPluginParam( 'Archive2SEs', ['CERN-ARCHIVE', 'CNAF-ARCHIVE', 'GRIDKA-ARCHIVE',
                                                         'IN2P3-ARCHIVE', 'SARA-ARCHIVE', 'PIC-ARCHIVE', 'RAL-ARCHIVE'] )
    mandatorySEs = self.util.getPluginParam( 'MandatorySEs', ['CERN_MC_M-DST'] )
    secondarySEs = self.util.getPluginParam( 'SecondarySEs', ['CNAF_MC-DST', 'GRIDKA_MC-DST', 'IN2P3_MC-DST',
                                                           'SARA_MC-DST', 'PIC_MC-DST', 'RAL_MC-DST'] )
    numberOfCopies = self.util.getPluginParam( 'NumberOfReplicas', 3 )
    return self._lhcbBroadcast( archive1SEs, archive2SEs, mandatorySEs, secondarySEs, numberOfCopies )

  def _lhcbBroadcast( self, archive1SEs, archive2SEs, mandatorySEs, secondarySEs, numberOfCopies ):
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
        res = self.transClient.getTransformationFiles( condDict={'TransformationID':self.transID,
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
          self.util.logVerbose( "Selecting SEs for %d files: %s" % ( len( runLfns ), str( runLfns ) ) )
          stringTargetSEs = self.util.setTargetSEs( numberOfCopies, archive1SEs, archive2SEs,
                                                 mandatorySEs, secondarySEs, existingSEs, exclusiveSEs=False )
          runUpdate[runID] = True

      # Update the TransformationRuns table with the assigned SEs (don't continue if it fails)
      if stringTargetSEs:
        if runUpdate[runID]:
          res = self.transClient.setTransformationRunsSite( self.transID, runID, stringTargetSEs )
          if not res['OK']:
            self.util.logError( "Failed to assign TransformationRun site", res['Message'] )
            return S_ERROR( "Failed to assign TransformationRun site" )

        #Now assign the individual files to their targets
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
                                                         'IN2P3-ARCHIVE', 'SARA-ARCHIVE', 'PIC-ARCHIVE', 'RAL-ARCHIVE'] )
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
                                               mandatorySEs, secondarySEs, existingSEs, exclusiveSEs=True )
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
    numberOfCopies = self.util.getPluginParam( 'NumberOfReplicas', 0 )
    return self._simpleReplication( destSEs, secondarySEs, numberOfCopies )

  def _ArchiveDataset( self ):
    """ Plugin for archiving datasets (normally 2 archives, unless one of the lists is empty)
    """
    archive1SEs = self.util.getPluginParam( 'Archive1SEs', [] )
    archive2SEs = self.util.getPluginParam( 'Archive2SEs', ['CERN-ARCHIVE', 'CNAF-ARCHIVE', 'GRIDKA-ARCHIVE',
                                                         'IN2P3-ARCHIVE', 'SARA-ARCHIVE', 'PIC-ARCHIVE', 'RAL-ARCHIVE'] )
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
    return self._simpleReplication( archive1SE, archive2ActiveSEs, numberOfCopies=numberOfCopies )

  def _simpleReplication( self, mandatorySEs, secondarySEs, numberOfCopies=0 ):
    """ Actually creates the replication tasks for replication plugins
    """
    self.util.logInfo( "Starting execution of plugin" )
    if not numberOfCopies:
      numberOfCopies = len( secondarySEs ) + len( mandatorySEs )
      activeSecondarySEs = secondarySEs
    else:
      activeSecondarySEs = self.util.getActiveSEs( secondarySEs )
      numberOfCopies = max( len( mandatorySEs ), numberOfCopies )

    alreadyCompleted = []
    fileTargetSEs = {}
    for replicaSE, lfnGroup in getFileGroups( self.transReplicas ).items():
      existingSEs = [se for se in replicaSE.split( ',' ) if not isFailover( se )]
      # If there is no choice on the SEs, send all files at once, otherwise make chunks
      if numberOfCopies >= len( mandatorySEs ) + len( activeSecondarySEs ):
        lfnChunks = [lfnGroup]
      else:
        lfnChunks = breakListIntoChunks( lfnGroup, 100 )

      for lfns in lfnChunks:
        candidateSEs = closerSEs( existingSEs, activeSecondarySEs )
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
          stringTargetSEs = ','.join( sorted( targetSEs ) )
          #Now assign the individual files to their targets
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
      self.pluginCallback( self.transID, invalidateCache=True )
    return S_OK( self.util.createTasks( storageElementGroups ) )

  def _DestroyDataset( self ):
    """ Plugin setting all existing SEs as targets
    """
    return self._removeReplicas( keepSEs=[], minKeep=0 )

  def _DeleteDataset( self ):
    """ Plugin used to remove disk replicas, keeping some (e.g. archives)
    """
    keepSEs = self.util.getPluginParam( 'KeepSEs', ['CERN-ARCHIVE', 'CNAF-ARCHIVE', 'GRIDKA-ARCHIVE',
                                                 'IN2P3-ARCHIVE', 'NIKHEF-ARCHIVE', 'SARA-ARCHIVE',
                                                 'PIC-ARCHIVE', 'RAL-ARCHIVE'] )
    return self._removeReplicas( keepSEs=keepSEs, minKeep=0 )

  def _DeleteReplicas( self ):
    """ Plugin for removing replicas from specific SEs or reduce the number of replicas
    """
    listSEs = self.util.getPluginParam( 'FromSEs', [] )
    keepSEs = self.util.getPluginParam( 'KeepSEs', ['CERN-ARCHIVE', 'CNAF-ARCHIVE', 'GRIDKA-ARCHIVE',
                                                 'IN2P3-ARCHIVE', 'NIKHEF-ARCHIVE', 'SARA-ARCHIVE',
                                                 'PIC-ARCHIVE', 'RAL-ARCHIVE'] )
    mandatorySEs = self.util.getPluginParam( 'MandatorySEs', ['CERN_MC_M-DST', 'CERN_M-DST', 'CERN-DST', 'CERN_MC-DST'] )
    # Allow removing explicitly from SEs in mandatorySEs
    mandatorySEs = [se for se in mandatorySEs if se not in listSEs]
    # this is the number of replicas to be kept in addition to keepSEs and mandatorySEs
    minKeep = self.util.getPluginParam( 'NumberOfReplicas', 1 )

    return self._removeReplicas( listSEs=listSEs, keepSEs=keepSEs, mandatorySEs=mandatorySEs, minKeep=minKeep )

  def _removeReplicas( self, listSEs=None, keepSEs=None, mandatorySEs=None, minKeep=999 ):
    """ Utility acutally implementing the logic to remove replicas or files
    """
    if not listSEs:
      listSEs = []
    if not keepSEs:
      keepSEs = []
    if not mandatorySEs:
      mandatorySEs = []
    self.util.logInfo( "Starting execution of plugin" )

    storageElementGroups = {}
    for replicaSE, lfns in getFileGroups( self.transReplicas ).items():
      replicaSE = replicaSE.split( ',' )
      if minKeep == 0 and keepSEs:
        # Check that the dataset exists at least at 1 keepSE
        if not [se for se in replicaSE if se in keepSEs]:
          self.util.logInfo( "Found %d files that are not in aat least one keepSEs, no removal done" % len( lfns ) )
          self.transClient.setFileStatusForTransformation( self.transID, 'Problematic', lfns )
          continue
      existingSEs = [se for se in replicaSE if se not in keepSEs and not isFailover( se )]
      if minKeep == 0:
        # We only keep the replicas in keepSEs
        targetSEs = [se for se in existingSEs]
      else:
        targetSEs = []
        # Take into account the mandatory SEs
        secondarySEs = [se for se in existingSEs if se in mandatorySEs]
        if secondarySEs:
          existingSEs = [se for se in existingSEs if se not in secondarySEs]
          toKeep = minKeep - len( secondarySEs )
        else:
          toKeep = minKeep
        if len( existingSEs ) > toKeep:
          # explicit deletion
          if listSEs:
            # check how  many replicas would be left if we remove from listSEs
            nLeft = len( [se for se in existingSEs if se not in listSEs] )
            # we can delete all replicas in listSEs
            targetSEs = [se for se in listSEs if se in existingSEs] + \
                        randomize( [se for se in existingSEs if se not in listSEs] )
            if nLeft < toKeep:
              # we should keep some in listSEs, too bad
              targetSEs = targetSEs[0:toKeep - nLeft]
              self.util.logInfo( "Found %d files that could only be deleted in %d of the requested SEs" % ( len( lfns ),
                                                                                                         toKeep - nLeft ) )
            else:
              targetSEs = targetSEs[0:-toKeep]
          else:
            # remove all replicas and keep only toKeep
            targetSEs = randomize( existingSEs )
            if toKeep:
              targetSEs = targetSEs[0:-toKeep]
        elif [se for se in listSEs if se in existingSEs]:
          nLeft = len( [se for se in existingSEs if not se in listSEs] )
          self.util.logInfo( "Found %d files at requested SEs with not enough replicas (%d left, %d requested)" % ( len( lfns ), nLeft, toKeep ) )
          self.util.logVerbose( "First files at %s are: %s" % ( str( existingSEs ), str( lfns[0:10] ) ) )
          self.transClient.setFileStatusForTransformation( self.transID, 'Problematic', lfns )
          continue

      if targetSEs:
        stringTargetSEs = ','.join( sorted( targetSEs ) )
        storageElementGroups.setdefault( stringTargetSEs, [] ).extend( lfns )
      else:
        self.util.logInfo( "Found %s files that don't need any replica deletion" % len( lfns ) )
        self.transClient.setFileStatusForTransformation( self.transID, 'Processed', lfns )

    return S_OK( self.util.createTasks( storageElementGroups ) )

  def _DeleteReplicasWhenProcessed( self ):
    """  This plugin considers files and checks whether they were processed for a list of processing passes
         For files that were processed, it sets replica removal tasks from a set of SEs
    """
    from LHCbDIRAC.BookkeepingSystem.Client.BKQuery import BKQuery

    listSEs = self.util.getPluginParam( 'FromSEs', [] )
    processingPasses = self.util.getPluginParam( 'ProcessingPasses', [] )
    period = self.util.getPluginParam( 'Period', 6 )
    cacheLifeTime = self.util.getPluginParam( 'CacheLifeTime', 24 )

    transStatus = self.params['Status']
    self.util.readCacheFile( self.workDirectory )

    if not listSEs:
      self.util.logInfo( "No SEs selected" )
      return S_OK( [] )
    if not processingPasses:
      self.util.logInfo( "No processing pass(es)" )
      return S_OK( [] )
    try:
      now = datetime.datetime.utcnow()
      cacheOK = False
      cachedProductions = self.util.getCachedProductions()
      if cachedProductions and ( now - cachedProductions['CacheTime'] ) < datetime.timedelta( hours=cacheLifeTime ):
        productions = cachedProductions
        # If we haven't found productions for one of the processing passes, retry
        cacheOK = True
        for procPass in processingPasses:
          if not productions.get( procPass ):
            cacheOK = False
            break
      if cacheOK:
        if transStatus != 'Flush' and ( now - cachedProductions['LastCall_%s' % self.transID] ) < datetime.timedelta( hours=period ):
          self.util.logInfo( "Skip this loop (less than %s hours since last call)" % period )
          return S_OK( [] )
      else:
        self.util.logVerbose( "Cache is being refreshed (lifetime %d hours)" % cacheLifeTime )
        productions = {}
        res = self.transClient.getBookkeepingQueryForTransformation( self.transID )
        if not res['OK']:
          self.util.logError( "Failed to get BK query for transformation", res['Message'] )
          return S_OK( [] )
        bkQuery = BKQuery( res['Value'] )
        self.util.logVerbose( "BKQuery: %s" % bkQuery )
        transProcPass = bkQuery.getProcessingPass()
        bkQuery.setFileType( None )
        for procPass in processingPasses:
          bkQuery.setProcessingPass( os.path.join( transProcPass, procPass ) )
          # Temporary work around for getting Stripping production from merging (parent should be set to False)
          bkQuery.setEventType( None )
          prods = bkQuery.getBKProductions( visible='ALL' )
          if not prods:
            self.util.logVerbose( "For procPass %s, found no productions, wait next time" % ( procPass ) )
            return S_OK( [] )
          self.util.logVerbose( "For procPass %s, found productions %s" % ( procPass, prods ) )
          productions[procPass] = [int( p ) for p in prods]
        cachedProductions = productions
        cachedProductions['CacheTime'] = now

      cachedProductions['LastCall_%s' % self.transID] = now
      self.util.setCachedProductions( cachedProductions )
      storageElementGroups = {}
      for replicaSEs, lfns in getFileGroups( self.transReplicas ).items():
        replicaSE = replicaSEs.split( ',' )
        targetSEs = [se for se in listSEs if se in replicaSE]
        if not targetSEs:
          self.util.logVerbose( "%s storage elements not in required list" % replicaSE )
          continue
        res = self.transClient.getTransformationFiles( {'LFN': lfns, 'Status':'Processed'} )
        if not res['OK']:
          self.util.logError( "Failed to get transformation files for %d files" % len( lfns ) )
          continue
        lfnsNotProcessed = {}
        for trDict in res['Value']:
          trans = int( trDict['TransformationID'] )
          lfn = trDict['LFN']
          lfnsNotProcessed.setdefault( lfn, list( processingPasses ) )
          for procPass in lfnsNotProcessed[lfn]:
            if trans in productions[procPass]:
              lfnsNotProcessed[lfn].remove( procPass )
              break
        lfnsProcessed = [lfn for lfn in lfnsNotProcessed if not lfnsNotProcessed[lfn]]
        lfnsNotProcessed = [lfn for lfn in lfns if lfnsNotProcessed.get( lfn, True )]
        #print lfnsProcessed, lfnsNotProcessed
        if lfnsNotProcessed:
          self.util.logVerbose( "Found %d files that are not fully processed at %s" % ( len( lfnsNotProcessed ), targetSEs ) )
        if lfnsProcessed:
          self.util.logVerbose( "Found %d files that are fully processed at %s" % ( len( lfnsProcessed ), targetSEs ) )
          stringTargetSEs = ','.join( sorted( targetSEs ) )
          storageElementGroups.setdefault( stringTargetSEs, [] ).extend( lfnsProcessed )
      if not storageElementGroups:
        return S_OK( [] )
    except:
      error = 'Exception while executing the plugin'
      self.util.logError( error )
      return S_ERROR( error )
    finally:
      self.util.writeCacheFile()
      if self.pluginCallback:
        self.pluginCallback( self.transID, invalidateCache=True )
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
      candidateSEs = closerSEs( replicaSE, targetSEs, local=True )
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
      replicaSE = [se for se in replicaSE.split( ',' ) if not isFailover( se ) and not isArchive( se )]
      if not replicaSE:
        self.util.logInfo( "Found %d files that don't have a suitable source replica. Set Problematic" % len( lfns ) )
        res = self.transClient.setFileStatusForTransformation( self.transID, 'Problematic', lfns )
        continue
      # get other replicas
      res = self.rm.getCatalogReplicas( lfns, allStatus=True )
      if not res['OK']:
        self.util.logError( 'Error getting catalog replicas', res['Message'] )
        continue
      replicas = res['Value']['Successful']
      noMissingSE = []
      for lfn in replicas:
        targetSEs = [se for se in replicas[lfn] if se not in replicaSE and not isFailover( se ) and not isArchive( se )]
        if targetSEs:
          storageElementGroups.setdefault( ','.join( targetSEs ), [] ).append( lfn )
        else:
          #print lfn, sorted( replicas[lfn] ), sorted( replicaSE )
          noMissingSE.append( lfn )
      if noMissingSE:
        self.util.logInfo( "Found %d files that are already present in the destination SEs (set Processed)" % len( noMissingSE ) )
        res = self.transClient.setFileStatusForTransformation( self.transID, 'Processed', noMissingSE )
        if not res['OK']:
          self.util.logError( "Can't set %d files of transformation %s to 'Processed: %s'" % ( len( noMissingSE ),
                                                                                            str( self.transID ),
                                                                                            res['Message'] ) )
          return res


    return S_OK( self.util.createTasks( storageElementGroups ) )
