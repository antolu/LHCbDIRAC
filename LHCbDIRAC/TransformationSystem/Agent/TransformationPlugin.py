"""  TransformationPlugin is a class wrapping the supported LHCb transformation plugins
"""
__RCSID__ = "$Id$"

import time
import datetime
import os
import random

from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Utilities.List import breakListIntoChunks, randomize
from DIRAC.Core.Utilities.Time import timeThis
from DIRAC.DataManagementSystem.Utilities.DMSHelpers import resolveSEGroup
from DIRAC.Resources.Catalog.FileCatalog import FileCatalog
from DIRAC.TransformationSystem.Agent.TransformationPlugin import TransformationPlugin as DIRACTransformationPlugin
from DIRAC.TransformationSystem.Client.Utilities import getFileGroups, sortExistingSEs

from LHCbDIRAC.BookkeepingSystem.Client.BKQuery import BKQuery, makeBKPath
from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
from LHCbDIRAC.ResourceStatusSystem.Client.ResourceManagementClient import ResourceManagementClient
from LHCbDIRAC.TransformationSystem.Client.TransformationClient import TransformationClient
from LHCbDIRAC.TransformationSystem.Utilities.PluginUtilities import PluginUtilities, groupByRun, getActiveSEs

class TransformationPlugin( DIRACTransformationPlugin ):
  """ Extension of DIRAC TransformationPlugin - instantiated by the TransformationAgent
  """

  def __init__( self, plugin,
                transClient = None, dataManager = None,
                bkkClient = None, rmClient = None, fc = None,
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

    if not fc:
      self.fc = FileCatalog()
    else:
      self.fc = fc

    self.params = {}
    self.workDirectory = None
    self.pluginCallback = self.voidMethod
    self.startTime = time.time()
    self.transReplicas = {}
    self.transFiles = []
    self.transID = None
    self.debug = debug
    if transClient is None:
      self.transClient = TransformationClient()
    else:
      self.transClient = transClient

    self.util = PluginUtilities( plugin = plugin,
                                 transClient = transClient, dataManager = dataManager,
                                 bkClient = self.bkClient, rmClient = self.rmClient,
                                 debug = debug, transInThread = transInThread if transInThread else {} )
    self.setDebug( self.util.getPluginParam( 'Debug', False ) )

    self.processingShares = ( None, None )

  def voidMethod( self, _id, invalidateCache = False ):
    return

  def setInputData( self, data ):
    """
    self.transReplicas are the replica location of the transformation files.
    However if some don't have a replica, they are not in this dictionary
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

  def __del__( self ):
    self.util.logInfo( "Execution finished, timing: %.3f seconds" % ( time.time() - self.startTime ) )

  @timeThis
  def _removeProcessedFiles( self ):
    """
    Checks if the LFNs have descendants in the same transformation. Removes them from self.transReplicas
    and sets them 'Processed'
    """
    self.util.logVerbose( 'Checking if %d files are processed' % len( self.transReplicas ) )
    descendants = self.util.getProcessedFiles( self.transReplicas.keys() )
    self.util.logVerbose( 'Successful check for %d files' % len( descendants ) )
    if descendants:
      processedLfns = [lfn for lfn in descendants if descendants[lfn]]
      self.util.logVerbose( "Found %d input files that have already been processed (setting status)" % len( processedLfns ) )
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
    # Specify no staging
    self.params['PrestageShares'] = ''
    return self._RAWReplication()

  def _RAWSharesOld( self ):
    """
    Plugin for replicating RAW data to Tier1s according to shares, excluding CERN which is the source of transfers...
    """
    self.util.logInfo( "Starting execution of plugin" )
    sourceSE = 'CERN-RAW'
    rawTargets = self.util.getPluginParam( 'RAWStorageElements', ['Tier1-RAW'] )
    rawTargets = set( resolveSEGroup( rawTargets ) ) - set( [sourceSE] )

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
      self.util.cleanFiles( self.transFiles, self.transReplicas, status = 'Processed' )

    # Group the remaining data by run
    res = groupByRun( self.transFiles )
    if not res['OK']:
      return res
    runFileDict = res['Value']
    if not runFileDict:
      # No files, no tasks!
      return S_OK( [] )

    # For each of the runs determine the destination of any previous files
    res = self.util.getTransformationRuns( runFileDict )
    if not res['OK']:
      self.util.logError( "Failed to obtain TransformationRuns", res['Message'] )
      return res
    runSEDict = dict( [( runDict['RunNumber'], runDict['SelectedSite'] ) for runDict in res['Value']] )

    # Choose the destination SE
    tasks = []
    for runID in set( runFileDict ) & set( runSEDict ):
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

      if assignedSE in rawTargets:
        # Update the TransformationRuns table with the assigned (if this fails do not create the tasks)
        if update:
          res = self.transClient.setTransformationRunsSite( self.transID, runID, assignedSE )
          if not res['OK']:
            self.util.logError( "Failed to assign TransformationRun SE", res['Message'] )
            return res
        # Create the tasks
        tasks.append( ( assignedSE, runLfns ) )
    return S_OK( tasks )

  def _RAWReplication( self ):
    """
    Plugin for replicating RAW data to Tier1s according to shares, and defining the processing destination site
    """
    self.util.logInfo( "Starting execution of plugin" )
    sourceSE = 'CERN-RAW'
    rawTargets = self.util.getPluginParam( 'RAWStorageElements', ['Tier1-RAW'] )
    rawTargets = set( resolveSEGroup( rawTargets ) ) - set( [sourceSE] )
    bufferTargets = self.util.getPluginParam( 'ProcessingStorageElements', ['Tier1-BUFFER'] )
    bufferTargets = set( resolveSEGroup( bufferTargets ) )
    useRunDestination = self.util.getPluginParam( 'UseRunDestination', False )
    preStageShares = self.util.getPluginParam( 'PrestageShares', 'CPUforRAW' )
    if preStageShares not in ( 'CPUforRAW', 'CPUforReprocessing' ) or not bufferTargets:
      self.util.logInfo( 'No prestaging required' )
      preStageShares = None
    self.util.logVerbose( 'Targets for replication are %s and %s' % ( sorted( rawTargets ), sorted( bufferTargets ) ) )
    if preStageShares:
      self.util.logInfo( "Using prestage shares from %s" % preStageShares )

    # Get the requested shares from the CS
    res = self.util.getShares( section = 'RAW' )
    if not res['OK']:
      self.util.logError( "Section RAW in Shares not available" )
      return res
    existingCount, targetShares = res['Value']

    # Split the files in run groups
    res = groupByRun( self.transFiles )
    if not res['OK']:
      self.util.logError( "Error splitting by run", res['Message'] )
      return res
    runFileDict = res['Value']
    if not runFileDict:
      # No files, no tasks!
      self.util.logVerbose( "No runs found!" )
      return S_OK( [] )
    self.util.logVerbose( 'Obtained %d runs' % len( runFileDict ) )

    zeroRun = runFileDict.pop( 0, None )
    if zeroRun:
      self.util.logInfo( "Setting run number for files with run #0, which means it was not set yet" )
      newRuns = self.util.setRunForFiles( zeroRun )
      for newRun, runLFNs in newRuns.items():
        runFileDict.setdefault( newRun, [] ).extend( runLFNs )
    # For each of the runs determine the destination of any previous files
    res = self.util.getTransformationRuns( runFileDict )
    if not res['OK']:
      self.util.logError( "Failed to obtain TransformationRuns", res['Message'] )
      return res
    runSEDict = dict( [( runDict['RunNumber'], runDict['SelectedSite'] ) for runDict in res['Value']] )

    # Choose the destination SE
    tasks = []
    alreadyReplicated = []
    for runID in set( runFileDict ) & set( runSEDict ):
      bufferLogged = False
      rawLogged = False
      runLfns = runFileDict[runID]
      assignedSE = runSEDict[runID]
      if assignedSE:
        # We already know where this run should go
        ses = set( assignedSE.split( ',' ) )
        assignedRAW = ses & rawTargets
        assignedBuffer = ses & bufferTargets
      else:
        assignedRAW = None
        assignedBuffer = None
      # Now determine where these files should go
      # Group by location
      replicaGroups = getFileGroups( dict( [( lfn, self.transReplicas[lfn] ) for lfn in runLfns] ) )
      runSEs = set()
      for replicaSE in replicaGroups:
        # Get all locations where files are
        ses = set( replicaSE.split( ',' ) )
        runSEs.update( ses )
      # Find potential locations
      if not assignedRAW:
        assignedRAW = runSEs & rawTargets
      if assignedRAW:
        assignedRAW = list( assignedRAW )[0]
      if not assignedBuffer and preStageShares:
        assignedBuffer = runSEs & bufferTargets
      if assignedBuffer:
        assignedBuffer = list( assignedBuffer )[0]
      updated = False
      for replicaSE, lfns in replicaGroups.items():
        replicaSE = set( replicaSE.split( ',' ) )
        if not assignedRAW:
          # Files are not yet at a Tier1-RAW
          if useRunDestination:
            assignedRAW = self.util.getSEForDestination( runID, rawTargets )
          if assignedRAW:
            self.util.logVerbose( 'RAW destination obtained from run %d destination: %s' % ( runID, assignedRAW ) )
          else:
            res = self._getNextSite( existingCount, targetShares )
            if not res['OK']:
              self.util.logError( "Failed to get next destination SE", res['Message'] )
              return res
            else:
              assignedRAW = res['Value']
              self.util.logVerbose( "RAW target assigned for run %d: %s" % ( runID, assignedRAW ) )
          rawLogged = True
        elif not rawLogged:
          self.util.logVerbose( 'RAW replica existing for run %d: %s' % ( runID, assignedRAW ) )

        # # Now get a buffer destination is prestaging is required
        if preStageShares and not assignedBuffer:
          if useRunDestination:
            assignedBuffer = self.util.getSEForDestination( runID, bufferTargets )
          if assignedBuffer:
            bufferLogged = True
            self.util.logVerbose( 'Buffer destination obtained from run %d destination: %s' % ( runID, assignedBuffer ) )
          else:
            # Files are not at a buffer for processing
            res = self._selectRunSite( runID, sourceSE, replicaSE | set( [assignedRAW] ), bufferTargets, preStageShares = preStageShares, useRunDestination = useRunDestination )
            if not res['OK']:
              self.util.logError( "Error selecting the destination site", res['Message'] )
              return res
            assignedBuffer = res['Value']
            if assignedBuffer:
              bufferLogged = True
              self.util.logVerbose( 'Selected destination SE for run %d: %s' % ( runID, assignedBuffer ) )
            else:
              self.util.logWarn( 'Failed to find destination SE for run', str( runID ) )
        elif assignedBuffer and not bufferLogged:
          self.util.logVerbose( 'Buffer destination existing for run %d: %s' % ( runID, assignedBuffer ) )

        # # Find out if the replication is necessary
        assignedSE = [assignedRAW, assignedBuffer] if assignedBuffer else [assignedRAW]
        if not updated:
          updated = True
          res = self.transClient.setTransformationRunsSite( self.transID, runID, ','.join( assignedSE ) )
          if not res['OK']:
            self.util.logError( "Failed to assign TransformationRun SE", res['Message'] )
            return res
        ses = sorted( set( assignedSE ) - replicaSE )
        # Update the counters as we know the number of files
        if assignedRAW in ses:
          existingCount[assignedRAW] = existingCount.setdefault( assignedRAW, 0 ) + len( lfns )
        assignedSE = ','.join( ses )
        if assignedSE:
          self.util.logVerbose( 'Creating a task for SEs %s' % assignedSE )
          tasks.append( ( assignedSE, lfns ) )
        else:
          alreadyReplicated += lfns
          self.util.logVerbose( '%d files found already replicated at %s' % ( len( lfns ), replicaSE ) )

    if alreadyReplicated:
      for lfn in alreadyReplicated:
        self.transReplicas.pop( lfn )
      self.util.cleanFiles( self.transFiles, self.transReplicas, status = 'Processed' )

    return S_OK( tasks )

  def _RAWProcessing( self ):
    """
    Create tasks for RAW data processing using the run destination table
    """
    fromSEs = set( resolveSEGroup( self.util.getPluginParam( 'FromSEs', [] ) ) )
    if not fromSEs:
      self.util.logWarn( 'No processing SEs are provided' )
      return S_OK( [] )

    # Split the files in run groups
    res = groupByRun( self.transFiles )
    if not res['OK']:
      self.util.logError( "Error splitting by run", res['Message'] )
      return res
    runFileDict = res['Value']
    if not runFileDict:
      # No files, no tasks!
      self.util.logVerbose( "No runs found!" )
      return S_OK( [] )
    self.util.logVerbose( 'Obtained %d runs' % len( runFileDict ) )

    zeroRun = runFileDict.pop( 0, None )
    if zeroRun:
      self.util.logInfo( "Setting run number for files with run #0, which means it was not set yet" )
      newRuns = self.util.setRunForFiles( zeroRun )
      for newRun, runLFNs in newRuns.items():
        runFileDict.setdefault( newRun, [] ).extend( runLFNs )
    # For each of the runs determine the destination of any previous files
    res = self.util.getTransformationRuns( runFileDict )
    if not res['OK']:
      self.util.logError( "Failed to obtain TransformationRuns", res['Message'] )
      return res
    runSEDict = dict( [( runDict['RunNumber'], runDict['SelectedSite'] ) for runDict in res['Value']] )

    # Choose the destination SE
    tasks = []
    for runID in set( runFileDict ) & set( runSEDict ):
      runLfns = runFileDict[runID]
      assignedSE = runSEDict[runID]
      runSEs = set( assignedSE.split( ',' ) ) if assignedSE and isinstance( assignedSE, basestring ) else set()
      # Now determine where these files should go
      # Group by location
      update = False
      replicaGroups = getFileGroups( dict( [( lfn, self.transReplicas[lfn] ) for lfn in runLfns] ) )
      notAtSE = 0
      for replicaSE, lfns in replicaGroups.items():
        targetSEs = set( replicaSE.split( ',' ) ) & fromSEs
        if targetSEs:
          # The files are at at least one of the requested SEs, set in run site for transformation
          #   and create task
          update = targetSEs - runSEs
          if update:
            runSEs |= targetSEs
          targetSEs = ','.join( sorted( targetSEs ) )
          self.util.logVerbose( 'Creating tasks with %d files for run %s at %s' % ( len( lfns ), runID, targetSEs ) )
          newTasks = self.util.createTasksBySize( lfns, targetSEs, flush = True )
          tasks += newTasks
          self.util.logVerbose( 'Created %d tasks' % len( newTasks ) )
        else:
          notAtSE += len( lfns )
      if notAtSE:
        self.util.logVerbose( 'Found %d files not yet at required SEs for run %d' % ( notAtSE, runID ) )
      # If there are new run site destination SEs, set them
      if update:
        res = self.transClient.setTransformationRunsSite( self.transID, runID, ','.join( sorted( runSEs ) ) )
        if not res['OK']:
          self.util.logError( "Failed to assign TransformationRun SE", res['Message'] )
          return res

    if self.pluginCallback:
      self.pluginCallback( self.transID, invalidateCache = True )
    return S_OK( tasks )

  def _selectRunSite( self, runID, backupSE, rawSEs, bufferTargets, preStageShares = None, useRunDestination = False ):
    if not preStageShares:
      return S_OK()

    if self.processingShares[0] is None:
      res = self.util.getShares( section = preStageShares, transID = self.transID, backupSE = backupSE )
      if not res['OK']:
        self.util.logError( "Error getting CPU shares for RAW processing", res['Message'] )
        return res
      else:
        self.processingShares = res['Value']

    self.util.logVerbose( "Select processing SE for run %d within %s" % ( runID, sorted( rawSEs ) ) )
    rawFraction, cpuShares = self.processingShares
    if len( rawSEs ) == 1:
      selectedSE = list( rawSEs )[0]
    else:
      existingSEs = set( cpuShares ) & rawSEs
      if not existingSEs:
        errStr = "Could not find shares for SEs"
        self.util.logError( errStr, sorted( rawSEs ) )
        return S_ERROR( errStr )

      prob = 0
      seProbs = {}
      rawSEs = sorted( ( rawSEs & set( rawFraction ) ) - set( [backupSE] ) )
      for se in rawSEs:
        prob += rawFraction[se] / len( rawSEs )
        seProbs[se] = prob
      rawSEs.append( backupSE )
      seProbs[backupSE] = 1.
      rand = random.uniform( 0., 1. )
      strProbs = ','.join( [' %s:%.3f' % ( se, seProbs[se] ) for se in rawSEs] )
      self.util.logInfo( "For run %d, SE integrated fraction =%s, random number = %.3f" % ( runID, strProbs, rand ) )
      selectedSE = None
      for se in rawSEs:
        prob = seProbs[se]
        if rand <= prob:
          selectedSE = se
          break

    # Find out the site associated to that SE
    res = self.util.dmsHelper.getLocalSiteForSE( selectedSE )
    if not res['OK']:
      return res
    site = res['Value']
    res = self.transClient.setDestinationForRun( runID, site )
    if not res['OK']:
      return res
    self.util.logVerbose( 'Successfully set destination for run %d: %s' % ( runID, site ) )

    return self.util.dmsHelper.getSEInGroupAtSite( bufferTargets, site )

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

    activeRAWSEs = getActiveSEs( cpuShares.keys() )
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
    res = self.util.getTransformationRuns( runFileDict )
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
        self.util.logVerbose( "Run %s: %d files are not considered (not %d replicas)" %
                              ( runID, len( notConsidered ), minNbReplicas ) )
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

  def _ByRun( self, param = '', plugin = 'LHCbStandard', requireFlush = False, forceFlush = False ):
    try:
      return self.__byRun( param = param, plugin = plugin, requireFlush = requireFlush, forceFlush = forceFlush )
    except Exception as x:
      self.util.logException( "Exception in _ByRun plugin:", '', x )
      return S_ERROR( [] )

  @timeThis
  def __byRun( self, param = '', plugin = 'LHCbStandard', requireFlush = False, forceFlush = False ):
    """ Basic plugin for when you want to group files by run
    """
    self.util.logInfo( "Starting execution of plugin" )
    # If flush is force, it is obviously required! try and get forcing from parameter
    if self.util.getPluginParam( 'ForceFlush', False ):
      forceFlush = True
    requireFlush |= forceFlush
    pluginStartTime = time.time()
    allTasks = []
    groupSize = self.util.getPluginParam( 'GroupSize' )
    typesWithNoCheck = self.util.getPluginParam( 'NoCheckTypes', ['Merge', 'MCMerge', 'Replication', 'Removal'] )
    fromSEs = set( resolveSEGroup( self.util.getPluginParam( 'FromSEs', [] ) ) )
    maxTime = self.util.getPluginParam( 'MaxTimeAllowed', 0 )
    self.util.readCacheFile( self.workDirectory )
    if not self.transReplicas:
      self.util.logVerbose( "No data to be processed by plugin" )
      return S_OK( allTasks )

    self.util.logInfo( "Grouping %d files by runs %s " %
                       ( len( self.transFiles ), 'and %s' % param if param else '' ) )
    res = self.util.groupByRunAndParam( self.transReplicas, self.transFiles, param = param )
    if not res['OK']:
      self.util.logError( "Error when grouping %d files by run for param %s" % ( len( self.transReplicas ), param ) )
      return res
    runDict = res['Value']

    # If necessary fix files with run number 0
    zeroRunDict = runDict.pop( 0, None )
    if zeroRunDict:
      nZero = 0
      for paramValue, zeroRun in zeroRunDict.items():
        newRuns = self.util.setRunForFiles( zeroRun )
        for newRun, runLFNs in newRuns.items():
          runDict.setdefault( newRun, {} ).setdefault( paramValue, [] ).extend( runLFNs )
          nZero += len( runLFNs )
      self.util.logInfo( "Set run number for %d files with run #0, which means it was not set yet" % nZero )

    transStatus = self.params['Status']
    res = self.util.getTransformationRuns( runDict )
    if not res['OK']:
      self.util.logError( "Error when getting transformation runs for runs %s" % str( runDict.keys() ) )
      return res
    transRuns = res['Value']
    runSites = dict( [ ( run['RunNumber'], run['SelectedSite'] ) for run in transRuns if run['SelectedSite'] ] )
    # Loop on all runs that have new files
    inputData = self.transReplicas.copy()
    setInputData = set( inputData )
    runEvtType = {}
    runList = []
    # Restart where we finished last time
    lastRun = self.util.getCachedLastRun()
    # runList = [run for run in transRuns if run['RunNumber'] > lastRun]
    # If none left, restart from the beginning
    if not runList:
      runList = transRuns
      lastRun = 0
      self.util.setCachedLastRun( lastRun )
    nRunsLeft = len( runList )
    missingAtSEs = False
    self.util.logInfo( "Processing %d runs starting at run %d" % ( len( runList ), lastRun ) )
    #
    # # # # # # # Loop on all selected runs # # # # # # #
    #
    timeout = False
    processedFiles = 0
    for run in sorted( runList, cmp = ( lambda d1, d2: int( d1['RunNumber'] - d2['RunNumber'] ) ) ):
      runID = run['RunNumber']
      self.util.logDebug( "Processing run %d, still %d runs left" % ( runID, nRunsLeft ) )
      nRunsLeft -= 1
      runStatus = 'Flush' if transStatus == 'Flush' else run['Status']
      paramDict = runDict.get( runID, {} )
      targetSEs = [se for se in runSites.get( runID, '' ).split( ',' ) if se]
      #
      # Loop on parameters (None if not by param
      #
      flushed = []
      for paramValue in sorted( paramDict ):
        paramStr = " (%s : %s) " % ( param, paramValue ) if paramValue else ' '
        runParamLfns = set( paramDict[paramValue] )
        processedFiles += len( runParamLfns )
        # Check if something was new since last time...
        cachedLfns = self.util.getCachedRunLFNs( runID, paramValue )
        newLfns = runParamLfns - cachedLfns
        if len( newLfns ) == 0 and \
           not forceFlush and \
           self.transID > 0 and \
           runStatus != 'Flush' and \
           not self.util.cacheExpired( runID ) and \
           ( plugin != 'LHCbStandard' or groupSize != 1 ):
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
              self.util.logWarn( "Can't determine event type for transformation%s, can't flush" %
                                 paramStr, res['Message'] )
              runEvtType[paramValue] = None
          evtType = runEvtType[paramValue]
          if not evtType:
            runFlush = False
        runParamReplicas = {}
        for lfn in runParamLfns & setInputData:
          runParamReplicas[lfn ] = [se for se in inputData[lfn]
                                    if self.util.dmsHelper.isSEForJobs( se, checkSE = False )]
        # We need to replace the input replicas by those of this run before calling the helper plugin
        # As it may use self.data, set both transReplicas and data members
        self.transReplicas = runParamReplicas
        # Check if files have already been processed
        if self.params['Type'] not in typesWithNoCheck:
          self._removeProcessedFiles()
        self.data = self.transReplicas
        status = runStatus
        if status != 'Flush' and runFlush:
          # If all files in that run have been processed and received, flush
          # Get the number of RAW files in that run
          if not forceFlush:
            rawFiles = self.util.getNbRAWInRun( runID, evtType )
            ancestorRawFiles = self.util.getRAWAncestorsForRun( runID, param, paramValue )
            self.util.logVerbose( "Obtained %d ancestor RAW files" % ancestorRawFiles )
            runProcessed = ( ancestorRawFiles == rawFiles )
          else:
            runProcessed = False
          if forceFlush or runProcessed:
            if runProcessed:
              # The whole run was processed by the parent production and we received all files
              self.util.logInfo( "All RAW files (%d) ready for run %d%s- Flushing run" %
                                 ( rawFiles, runID, paramStr ) )
            status = 'Flush'
            runStatus = status
            self.transClient.setTransformationRunStatus( self.transID, runID, 'Flush' )
          elif rawFiles:
            self.util.logVerbose( "Only %d ancestor RAW files (of %d) available for run %d" %
                                  ( ancestorRawFiles, rawFiles, runID ) )
          else:
            self.util.logVerbose( "Run %d is not finished yet" % runID )
        if runStatus == 'Flush':
          flushed.append( ( paramValue, len( self.data ) ) )
        # Now calling the helper plugin... Set status to a fake value
        self.params['Status'] = status
        res = eval( 'self._%s()' % plugin )
        # Resetting status
        self.params['Status'] = transStatus
        if not res['OK']:
          return res
        tasks = res['Value']
        if fromSEs:
          nTasks = len( tasks )
          for task in list( tasks ):
            # If fromSEs is defined, check if in the list
            if not fromSEs & set( task[0].split( ',' ) ):
              tasks.remove( task )
          if len( tasks ) != nTasks:
            missingAtSEs = True
            self.util.logInfo( "%d tasks could not be created for run %d as files are not at required SEs" %
                               ( nTasks - len( tasks ), runID ) )
        self.util.logVerbose( "Created %d tasks for run %d%s" %
                              ( len( tasks ), runID, paramStr ) )
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
            self.util.logError( "Failed to set target SEs to run %s as %s" %
                                ( str( runID ), str( targetSEs ) ), res['Message'] )
        self.util.setCachedRunLfns( runID, paramValue, [lfn for lfn in runParamLfns if lfn not in taskLfns] )
      # # # # # # # # # # #
      # End of param loop #
      # # # # # # # # # # #
      if flushed:
        # Print out info about what was flushed
        prStr = "Run %d is flushed%s:" % ( runID, " for %s" % param if param else "" )
        prStr += ','.join( [ " %s (%d)" % flTuple if flTuple[0] else " %d files" % flTuple[1] for flTuple in sorted( flushed )] )
        self.util.logInfo( prStr )

      # if enough time already spent, exit
      timeSpent = time.time() - pluginStartTime
      lastRun = runID
      if maxTime and timeSpent > maxTime:
        timeout = True
        self.util.logInfo( "Enough time spent in plugin (%.1f seconds), exit at run %d, %d runs left" %
                           ( timeSpent, runID, nRunsLeft ) )
        break
    # # # # # # # # # #
    # End of run loop #
    # # # # # # # # # #
    timeSpent = time.time() - pluginStartTime
    self.util.logInfo( "Processed %d files in %.1f seconds" % ( processedFiles, timeSpent ) )
    self.util.setCachedLastRun( lastRun )
    # reset the input data as it was when calling the plugin
    self.setInputData( inputData )
    self.util.writeCacheFile()
    if missingAtSEs and self.pluginCallback:
      # If some files could not be scheduled, clear the cache
      self.pluginCallback( self.transID, invalidateCache = True )
    ret = S_OK( allTasks )
    ret['Timeout'] = timeout
    return ret

  def _ByRunWithFlush( self ):
    # If groupSize is 1, no need to flush!
    groupSize = self.util.getPluginParam( 'GroupSize' )
    return self._ByRun( requireFlush = groupSize != 1 )

  def _ByRunForceFlush( self ):
    # If groupSize is 1, no need to flush!
    groupSize = self.util.getPluginParam( 'GroupSize' )
    return self._ByRun( forceFlush = groupSize != 1 )

  def _ByRunSize( self ):
    return self._ByRun( plugin = 'BySize' )

  def _ByRunSizeWithFlush( self ):
    return self._ByRun( plugin = 'BySize', requireFlush = True )

  def _ByRunSizeForceFlush( self ):
    return self._ByRun( plugin = 'BySize', forceFlush = True )

  def _ByRunFileType( self ):
    return self._ByRun( param = 'FileType' )

  def _ByRunFileTypeWithFlush( self ):
    # If groupSize is 1, no need to flush!
    groupSize = self.util.getPluginParam( 'GroupSize' )
    return self._ByRun( param = 'FileType', requireFlush = groupSize != 1 )

  def _ByRunFileTypeForceFlush( self ):
    # If groupSize is 1, no need to flush!
    groupSize = self.util.getPluginParam( 'GroupSize' )
    return self._ByRun( param = 'FileType', forceFlush = groupSize != 1 )

  def _ByRunFileTypeSize( self ):
    return self._ByRun( param = 'FileType', plugin = 'BySize' )

  def _ByRunFileTypeSizeWithFlush( self ):
    return self._ByRun( param = 'FileType', plugin = 'BySize', requireFlush = True )

  def _ByRunFileTypeSizeForceFlush( self ):
    return self._ByRun( param = 'FileType', plugin = 'BySize', forceFlush = True )

  def _ByRunEventType( self ):
    return self._ByRun( param = 'EventTypeId' )

  def _ByRunEventTypeWithFlush( self ):
    # If groupSize is 1, no need to flush!
    groupSize = self.util.getPluginParam( 'GroupSize' )
    return self._ByRun( param = 'EventTypeId', requireFlush = groupSize != 1 )

  def _ByRunEventTypeForceFlush( self ):
    # If groupSize is 1, no need to flush!
    groupSize = self.util.getPluginParam( 'GroupSize' )
    return self._ByRun( param = 'EventTypeId', forceFlush = groupSize != 1 )

  def _ByRunEventTypeSize( self ):
    return self._ByRun( param = 'EventTypeId', plugin = 'BySize' )

  def _ByRunEventTypeSizeWithFlush( self ):
    return self._ByRun( param = 'EventTypeId', plugin = 'BySize', requireFlush = True )

  def _ByRunEventTypeSizeForceFlush( self ):
    return self._ByRun( param = 'EventTypeId', plugin = 'BySize', forceFlush = True )

  def _LHCbDSTBroadcast( self ):
    """ Usually for replication of real data (4 copies)
    """
    archive1SEs = resolveSEGroup( self.util.getPluginParam( 'Archive1SEs', [] ) )
    archive2SEs = resolveSEGroup( self.util.getPluginParam( 'Archive2SEs', [] ) )
    mandatorySEs = resolveSEGroup( self.util.getPluginParam( 'MandatorySEs', [] ) )
    secondarySEs = resolveSEGroup( self.util.getPluginParam( 'SecondarySEs', [] ) )
    numberOfCopies = self.util.getPluginParam( 'NumberOfReplicas', 4 )
    return self._lhcbBroadcast( archive1SEs, archive2SEs, mandatorySEs, secondarySEs, numberOfCopies, forceRun = True )

  def _LHCbMCDSTBroadcast( self ):
    """ For replication of MC data (3 copies)
    """
    archive1SEs = resolveSEGroup( self.util.getPluginParam( 'Archive1SEs', [] ) )
    archive2SEs = resolveSEGroup( self.util.getPluginParam( 'Archive2SEs', [] ) )
    mandatorySEs = resolveSEGroup( self.util.getPluginParam( 'MandatorySEs', [] ) )
    secondarySEs = resolveSEGroup( self.util.getPluginParam( 'SecondarySEs', [] ) )
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
    res = self.util.getTransformationRuns( runFileDict )
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
    archive1SEs = resolveSEGroup( self.util.getPluginParam( 'Archive1SEs', [] ) )
    archive2SEs = resolveSEGroup( self.util.getPluginParam( 'Archive2SEs', [] ) )
    mandatorySEs = resolveSEGroup( self.util.getPluginParam( 'MandatorySEs', [] ) )
    secondarySEs = resolveSEGroup( self.util.getPluginParam( 'SecondarySEs', [] ) )
    numberOfCopies = self.util.getPluginParam( 'NumberOfReplicas', 3 )

    # We need at least all mandatory copies
    numberOfCopies = max( numberOfCopies, len( mandatorySEs ) )

    storageElementGroups = {}
    for replicaSE, lfnGroup in getFileGroups( self.transReplicas ).items():
      existingSEs = [se for se in replicaSE.split( ',' ) if not self.util.dmsHelper.isSEFailover( se )]
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
    destSEs = resolveSEGroup( self.util.getPluginParam( 'DestinationSEs', [] ) )
    if not destSEs:
      destSEs = resolveSEGroup( self.util.getPluginParam( 'MandatorySEs', [] ) )
    secondarySEs = resolveSEGroup( self.util.getPluginParam( 'SecondarySEs', [] ) )
    fromSEs = resolveSEGroup( self.util.getPluginParam( 'FromSEs', [] ) )
    numberOfCopies = self.util.getPluginParam( 'NumberOfReplicas', 0 )
    return self._simpleReplication( destSEs, secondarySEs, numberOfCopies, fromSEs = fromSEs )

  def _ArchiveDataset( self ):
    """ Plugin for archiving datasets (normally 2 archives, unless one of the lists is empty)
    """
    archive1SEs = resolveSEGroup( self.util.getPluginParam( 'Archive1SEs', [] ) )
    archive2SEs = resolveSEGroup( self.util.getPluginParam( 'Archive2SEs', [] ) )
    archive1ActiveSEs = getActiveSEs( archive1SEs )
    numberOfCopies = self.util.getPluginParam( 'NumberOfReplicas', 1 )
    if not archive1ActiveSEs:
      archive1ActiveSEs = archive1SEs
    archive2ActiveSEs = getActiveSEs( archive2SEs )
    if not archive2ActiveSEs:
      archive2ActiveSEs = archive2SEs
    if archive1ActiveSEs:
      archive1SE = [randomize( archive1ActiveSEs )[0]]
    else:
      archive1SE = []
    return self._simpleReplication( archive1SE, archive2ActiveSEs, numberOfCopies = numberOfCopies )

  def _simpleReplication( self, mandatorySEs, secondarySEs, numberOfCopies = 0, fromSEs = None ):
    """ Actually creates the replication tasks for replication plugins
    """
    self.util.logInfo( "Starting execution of plugin" )
    mandatorySEs = set( mandatorySEs )
    secondarySEs = set( secondarySEs ) - mandatorySEs
    if not numberOfCopies:
      numberOfCopies = len( secondarySEs ) + len( mandatorySEs )
      activeSecondarySEs = secondarySEs
    else:
      activeSecondarySEs = getActiveSEs( secondarySEs )
      numberOfCopies = max( len( mandatorySEs ), numberOfCopies )

    self.util.logVerbose( "%d replicas, mandatory at %s, optional at %s" % ( numberOfCopies,
                                                                             mandatorySEs, activeSecondarySEs ) )

    alreadyCompleted = []
    fileTargetSEs = {}
    for replicaSE, lfnGroup in getFileGroups( self.transReplicas ).items():
      existingSEs = [se for se in replicaSE.split( ',' ) if not self.util.dmsHelper.isSEFailover( se )]
      # If a FromSEs parameter is given, only keep the files that are at one of those SEs, mark the others NotProcessed
      if fromSEs:
        if not isinstance( fromSEs, list ):
          return S_ERROR( "fromSEs parameter should be a list" )
        if not [se for se in existingSEs if se in fromSEs]:
          res = self.transClient.setFileStatusForTransformation( self.transID, 'NotProcessed', lfnGroup )
          if not res['OK']:
            self.util.logError( 'Error setting files NotProcessed', res['Message'] )
          else:
            self.util.logVerbose( 'Found %d files that are not in %s, set NotProcessed' % ( len( lfnGroup ), fromSEs ) )
          continue

      # If there is no choice on the SEs, send all files at once, otherwise make chunks
      if numberOfCopies >= len( mandatorySEs ) + len( activeSecondarySEs ):
        lfnChunks = [lfnGroup]
      else:
        lfnChunks = breakListIntoChunks( lfnGroup, 100 )

      for lfns in lfnChunks:
        candidateSEs = self.util.closerSEs( existingSEs, secondarySEs )
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
    return self._RemoveDataset()
  def _RemoveDataset( self ):
    """ Plugin used to remove disk replicas, keeping some (e.g. archives)
    """
    keepSEs = resolveSEGroup( self.util.getPluginParam( 'KeepSEs', ['Tier1-ARCHIVE'] ) )
    return self._removeReplicas( keepSEs = keepSEs, minKeep = 0 )

  def _DeleteReplicas( self ):
    return self._RemoveReplicas()
  def _RemoveReplicas( self ):
    """ Plugin for removing replicas from specific SEs specified in FromSEs
    """
    fromSEs = resolveSEGroup( self.util.getPluginParam( 'FromSEs', [] ) )
    keepSEs = resolveSEGroup( self.util.getPluginParam( 'KeepSEs', ['Tier1-ARCHIVE'] ) )
    mandatorySEs = resolveSEGroup( self.util.getPluginParam( 'MandatorySEs', [] ) )
    # Allow removing explicitly from SEs in mandatorySEs
    mandatorySEs = [se for se in mandatorySEs if se not in fromSEs]
    # this is the number of replicas to be kept in addition to keepSEs and mandatorySEs
    minKeep = self.util.getPluginParam( 'NumberOfReplicas', 1 )

    return self._removeReplicas( fromSEs = fromSEs, keepSEs = keepSEs, mandatorySEs = mandatorySEs, minKeep = minKeep )

  def _ReduceReplicas( self ):
    """ Plugin for reducing the number of replicas to NumberOfReplicas
    """
    #
    fromSEs = resolveSEGroup( self.util.getPluginParam( 'FromSEs', [] ) )
    keepSEs = resolveSEGroup( self.util.getPluginParam( 'KeepSEs', ['Tier1-ARCHIVE'] ) )
    mandatorySEs = resolveSEGroup( self.util.getPluginParam( 'MandatorySEs', [] ) )
    # Allow removing explicitly from SEs in mandatorySEs
    mandatorySEs = [se for se in mandatorySEs if se not in fromSEs]
    # this is the number of replicas to be kept in addition to keepSEs and mandatorySEs
    minKeep = -abs( self.util.getPluginParam( 'NumberOfReplicas', 1 ) )

    return self._removeReplicas( fromSEs = fromSEs, keepSEs = keepSEs, mandatorySEs = mandatorySEs, minKeep = minKeep )

  def _removeReplicas( self, fromSEs = None, keepSEs = None, mandatorySEs = None, minKeep = 999 ):
    """ Utility actually implementing the logic to remove replicas or files
    """
    if fromSEs is None:
      fromSEs = []
    if keepSEs is None:
      keepSEs = []
    if mandatorySEs is None:
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
      existingSEs = [se for se in replicaSE if se not in keepSEs and not self.util.dmsHelper.isSEFailover( se )]
      if minKeep == 0:
        # We only keep the replicas in keepSEs
        targetSEs = sorted( existingSEs )
      else:
        targetSEs = []
        # Take into account the mandatory SEs
        existingSEs = [se for se in existingSEs if se not in mandatorySEs]
        self.util.logVerbose( "%d files, non-keep SEs: %s, removal from %s, keep %d" % ( len( lfns ), existingSEs,
                                                                                         fromSEs, minKeep ) )
        # print existingSEs, fromSEs, minKeep
        fromSet = set( fromSEs )
        existingSet = set( existingSEs )
        if len( existingSEs ) > minKeep:
          # explicit deletion
          if fromSEs and not reduceSEs:
            # check how  many replicas would be left if we remove all from fromSEs
            nLeft = len( existingSet - fromSet )
            # we can delete all replicas in fromSEs
            targetSEs = list( existingSet & fromSet )
            self.util.logVerbose( "Target SEs, 1st level: %s, number of left replicas: %d" % ( targetSEs, nLeft ) )
            if nLeft < minKeep:
              # we should keep some in fromSEs, too bad
              targetSEs = randomize( targetSEs )[0:minKeep - nLeft]
              self.util.logInfo( "Found %d files that could only be deleted in %d of the requested SEs" %
                                 ( len( lfns ), minKeep - nLeft ) )
              self.util.logVerbose( "Target SEs, 2nd level: %s" % targetSEs )
          elif fromSEs:
            # Here the fromSEs are only a preference (we want to keep only exactly minKeep replicas)
            targetSEs = list( existingSet & fromSet ) + randomize( list( existingSet - fromSet ) )
            targetSEs = targetSEs[0:-minKeep]
          else:
            # remove all replicas and keep only minKeep
            targetSEs = randomize( existingSEs )[0:-minKeep]
          targetSEs.sort()
          self.util.logVerbose( "Remove %d replicas from %s" % ( len( lfns ), ','.join( targetSEs ) ) )
        elif existingSet & fromSet:
          nLeft = len( existingSet - fromSet )
          self.util.logInfo( "Found %d files at %s with not enough replicas (%d left, %d requested), set Problematic" %
                             ( len( lfns ), ','.join( existingSEs ), nLeft, minKeep ) )
          self.transClient.setFileStatusForTransformation( self.transID, 'Problematic', lfns )
          continue

      if targetSEs:
        stringTargetSEs = ','.join( targetSEs )
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
    return self._RemoveReplicasWhenProcessed()
  def _RemoveReplicasWhenProcessed( self ):
    """ This plugin considers files and checks whether they were processed for a list of processing passes
        For files that were processed, it sets replica removal tasks from a set of SEs
    """
    keepSEs = resolveSEGroup( self.util.getPluginParam( 'KeepSEs', [] ) )
    fromSEs = set( resolveSEGroup( self.util.getPluginParam( 'FromSEs', [] ) ) ) - set( keepSEs )
    if not fromSEs:
      self.util.logError( 'No SEs where to delete from, check overlap with %s' % keepSEs )
      return S_OK( [] )
    processingPasses = self.util.getPluginParam( 'ProcessingPasses', [] )

    transStatus = self.params['Status']
    self.util.readCacheFile( self.workDirectory )
    onlyAtList = False

    if not processingPasses:
      self.util.logWarn( "No processing pass(es)" )
      return S_OK( [] )
    skip = False

    transQuery = self.util.getTransQuery( self.transReplicas )
    if transQuery is None:
      return S_ERROR( "Could not get transformation BK query" )
    transProcPass = transQuery['ProcessingPass']
    eventType = transQuery['EventType']
    if not transProcPass:
      self.util.logError( 'Unable to find processing pass for transformation' )
      return S_ERROR( 'No processing pass found' )

    fullBKPaths = [bkPath for bkPath in processingPasses if bkPath.startswith( '/' )]
    relBKPaths = [bkPath for bkPath in processingPasses if not bkPath.startswith( '/' )]
    bkPathList = {}
    if fullBKPaths:
      # We were given full BK paths, use them
      bkPathList.update( dict( [( bkPath.replace( 'RealData', 'Real Data' ), BKQuery( bkPath, visible = 'All' ).getQueryDict()['ProcessingPass'] ) \
                                for bkPath in fullBKPaths] ) )

    if relBKPaths:
      for procPass in relBKPaths:
        if not transProcPass.endswith( procPass ):
          newPass = os.path.join( transProcPass, procPass )
        else:
          newPass = procPass
        transQuery.update( {'ProcessingPass':newPass, 'Visibility':'All'} )
        transQuery.pop( 'FileType', None )
        bkPathList[ makeBKPath( transQuery ) ] = newPass
    self.util.logVerbose( 'List of BK paths:', '\n\t'.join( [''] + ['%s: %s' % \
                                                                    ( bkPath.replace( 'Real Data', 'RealData' ), bkPathList[bkPath].replace( 'Real Data', 'RealData' ) ) \
                                                                    for bkPath in sorted( bkPathList )] ) )
    # Now we must find out whether the input files have a descendant in these BK paths

    try:
      productions = self.util.getProductions( bkPathList, eventType, transStatus )
      if productions is None or not productions.get( 'List' ):
        return S_OK( [] )

      replicaGroups = getFileGroups( self.data )
      self.util.logVerbose( "Using %d input files, in %d groups" % ( len( self.data ), len( replicaGroups ) ) )
      storageElementGroups = {}
      newGroups = {}
      for stringSEs, lfns in replicaGroups.items():
        replicaSEs = set( stringSEs.split( ',' ) )
        targetSEs = fromSEs & replicaSEs
        if not targetSEs:
          # This is a fake to have a placeholder for the replica location... Later it is not used
          self.util.logVerbose( "%d files are not in required list (only at %s)" % ( len( lfns ), sorted( replicaSEs ) ) )
          newGroups.setdefault( ','.join( list( replicaSEs ) ), [] ).extend( lfns )
        elif not replicaSEs - fromSEs :
          self.util.logInfo( "%d files are only in required list (only at %s), don't remove (yet)" % \
                             ( len( lfns ), sorted( replicaSEs ) ) )
          onlyAtList = True
        else:
          newGroups.setdefault( ','.join( list( targetSEs ) ), [] ).extend( lfns )

      # Restrict the query to the BK to the interesting productions
      transPassLen = len( transProcPass.split( '/' ) )
      for stringTargetSEs, lfns in newGroups.items():
        self.util.logInfo( 'Checking descendants for %d files at %s' % ( len( lfns ), stringTargetSEs ) )
        # Use the cached information if any
        bkPathsToCheck = dict( [( lfn, set( self.util.cachedLFNProcessedPath.get( lfn, bkPathList ) ) & set( bkPathList ) ) for lfn in lfns] )
        # Only check files that are not fully processed
        lfnsToCheck = set( [lfn for lfn in bkPathsToCheck if bkPathsToCheck[lfn]] )
        # Update with the cached information
        for bkPath in bkPathList:
          prods = productions['List'][bkPath]
          if not prods:
            break
          lfnsToCheckForPath = set( [lfn for lfn in lfnsToCheck if bkPath in bkPathsToCheck[lfn]] )
          depth = len( bkPathList[bkPath].split( '/' ) ) - transPassLen + 1
          for prod in sorted( prods ):
            if not lfnsToCheckForPath:
              # All files have been processed, go to next bkPath
              break
            self.util.logVerbose( 'Checking descendants for %d files in production %d, depth %d' % ( len( lfnsToCheckForPath ), prod, depth ) )
            startTime = time.time()
            processedLfns = set()
            res = self.bkClient.getFileDescendants( list( lfnsToCheckForPath ), production = prod, depth = depth )
            if res['OK']:
              processedLfns.update( res['Value']['Successful'] )
            self.util.logVerbose( 'Found %s descendants in %.1f seconds' % \
                                  ( len( processedLfns ) if processedLfns else 'no',
                                    time.time() - startTime ) )
            for lfn in processedLfns:
              if bkPath not in bkPathsToCheck[lfn]:
                self.util.logWarn( 'LFN not in list: %s' % lfn, str( bkPathsToCheck[lfn] ) )
              else:
                bkPathsToCheck[lfn].remove( bkPath )
            lfnsToCheckForPath -= processedLfns
          notProcessed = [lfn for lfn in lfnsToCheckForPath if bkPath in bkPathsToCheck[lfn]]
          if notProcessed:
            self.util.logVerbose( "%d files not processed by processing pass %s, don't check further" % ( len( notProcessed ), bkPathList[bkPath] ) )
            lfnsToCheck -= set( notProcessed )
            if not lfnsToCheck:
              break

        lfnsProcessed = [lfn for lfn in lfns if not bkPathsToCheck[lfn]]
        self.util.cachedLFNProcessedPath.update( bkPathsToCheck )
        # print lfnsProcessed, bkPathsToCheck
        self.util.logInfo( "Found %d / %d files that are processed (/ not) at %s" % ( len( lfnsProcessed ),
                                                                                      len( [lfn for lfn in lfns if bkPathsToCheck[lfn]] ),
                                                                                      stringTargetSEs ) )
        if lfnsProcessed:
          targetSEs = set( stringTargetSEs.split( ',' ) )
          if not targetSEs & fromSEs:
            # Files are processed but are no longer at the requested SEs, set them Processed
            self.util.logInfo( "Processed files are no longer in required list: set them Processed" )
            self.transClient.setFileStatusForTransformation( self.transID, 'Processed', lfnsProcessed )
          else:
            storageElementGroups.setdefault( stringTargetSEs, [] ).extend( lfnsProcessed )
      if not storageElementGroups:
        return S_OK( [] )
    except Exception as e:
      self.util.logException( 'Exception while executing the plugin', '', lException = e )
      return S_ERROR( e )
    finally:
      self.util.writeCacheFile()
      if not skip and onlyAtList and self.pluginCallback:
        self.pluginCallback( self.transID, invalidateCache = True )
    return S_OK( self.util.createTasks( storageElementGroups ) )

  def _ReplicateToLocalSE( self ):
    """ Used for example to replicate from a buffer to a tape SE on the same site
    """
    destSEs = resolveSEGroup( self.util.getPluginParam( 'DestinationSEs', [] ) )
    watermark = self.util.getPluginParam( 'MinFreeSpace', 30 )
    if not destSEs:
      self.util.logWarn( 'No destination SE given' )
      return S_OK( [] )

    storageElementGroups = {}

    for replicaSE, lfns in getFileGroups( self.transReplicas ).items():
      replicaSE = [se for se in replicaSE.split( ',' ) if not self.util.dmsHelper.isSEFailover( se ) and not self.util.dmsHelper.isSEArchive( se )]
      if not replicaSE:
        continue
      if [se for se in replicaSE if se in destSEs]:
        self.util.logInfo( "Found %d files that are already present in the destination SEs (status set Processed)" % len( lfns ) )
        res = self.transClient.setFileStatusForTransformation( self.transID, 'Processed', lfns )
        if not res['OK']:
          self.util.logError( "Can't set files to Processed", '(%d files): %s' % ( len( lfns ), res['Message'] ) )
          return res
        continue
      targetSEs = [se for se in destSEs if se not in replicaSE]
      candidateSEs = self.util.closerSEs( replicaSE, targetSEs, local = True )
      if candidateSEs:
        freeSpace = self.util.getStorageFreeSpace( candidateSEs )
        shortSEs = [se for se in candidateSEs if freeSpace[se] < watermark]
        if shortSEs:
          self.util.logVerbose( "No enough space (%s TB) found at %s" % ( watermark, ','.join( shortSEs ) ) )
        candidateSEs = [se for se in candidateSEs if se not in shortSEs]
        if candidateSEs:
          storageElementGroups.setdefault( candidateSEs[0], [] ).extend( lfns )
      else:
        self.util.logWarn( "Could not find a local SE for %d files, set them Problematic" % len( lfns ) )
        res = self.transClient.setFileStatusForTransformation( self.transID, 'Problematic', lfns )
        if not res['OK']:
          self.util.logError( "Can't set files to Problematic", '(%d files): %s' % ( len( lfns ), res['Message'] ) )
          return res

    return S_OK( self.util.createTasks( storageElementGroups ) )

  def _Healing( self ):
    """ Plugin that creates task for replicating files to the same SE where they are declared problematic
    """
    self.util.cleanFiles( self.transFiles, self.transReplicas )
    storageElementGroups = {}

    for replicaSE, lfns in getFileGroups( self.transReplicas ).items():
      replicaSE = set( [se for se in replicaSE.split( ',' ) if not self.util.dmsHelper.isSEFailover( se ) and not self.util.dmsHelper.isSEArchive( se )] )
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
