"""
Actual executor methods of the dirac-transformation-debug script
"""

import sys
import os
import datetime

import DIRAC
from DIRAC.Core.Utilities.File import mkDir
from DIRAC import gLogger
from DIRAC.Core.Base import Script
from DIRAC.RequestManagementSystem.Client.ReqClient import ReqClient, printOperation
from DIRAC.DataManagementSystem.Client.DataManager import DataManager
from DIRAC.Resources.Catalog.FileCatalog import FileCatalog
from DIRAC.Core.Utilities.List import breakListIntoChunks

from LHCbDIRAC.TransformationSystem.Client.TransformationClient import TransformationClient
from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
from LHCbDIRAC.TransformationSystem.Utilities.PluginUtilities import PluginUtilities

class TransformationDebug( object ):
  """
  This class houses all methods for debugging transformations
  """
  def __init__( self ):

    self.transClient = TransformationClient()
    self.reqClient = ReqClient()
    self.bkClient = BookkeepingClient()
    self.dm = DataManager()
    self.fc = FileCatalog()
    self.dmTransTypes = ( "Replication", "Removal" )
    self.transID = None
    self.transType = None
    self.fixIt = False
    self.kickRequests = False
    self.pluginUtil = None
    self.listOfAssignedRequests = {}

  def __getFilesForRun( self, runID = None, status = None, lfnList = None, seList = None, taskList = None ):
    # print transID, runID, status, lfnList
    selectDict = {}
    if runID is not None:
      if runID:
        selectDict["RunNumber"] = runID
      else:
        selectDict["RunNumber"] = str( runID )
    if status:
      selectDict['Status'] = status
    if lfnList:
      selectDict['LFN'] = lfnList
    if seList:
      selectDict['UsedSE'] = seList
    if taskList:
      selectDict['TaskID'] = taskList
    selectDict['TransformationID'] = self.transID
    res = self.transClient.getTransformationFiles( selectDict )
    if res['OK']:
      return res['Value']
    else:
      gLogger.error( "Error getting TransformationFiles:", res['Message'] )
      return []

  def __filesProcessed( self, runID ):
    transFilesList = self.__getFilesForRun( runID, None )
    files = 0
    processed = 0
    for fileDict in transFilesList:
      files += 1
      if fileDict['Status'] == "Processed":
        processed += 1
    return ( files, processed )

  def __getRuns( self, runList = None, byRuns = True, seList = None, status = None, taskList = None ):
    runs = []
    if status and byRuns and not runList:
      files = self.__getFilesForRun( status = status, taskList = taskList )
      runList = []
      for fileDict in files:
        run = fileDict['RunNumber']
        if run not in runList:
          runList.append( str( run ) )

    if runList:
      for runRange in runList:
        runRange = runRange.split( ':' )
        if len( runRange ) == 1:
          runs.append( int( runRange[0] ) )
        else:
          for run in xrange( int( runRange[0] ), int( runRange[1] ) + 1 ):
            runs.append( run )
      selectDict = {'TransformationID':self.transID, 'RunNumber': runs}
      if runs == [0]:
        runs = [{'RunNumber':0}]
      else:
        if seList:
          selectDict['SelectedSite'] = seList
        res = self.transClient.getTransformationRuns( selectDict )
        if res['OK']:
          if not len( res['Value'] ):
            gLogger.notice( "No runs found, set to None" )
            runs = [{'RunNumber':None}]
          else:
            runs = res['Value']
    elif not byRuns:
      # No run selection
      runs = [{'RunNumber': None}]
    elif not status:
      # All runs selected explicitly
      selectDict = {'TransformationID':self.transID}
      if seList:
        selectDict['SelectedSite'] = seList
      res = self.transClient.getTransformationRuns( selectDict )
      if res['OK']:
        if not len( res['Value'] ):
          gLogger.notice( "No runs found, set to 0" )
          runs = [{'RunNumber':None}]
        else:
          runs = res['Value']
    return runs

  def __justStats( self, status, seList ):
    improperJobs = []
    if not status:
      status = "Assigned"
    transFilesList = self.__getFilesForRun( None, status, [], seList )
    if not transFilesList:
      return improperJobs
    statsPerSE = {}
    # print transFilesList
    statusList = {'Received', 'Checking', 'Staging', 'Waiting', 'Running', 'Stalled'}
    if status == 'Processed':
      statusList.update( {'Done', 'Completed', 'Failed'} )
    taskList = [fileDict['TaskID'] for fileDict in transFilesList]
    res = self.transClient.getTransformationTasks( {'TransformationID':self.transID, "TaskID":taskList} )
    if not res['OK']:
      gLogger.notice( "Could not get the list of tasks..." , res['Message'] )
      DIRAC.exit( 2 )
    for task in res['Value']:
      # print task
      targetSE = task['TargetSE']
      stat = task['ExternalStatus']
      statusList.add( stat )
      statsPerSE[targetSE][stat] = statsPerSE.setdefault( targetSE, dict.fromkeys( statusList, 0 ) ).setdefault( stat, 0 ) + 1
      if status == 'Processed' and stat not in ( 'Done', 'Completed', 'Stalled', 'Failed', 'Killed', 'Running' ):
        improperJobs.append( task['ExternalID'] )

    shift = 0
    for se in statsPerSE:
      shift = max( shift, len( se ) + 2 )
    prString = 'SE'.ljust( shift )
    for stat in statusList:
      prString += stat.ljust( 10 )
    gLogger.notice( prString )
    for se in sorted( statsPerSE ):
      prString = se.ljust( shift )
      for stat in statusList:
        prString += str( statsPerSE[se].get( stat, 0 ) ).ljust( 10 )
      gLogger.notice( prString )
    return improperJobs

  def __getTransformationInfo( self, transSep ):
    res = self.transClient.getTransformation( self.transID, extraParams = False )
    if not res['OK']:
      gLogger.notice( "Couldn't find transformation", self.transID )
      return None, None
    else:
      transStatus = res['Value']['Status']
      self.transType = res['Value']['Type']
      transBody = res['Value']['Body']
      self.transPlugin = res['Value']['Plugin']
      strPlugin = self.transPlugin
      if self.transType in ( 'Merge', 'MCMerge', 'DataStripping', 'MCStripping' ):
        strPlugin += ', GroupSize: %s' % str( res['Value']['GroupSize'] )
      if self.transType in self.dmTransTypes:
        taskType = "Request"
      else:
        taskType = "Job"
      transGroup = res['Value']['TransformationGroup']
    gLogger.notice( "%s Transformation %d (%s) of type %s (plugin %s) in %s" % ( transSep, self.transID, transStatus, self.transType, strPlugin, transGroup ) )
    if self.transType == 'Removal':
      gLogger.notice( "Transformation body:", transBody )
    res = self.transClient.getBookkeepingQuery( self.transID )
    if res['OK'] and res['Value']:
      gLogger.notice( "BKQuery:", res['Value'] )
      queryFileTypes = res['Value'].get( 'FileType' )
    else:
      gLogger.notice( "No BKQuery for this transformation" )
      queryFileTypes = None
    gLogger.notice( "" )
    return taskType, queryFileTypes

  def __fixRunNumber( self, filesToFix, fixRun, noTable = False ):
    if not fixRun:
      if noTable:
        gLogger.notice( '%d files have run number not in run table, use --FixRun to get this fixed' % len( filesToFix ) )
      else:
        gLogger.notice( '%d files have run number 0, use --FixRun to get this fixed' % len( filesToFix ) )
    else:
      fixedFiles = 0
      res = self.bkClient.getFileMetadata( filesToFix )
      if res['OK']:
        runFiles = {}
        for lfn, metadata in res['Value']['Successful'].iteritems():
          runFiles.setdefault( metadata['RunNumber'], [] ).append( lfn )
        for run in runFiles:
          if not run:
            gLogger.notice( "%d files found in BK with run '%s': %s" % ( len( runFiles[run] ), str( run ), str( runFiles[run] ) ) )
            continue
          res = self.transClient.addTransformationRunFiles( self.transID, run, runFiles[run] )
          # print run, runFiles[run], res
          if not res['OK']:
            gLogger.notice( "***ERROR*** setting %d files to run %d in transformation %d" % ( len( runFiles[run] ), run, self.transID ), res['Message'] )
          else:
            fixedFiles += len( runFiles[run] )
        if fixedFiles:
          gLogger.notice( "Successfully fixed run number for %d files" % fixedFiles )
        else:
          gLogger.notice( "There were no files for which to fix the run number" )
      else:
        gLogger.notice( "***ERROR*** getting metadata for %d files:" % len( filesToFix ), res['Message'] )

  def __getTransformations( self, args ):
    if not len( args ):
      gLogger.notice( "Specify transformation number..." )
      Script.showHelp()
    else:
      ids = args[0].split( "," )
      transList = []
      for transID in ids:
        r = transID.split( ':' )
        if len( r ) > 1:
          for i in xrange( int( r[0] ), int( r[1] ) + 1 ):
            transList.append( i )
        else:
          transList.append( int( r[0] ) )
    return transList

  def __checkFilesMissingInFC( self, transFilesList, status ):
    if 'MissingLFC' in status or 'MissingInFC' in status:
      lfns = [fileDict['LFN'] for fileDict in transFilesList]
      res = self.dm.getReplicas( lfns )
      if res['OK']:
        replicas = res['Value']['Successful']
        notMissing = len( replicas )
        if notMissing:
          if not self.kickRequests:
            gLogger.notice( "%d files are %s but indeed are in the FC - Use --KickRequests to reset them Unused" % ( notMissing, status ) )
          else:
            res = self.transClient.setFileStatusForTransformation( self.transID, 'Unused', replicas.keys(), force = True )
            if res['OK']:
              gLogger.notice( "%d files were %s but indeed are in the FC - Reset to Unused" % ( notMissing, status ) )
            else:
              gLogger.notice( "Error resetting %d files Unused" % notMissing, res['Message'] )
        else:
          res = self.bkClient.getFileMetadata( lfns )
          if not res['OK']:
            gLogger.notice( "ERROR getting metadata from BK", res['Message'] )
          else:
            metadata = res['Value']['Successful']
            lfnsWithReplicaFlag = [lfn for lfn in metadata if metadata[lfn]['GotReplica'] == 'Yes']
            if lfnsWithReplicaFlag:
              gLogger.notice( "All files are really missing in FC" )
              if not self.fixIt:
                gLogger.notice( '%d files are not in the FC but have a replica flag in BK, use --FixIt to fix' % len( lfnsWithReplicaFlag ) )
              else:
                res = self.bkClient.removeFiles( lfnsWithReplicaFlag )
                if not res['OK']:
                  gLogger.notice( "ERROR removing replica flag:", res['Message'] )
                else:
                  gLogger.notice( "Replica flag removed from %d files" % len( lfnsWithReplicaFlag ) )
            else:
              gLogger.notice( "All files are really missing in FC and BK" )

  def __getReplicas( self, lfns ):
    replicas = {}
    for lfnChunk in breakListIntoChunks( lfns, 200 ):
      res = self.dm.getReplicas( lfnChunk )
      if res['OK']:
        replicas.update( res['Value']['Successful'] )
      else:
        gLogger.notice( "Error getting replicas", res['Message'] )
    return replicas

  def __getTask( self, taskID ):
    res = self.transClient.getTransformationTasks( {'TransformationID':self.transID, "TaskID":taskID} )
    if not res['OK'] or not res['Value']:
      return None
    return res['Value'][0]

  def __fillStatsPerSE( self, SEStat, rep, listSEs ):
    SEStat["Total"] += 1
    completed = True
    if not rep:
      SEStat[None] = SEStat.setdefault( None, 0 ) + 1
    for se in listSEs:
      if self.transType == "Replication":
        if se not in rep:
          SEStat[se] = SEStat.setdefault( se, 0 ) + 1
          completed = False
      elif self.transType == "Removal":
        if se in rep:
          SEStat[se] = SEStat.setdefault( se, 0 ) + 1
          completed = False
      else:
        if se not in rep:
          SEStat[se] = SEStat.setdefault( se, 0 ) + 1
    return completed

  def __getRequestName( self, requestID ):
    level = gLogger.getLevel()
    gLogger.setLevel( 'FATAL' )
    try:
      if not requestID:
        return None
      res = self.reqClient.getRequestInfo( requestID )
      if res['OK']:
        return res['Value'][2]
      gLogger.notice( "No such request found: %s" % requestID )
      return None
    except:
      return None
    finally:
      gLogger.setLevel( level )

  def __getAssignedRequests( self ):
    if not self.listOfAssignedRequests:
      res = self.reqClient.getRequestIDsList( ['Assigned'], limit = 10000 )
      if res['OK']:
        self.listOfAssignedRequests = [reqID for reqID , _x, _y in res['Value']]

  def __printRequestInfo( self, task, lfnsInTask, taskCompleted, status, dmFileStatusComment ):
    requestID = int( task['ExternalID'] )
    taskID = task['TaskID']
    taskName = '%08d_%08d' % ( self.transID, taskID )
    if taskCompleted and ( task['ExternalStatus'] not in ( 'Done', 'Failed' ) or status in ( 'Assigned', 'Problematic' ) ):
      prString = "\tTask %s is completed: no %s replicas" % ( taskName, dmFileStatusComment )
      if self.kickRequests:
        res = self.transClient.setFileStatusForTransformation( self.transID, 'Processed', lfnsInTask, force = True )
        if res['OK']:
          prString += " - %d files set Processed" % len( lfnsInTask )
        else:
          prString += " - Failed to set %d files Processed (%s)" % ( len( lfnsInTask ), res['Message'] )
      else:
          prString += " - To mark files Processed, use option --KickRequests"
      gLogger.notice( prString )

    if not requestID:
      if task['ExternalStatus'] == 'Submitted' and not taskCompleted:
        prString = "\tTask %s is submitted but has no external ID" % taskName
        if self.kickRequests:
          res = self.transClient.setFileStatusForTransformation( self.transID, 'Unused', lfnsInTask )
          if res['OK']:
            prString += " - %d files set Unused" % len( lfnsInTask )
          else:
            prString += " - Failed to set %d files Unused (%s)" % ( len( lfnsInTask ), res['Message'] )
        else:
            prString += " - To mark files Unused, use option --KickRequests"
        gLogger.notice( prString )
      return 0
    # This method updates self.listOfAssignedRequests
    self.__getAssignedRequests()
    request = None
    res = self.reqClient.peekRequest( requestID )
    if res['OK']:
      if res['Value'] is not None:
        request = res['Value']
        requestStatus = request.Status if request.RequestID not in self.listOfAssignedRequests else 'Assigned'
        if requestStatus != task['ExternalStatus']:
          gLogger.notice( '\tRequest %d status: %s updated last %s' % ( requestID, requestStatus, request.LastUpdate ) )
        if task['ExternalStatus'] == 'Failed':
          # Find out why this task is failed
          for i, op in enumerate( request ):
            if op.Status == 'Failed':
              printOperation( ( i, op ), onlyFailed = True )
      else:
        requestStatus = 'NotExisting'
    else:
      gLogger.notice( "Failed to peek request:", res['Message'] )
      requestStatus = 'Unknown'

    res = self.reqClient.getRequestFileStatus( requestID, lfnsInTask )
    if res['OK']:
      reqFiles = res['Value']
      statFiles = {}
      for stat in reqFiles.itervalues():
        statFiles[stat] = statFiles.setdefault( stat, 0 ) + 1
      for stat in sorted( statFiles ):
        gLogger.notice( "\t%s: %d files" % ( stat, statFiles[stat] ) )
      # If all files failed, set the request as failed
      if requestStatus != 'Failed' and statFiles.get( 'Failed', -1 ) == len( reqFiles ):
        prString = "\tAll transfers failed for that request"
        if not self.kickRequests:
          prString += ": it should be marked as Failed, use --KickRequests"
        else:
          request.Status = 'Failed'
          res = self.reqClient.putRequest( request )
          if res['OK']:
            prString += ": request set to Failed"
          else:
            prString += ": error setting to Failed: %s" % res['Message']
        gLogger.notice( prString )
      # If some files are Scheduled, try and get information about the FTS jobs
      if statFiles.get( 'Scheduled', 0 ) and request:
        from DIRAC.DataManagementSystem.Client.FTSClient                                  import FTSClient
        ftsClient = FTSClient()
        res = ftsClient.getAllFTSFilesForRequest( request.RequestID )
        if res['OK']:
          statusCount = {}
          for ftsFile in res['Value']:
            statusCount[ftsFile.Status] = statusCount.setdefault( ftsFile.Status, 0 ) + 1
          prStr = []
          for status in statusCount:
            if statusCount[status]:
              prStr.append( '%s:%d' % ( status, statusCount[status] ) )
          gLogger.notice( '\tFTS files statuses: %s' % ', '.join( prStr ) )
        res = ftsClient.getFTSJobsForRequest( request.RequestID )
        if res['OK']:
          ftsJobs = res['Value']
          if ftsJobs:
            for job in ftsJobs:
              gLogger.notice( '\tFTS jobs associated:', '%s@%s (%s) from %s to %s' % ( job.FTSGUID, job.FTSServer, job.Status, job.SourceSE, job.TargetSE ) )
          else:
            gLogger.notice( '\tNo FTS jobs found for that request' )

    # Kicking stuck requests in status Assigned
    toBeKicked = 0
    assignedReqLimit = datetime.datetime.utcnow() - datetime.timedelta( hours = 2 )
    if request:
      if request.RequestID in self.listOfAssignedRequests and request.LastUpdate < assignedReqLimit:
        gLogger.notice( "\tRequest stuck: %d Updated %s" % ( request.RequestID, request.LastUpdate ) )
        toBeKicked += 1
        if self.kickRequests:
          res = self.reqClient.putRequest( request )
          if res['OK']:
            gLogger.notice( '\tRequest %d is reset' % requestID )
          else:
            gLogger.notice( '\tError resetting request', res['Message'] )
    else:
      selectDict = { 'RequestID':requestID}
      res = self.reqClient.getRequestSummaryWeb( selectDict, [], 0, 100000 )
      if res['OK']:
        params = res['Value']['ParameterNames']
        records = res['Value']['Records']
        for rec in records:
          subReqDict = {}
          subReqStr = ''
          conj = ''
          for i in xrange( len( params ) ):
            subReqDict.update( { params[i]:rec[i] } )
            subReqStr += conj + params[i] + ': ' + rec[i]
            conj = ', '

          if subReqDict['Status'] == 'Assigned' and subReqDict['LastUpdateTime'] < str( assignedReqLimit ):
            gLogger.notice( subReqStr )
            toBeKicked += 1
            if self.kickRequests:
              res = self.reqClient.setRequestStatus( requestID, 'Waiting' )
              if res['OK']:
                gLogger.notice( '\tRequest %d reset Waiting' % requestID )
              else:
                gLogger.notice( '\tError resetting request %d' % requestID, res['Message'] )
    return toBeKicked

  def __checkProblematicFiles( self, nbReplicasProblematic, problematicReplicas, failedFiles ):
    from DIRAC.Core.Utilities.Adler import compareAdler
    gLogger.notice( "\nStatistics for Problematic files in FC:" )
    existingReplicas = {}
    lfns = set()
    lfnsInFC = set()
    for n in sorted( nbReplicasProblematic ):
      gLogger.notice( "   %d replicas in FC: %d files" % ( n, nbReplicasProblematic[n] ) )
    # level = gLogger.getLevel()
    # gLogger.setLevel( 'FATAL' )
    lfnCheckSum = {}
    badChecksum = {}
    for se in problematicReplicas:
      lfns.update( problematicReplicas[se] )
      if se:
        lfnsInFC.update( problematicReplicas[se] )
        res = self.fc.getFileMetadata( [lfn for lfn in problematicReplicas[se] if lfn not in lfnCheckSum] )
        if res['OK']:
          success = res['Value']['Successful']
          lfnCheckSum.update( dict( ( lfn, success[lfn]['Checksum'] ) for lfn in success ) )
        res = self.dm.getReplicaMetadata( problematicReplicas[se], se )
        if res['OK']:
          for lfn in res['Value']['Successful']:
            existingReplicas.setdefault( lfn, [] ).append( se )
            # Compare checksums
            checkSum = res['Value']['Successful'][lfn]['Checksum']
            if not checkSum or not compareAdler( checkSum, lfnCheckSum[lfn] ):
              badChecksum.setdefault( lfn, [] ).append( se )
    nbProblematic = len( lfns ) - len( existingReplicas )
    nbExistingReplicas = {}
    for lfn in existingReplicas:
      nbReplicas = len( existingReplicas[lfn] )
      nbExistingReplicas[nbReplicas] = nbExistingReplicas.setdefault( nbReplicas, 0 ) + 1
    nonExistingReplicas = {}
    if nbProblematic == len( lfns ):
        gLogger.notice( "None of the %d problematic files actually have an active replica" % len( lfns ) )
    else:
      strMsg = "Out of %d problematic files" % len( lfns )
      if nbProblematic:
        strMsg += ", only %d have an active replica" % ( len( lfns ) - nbProblematic )
      else:
        strMsg += ", all have an active replica"
      gLogger.notice( strMsg )
      for n in sorted( nbExistingReplicas ):
        gLogger.notice( "   %d active replicas: %d files" % ( n, nbExistingReplicas[n] ) )
      for se in problematicReplicas:
        lfns = [lfn for lfn in problematicReplicas[se] if lfn not in existingReplicas or se not in existingReplicas[lfn]]
        str2Msg = ''
        if len( lfns ):
          nonExistingReplicas.setdefault( se, [] ).extend( lfns )
          if not self.fixIt:
            str2Msg = ' Use --FixIt to remove them'
          else:
            str2Msg = ' Will be removed from FC'
          strMsg = '%d' % len( lfns )
        else:
          strMsg = 'none'
        if se:
          gLogger.notice( "   %s : %d replicas of problematic files in FC, %s physically missing.%s" % ( str( se ).ljust( 15 ), len( problematicReplicas[se] ), strMsg, str2Msg ) )
        else:
          gLogger.notice( "   %s : %d files are not in self.fc." % ( ''.ljust( 15 ), len( problematicReplicas[se] ) ) )
      lfns = [lfn for lfn in existingReplicas if lfn in failedFiles]
      if lfns:
        prString = "Failed transfers but existing replicas"
        if self.fixIt:
          prString += '. Use --FixIt to fix it'
        else:
          for lfn in lfns:
            res = self.transClient.setFileStatusForTransformation( self.transID, 'Unused', lfns, force = True )
            if res['OK']:
              prString += " - %d files reset Unused" % len( lfns )
        gLogger.notice( prString )
    filesInFCNotExisting = list( lfnsInFC - set( existingReplicas ) )
    if filesInFCNotExisting:
      prString = '%d files are in the FC but are not physically existing. ' % len( filesInFCNotExisting )
      if self.fixIt:
        prString += 'Removing them now from self.fc...'
      else:
        prString += 'Use --FixIt to remove them'
      gLogger.notice( prString )
      if self.fixIt:
        self.__removeFiles( filesInFCNotExisting )
    if badChecksum:
      prString = '%d files have a checksum mismatch:' % len( badChecksum )
      replicasToRemove = {}
      filesToRemove = []
      for lfn in badChecksum:
        if badChecksum[lfn] == existingReplicas[lfn]:
          filesToRemove.append( lfn )
        else:
          replicasToRemove[lfn] = badChecksum[lfn]
      if filesToRemove:
        prString += ' %d files have no correct replica;' % len( filesToRemove )
      if replicasToRemove:
        prString += ' %d files have at least an incorrect replica' % len( replicasToRemove )
      if not self.fixIt:
        prString += ' Use --FixIt to remove them'
      else:
        prString += ' Removing them now...'
      gLogger.notice( prString )
      if self.fixIt:
        if filesToRemove:
          self.__removeFiles( filesToRemove )
        if replicasToRemove:
          seFiles = {}
          for lfn in replicasToRemove:
            for se in replicasToRemove[lfn]:
              seFiles.setdefault( se, [] ).append( lfn )
          for se in seFiles:
            res = self.dm.removeReplica( se, seFiles[se] )
            if not res['OK']:
              gLogger.notice( 'ERROR: error removing replicas', res['Message'] )
            else:
              gLogger.notice( "Successfully removed %d replicas from %s" % ( len( seFiles[se], se ) ) )
    elif existingReplicas:
      gLogger.notice( "All existing replicas have a good checksum" )
    if self.fixIt and nonExistingReplicas:
      nRemoved = 0
      failures = {}
      # If SE == None, the file is not in the FC
      notInFC = nonExistingReplicas.get( None )
      if notInFC:
        nonExistingReplicas.pop( None )
        nRemoved, transRemoved = self.__removeFilesFromTS( notInFC )
        if nRemoved:
          gLogger.notice( 'Successfully removed %d files from transformations %s' % ( nRemoved, ','.join( transRemoved ) ) )
      for se in nonExistingReplicas:
        lfns = [lfn for lfn in nonExistingReplicas[se] if lfn not in filesInFCNotExisting]
        res = self.dm.removeReplica( se, lfns )
        if not res['OK']:
          gLogger.notice( "ERROR when removing replicas from FC at %s" % se, res['Message'] )
        else:
          failed = res['Value']['Failed']
          if failed:
            gLogger.notice( "Failed to remove %d replicas at %s" % ( len( failed ), se ) )
            gLogger.notice( '\n'.join( sorted( failed ) ) )
            for lfn in failed:
              failures.setdefault( failed[lfn], [] ).append( lfn )
          nRemoved += len( res['Value']['Successful'] )
      if nRemoved:
        gLogger.notice( "Successfully removed %s replicas from FC" % nRemoved )
      if failures:
        gLogger.notice( "Failures:" )
        for error in failures:
          gLogger.notice( "%s: %d replicas" % ( error, len( failures[error] ) ) )
    gLogger.notice( "" )

  def __removeFilesFromTS( self, lfns ):
    res = self.transClient.getTransformationFiles( {'LFN':lfns} )
    if not res['OK']:
      gLogger.notice( "Error getting %d files in the TS" % len( lfns ), res['Message'] )
      return
    transFiles = {}
    removed = 0
    for fd in res['Value']:
      transFiles.setdefault( fd['TransformationID'], [] ).append( fd['LFN'] )
    for transID, lfns in transFiles.iteritems():
      res = self.transClient.setFileStatusForTransformation( transID, 'Removed', lfns, force = True )
      if not res['OK']:
        gLogger.notice( 'Error setting %d files Removed' % len( lfns ), res['Message'] )
      else:
        removed += len( lfns )
    return removed, [str( tr ) for tr in transFiles]

  def __removeFiles( self, lfns ):
    res = self.dm.removeFile( lfns )
    if res['OK']:
      gLogger.notice( "Successfully removed %d files from FC" % len( lfns ) )
      nRemoved, transRemoved = self.__removeFilesFromTS( lfns )
      if nRemoved:
        gLogger.notice( 'Successfully removed %d files from transformations %s' % ( nRemoved, ','.join( transRemoved ) ) )
    else:
      gLogger.notice( "ERROR when removing files from FC:", res['Message'] )

  def __checkReplicasForProblematic( self, lfns, replicas, nbReplicasProblematic, problematicReplicas ):
    for lfn in lfns:
      # Problematic files, let's see why
      realSEs = [se for se in replicas.get( lfn, [] ) if not se.endswith( '-ARCHIVE' )]
      nbSEs = len( realSEs )
      nbReplicasProblematic[nbSEs] = nbReplicasProblematic.setdefault( nbSEs, 0 ) + 1
      if not nbSEs:
        problematicReplicas.setdefault( None, [] ).append( lfn )
      for se in realSEs:
        problematicReplicas.setdefault( se, [] ).append( lfn )

  def __getLog( self, urlBase, logFile, debug = False ):
    import gzip, urllib, tarfile, fnmatch
    # if logFile == "" it is assumed the file is directly the urlBase
    # Otherwise it can either be referenced within urlBase or contained (.tar.gz)
    if debug:
      print "Entering getLog", urlBase, logFile
    url = urlBase
    if logFile and ".tgz" not in url:
      if debug: print "Opening URL ", url
      f = urllib.urlopen( url )
      c = f.read()
      f.close()
      if "was not found on this server." in c:
        return ""
      c = c.split( "\n" )
      logURL = None
      for l in c:
        # If the line matches the requested URL
        if fnmatch.fnmatch( l, '*' + logFile + '*' ):
          if debug:
            print "Match found:", l
          try:
            logURL = l.split( '"' )[1]
            break
          except:
            pass
        elif fnmatch.fnmatch( l, '*.tgz*' ):
          # If a tgz file is found, it could help!
          try:
            logURL = l.split( '"' )[1]
          except:
            pass
      if not logURL:
          return ''
      url += logURL
      if debug:
        print "URL found:", url
    tmp = None
    tmp1 = None
    tf = None
    if ".tgz" in url or '.gz' in url:
      if debug: print "Opening tgz file ", url
      # retrieve the zipped file
      tmp = os.path.join( os.environ.get( "TMPDIR", "/tmp" ), "logFile.tmp" )
      if debug: print "Retrieve the file in ", tmp
      if os.path.exists( tmp ):
        os.remove( tmp )
      urllib.urlretrieve( url, tmp )
      f = open( tmp, "r" )
      c = f.read()
      f.close()
      if "404 Not Found" in c:
        return ""
        # unpack the tarfile
      if debug: print "Open tarfile ", tmp
      tf = tarfile.open( tmp, 'r:gz' )
      mn = tf.getnames()
      f = None
      if debug: print "Found those members", mn, ', looking for', logFile
      for fileName in mn:
        if fnmatch.fnmatch( fileName, logFile + '*' ):
          if debug: print "Found ", logFile, " in tar object ", fileName
          if '.gz' in fileName:
            # file is again a gzip file!!
            tmp1 = os.path.join( os.environ.get( "TMPDIR", "/tmp" ), "logFile-1.tmp" )
            if debug: print "Extract", fileName, "into", tmp1, "and open it"
            tf.extract( fileName, tmp1 )
            tmp1 = os.path.join( tmp1, fileName )
            f = gzip.GzipFile( tmp1, 'r' )
          else:
            f = tf.extractfile( fileName )
          break
    else:
      f = urllib.urlopen( url )
    # read the actual file...
    if not f:
      if debug: print "Couldn't open file..."
      c = ''
    else:
      if debug: print "File successfully open"
      c = f.read()
      f.close()
      if "was not found on this server." not in c:
        c = c.split( "\n" )
        if debug: print "Reading the file now... %d lines" % len( c )
      else:
        c = ''
    if tf:
      tf.close()
    if tmp:
      os.remove( tmp )
    if tmp1:
      os.remove( tmp1 )
    return c

  def __getSandbox( self, job, logFile, debug = False ):
    from DIRAC.WorkloadManagementSystem.Client.SandboxStoreClient     import SandboxStoreClient
    import fnmatch
    sbClient = SandboxStoreClient()
    tmpDir = os.path.join( os.environ.get( "TMPDIR", "/tmp" ), "sandBoxes/" )
    mkDir( tmpDir )
    f = None
    files = []
    try:
      res = sbClient.downloadSandboxForJob( job, 'Output', tmpDir )
      if res['OK']:
        if debug: print 'Job', job, ': sandbox retrieved in', tmpDir
        files = os.listdir( tmpDir )
        if debug: print 'Files:', files
        for lf in files:
          if fnmatch.fnmatch( lf, logFile ):
            if debug: print file, 'matched', logFile
            f = open( os.path.join( tmpDir, lf ), 'r' )
            return f.readlines()
        return ''
    except Exception as e:
      gLogger.exception( 'Exception while getting sandbox', lException = e )
      return ''
    finally:
      if f:
        f.close()
      for lf in files:
        os.remove( os.path.join( tmpDir, lf ) )
      os.rmdir( tmpDir )


  def __checkXMLSummary( self, job, logURL ):
    xmlFile = self.__getLog( logURL, 'summary*.xml*', debug = False )
    if not xmlFile:
      xmlFile = self.__getSandbox( job, 'summary*.xml*', debug = False )
    lfns = {}
    if xmlFile:
      for line in xmlFile:
        if 'status="part"' in line and 'LFN:' in line:
          event = line.split( '>' )[1].split( '<' )[0]
          lfns.update( {line.split( 'LFN:' )[1].split( '"' )[0] : 'Partial (last event %s)' % event} )
        elif 'status="fail"' in line and 'LFN:' in line:
          lfns.update( {line.split( 'LFN:' )[1].split( '"' )[0] : 'Failed'} )
      if not lfns:
        lfns = {None:'No errors found in XML summary'}
    return lfns

  def __checkLog( self, logURL ):
    for i in xrange( 5, 0, -1 ):
      logFile = self.__getLog( logURL, '*_%d.log' % i, debug = False )
      if logFile:
        break
    logDump = []
    if logFile:
      space = False
      for line in logFile:
        if ' ERROR ' in line or '*** Break ***' in line:
          if space:
            logDump.append( '....' )
            space = False
          logDump.append( line )
        else:
          space = True
        if 'Stalled event' in line:
          logDump = ['Stalled Event']
          break
    else:
      logDump = ["Couldn't find log file in %s" % logURL]
    return logDump[-10:]

  def __genericLfn( self, lfn, lfnList ):
    if lfn not in lfnList and os.path.dirname( lfn ) == '':
      spl = lfn.split( '_' )
      if len( spl ) == 3:
        spl[1] = '<jobNumber>'
      lfn = '_'.join( spl )
    return lfn

  def __checkJobs( self, jobsForLfn, byFiles = False, checkLogs = False ):
    from DIRAC.Core.DISET.RPCClient                          import RPCClient
    monitoring = RPCClient( 'WorkloadManagement/JobMonitoring' )
    failedLfns = {}
    jobLogURL = {}
    jobSites = {}
    for lfnStr, allJobs in jobsForLfn.iteritems():
      lfnList = lfnStr.split( ',' )
      exitedJobs = []
      allJobs.sort()
      res = monitoring.getJobsStatus( allJobs )
      if res['OK']:
        jobStatus = res['Value']
        res = monitoring.getJobsMinorStatus( allJobs )
        if res['OK']:
          jobMinorStatus = res['Value']
          res = monitoring.getJobsApplicationStatus( allJobs )
          if res['OK']:
            jobApplicationStatus = res['Value']
      if not res['OK']:
        gLogger.notice( 'Error getting jobs statuses:', res['Message'] )
        return
      allStatus = {}
      for job in [int( j ) for j in allJobs]:
        status = jobStatus.get( job, {} ).get( 'Status', 'Unknown' ) + '; ' + \
                 jobMinorStatus.get( job, {} ).get( 'MinorStatus', 'Unknown' ) + '; ' + \
                 jobApplicationStatus.get( job, {} ).get( 'ApplicationStatus', 'Unknown' )
        allStatus[job] = status
      if byFiles or len( lfnList ) < 3:
        gLogger.notice( '\n %d LFNs: %s : Status of corresponding %d jobs (ordered):' % ( len( lfnList ), lfnList, len( allJobs ) ) )
      else:
        gLogger.notice( '\n %d LFNs: Status of corresponding %d jobs (ordered):' % ( len( lfnList ), len( allJobs ) ) )
      gLogger.notice( ' '.join( allJobs ) )
      prevStatus = None
      allStatus[sys.maxint] = ''
      jobs = []
      for job in sorted( allStatus ):
        status = allStatus[job]
        if status == prevStatus:
          jobs.append( job )
          continue
        elif not prevStatus:
          prevStatus = status
          jobs = [job]
          continue
        prStr = '%3d jobs' % len( jobs )
        if 'Failed' in prevStatus or 'Done' in prevStatus or 'Completed' in prevStatus:
          prStr += ' terminated with status:'
        else:
          prStr += ' in status:'
        gLogger.notice( prStr, prevStatus )
        majorStatus, minorStatus, applicationStatus = prevStatus.split( '; ' )
        if majorStatus == 'Failed' and ( 'Exited With Status' in applicationStatus or 'Problem Executing Application' in applicationStatus ):
          exitedJobs += jobs
        if majorStatus == 'Failed' and minorStatus == 'Job stalled: pilot not running':
          lastLine = ''
          # Now get last lines
          res = monitoring.getJobsSites( jobs )
          if res['OK']:
            jobSites.update( res['Value'] )
          for job1 in sorted( jobs ) + [0]:
            if job1:
              res = monitoring.getJobParameter( int( job1 ), 'StandardOutput' )
              if res['OK']:
                line = '(%s) ' % jobSites.get( job1, {} ).get( 'Site', 'Unknown' ) + res['Value'].get( 'StandardOutput', 'stdout not available\n' ).splitlines()[-1].split( 'UTC ' )[-1]
            else:
              line = ''
            if not lastLine:
              lastLine = line
              jobs = [job1]
              continue
            elif line == lastLine:
              jobs.append( job )
              continue
            for xx in ( 'LbLogin.sh', 'SetupProject.sh' ):
              if xx in lastLine:
                lastLine = lastLine.split()[0] + ' Executing %s' % xx + lastLine.split( xx )[1].split( '&&' )[0]
                break
            gLogger.notice( '\t%3d jobs stalled with last line: %s' % ( len( jobs ), lastLine ) )
            lastLine = line
            jobs = [job1]
        jobs = [job]
        prevStatus = status
        if exitedJobs:
          badLfns = {}
          for lastJob in sorted( exitedJobs, reverse = True )[0:10]:
            res = monitoring.getJobParameter( int( lastJob ), 'Log URL' )
            if res['OK'] and 'Log URL' in res['Value']:
              logURL = res['Value']['Log URL'].split( '"' )[1] + '/'
              jobLogURL[lastJob] = logURL
              lfns = self.__checkXMLSummary( lastJob, logURL )
              lfns = dict( ( self.__genericLfn( lfn, lfnList ), lfns[lfn] ) for lfn in lfns if lfn )
              if lfns:
                badLfns.update( {lastJob: lfns} )
            # break
          if not badLfns:
            gLogger.notice( "No error was found in XML summary files" )
          else:
            # lfnsFound is an AND of files found bad in all jobs
            lfnsFound = set( badLfns[sorted( badLfns, reverse = True )[0]] )
            for lfns in badLfns.itervalues():
              lfnsFound &= set( lfns )
            if lfnsFound:
              for lfn, job, reason in [( lfn, job, badLfns[job][lfn] )
                                       for job, lfns in badLfns.iteritems() for lfn in set( lfns ) & lfnsFound]:
                failedLfns.setdefault( ( lfn, reason ), [] ).append( job )
            else:
              gLogger.notice( "No common error was found in all XML summary files" )
    if failedLfns:
      gLogger.notice( "\nSummary of failures due to: Application Exited with non-zero status" )
      lfnDict = {}
      partial = 'Partial (last event '
      for ( lfn, reason ), jobs in failedLfns.items():
        if partial not in reason:
          continue
        failedLfns.pop( ( lfn, reason ) )
        otherReasons = lfnDict.get( lfn )
        if not otherReasons:
          lfnDict[lfn] = ( reason, jobs )
        else:
          lastEvent = int( reason.split( partial )[1][:-1] )
          lfnDict[lfn] = ( otherReasons[0][:-1] + ',%d)' % lastEvent, otherReasons[1] + jobs )
      for lfn, ( reason, jobs ) in lfnDict.iteritems():
        failedLfns[( lfn, reason )] = jobs

      for ( lfn, reason ), jobs in failedLfns.iteritems():
        jobs = sorted( set( jobs ) )
        res = monitoring.getJobsSites( jobs )
        if res['OK']:
          sites = sorted( set( val['Site'] for val in res['Value'].itervalues() ) )
        gLogger.notice( "ERROR ==> %s was %s during processing from jobs %s (sites %s): " % ( lfn, reason, ','.join( str( job ) for job in jobs ), ','.join( sites ) ) )
        # Get an example log if possible
        if checkLogs:
          logDump = self.__checkLog( jobLogURL[jobs[0]] )
          prStr = "\tFrom logfile of job %s: " % jobs[0]
          if len( logDump ) == 1:
            prStr += logDump[0]
          else:
            prStr += '\n\t'.join( [''] + logDump )
          gLogger.notice( prStr )
    gLogger.notice( '' )

  def __checkRunsToFlush( self, runID, transFilesList, runStatus, evtType = 90000000, fileTypes = None ):
    """
    Check whether the run is flushed and if not, why it was not
    """
    if not runID:
      gLogger.notice( "Cannot check flush status for run", runID )
      return
    rawFiles = self.pluginUtil.getNbRAWInRun( runID, evtType )
    if not rawFiles:
      gLogger.notice( 'Run %s is not finished...' % runID )
      return
    if 'FileType' in self.transPlugin:
      param = 'FileType'
    elif 'EventType' in self.transPlugin:
      param = 'EventType'
    else:
      param = ''
      paramValues = ['']
    if param:
      res = self.bkClient.getFileMetadata( [fileDict['LFN'] for fileDict in transFilesList] )
      if not res['OK']:
        gLogger.notice( 'Error getting files metadata', res['Message'] )
        DIRAC.exit( 2 )
      evtType = res['Value']['Successful'].values()[0]['EventType']
      if isinstance( fileTypes, ( list, set ) ) and param == 'FileType':
        paramValues = sorted( fileTypes )
      elif evtType and param == 'EventType':
        paramValues = [evtType]
      else:
        paramValues = sorted( set( meta[param] for meta in res['Value']['Successful'].itervalues() if param in meta ) )
    ancestors = {}
    # print "*** Param values", ','.join( paramValues )
    for paramValue in paramValues:
      try:
        nbAnc = self.pluginUtil.getRAWAncestorsForRun( runID, param, paramValue )
        # print '*** For %s = %s: %d ancestors' % ( param, paramValue, nbAnc )
        ancestors.setdefault( nbAnc, [] ).append( paramValue )
      except Exception as e:
        gLogger.exception( "Exception calling pluginUtilities:", lException = e )
    prStr = ''
    for anc in sorted( ancestors ):
      ft = ancestors[anc]
      if ft and ft != ['']:
        prStr += '%d ancestors found for %s; ' % ( anc, ','.join( ft ) )
      else:
        prStr = '%d ancestors found' % anc
    toFlush = False
    flushError = False
    for ancestorRawFiles in ancestors:
      if rawFiles == ancestorRawFiles:
        toFlush = True
      elif ancestorRawFiles > rawFiles:
        flushError = True

    # Missing ancestors, find out which ones
    if not toFlush and not flushError:
      gLogger.notice( "Run %s flushed: %s while %d RAW files" \
                      % ( 'should not be' if runStatus == 'Flush' else 'not', prStr, rawFiles ) )
      # Find out which ones are missing
      res = self.bkClient.getRunFiles( int( runID ) )
      if not res['OK']:
        gLogger.notice( "Error getting run files", res['Message'] )
      else:
        res = self.bkClient.getFileMetadata( sorted( res['Value'] ) )
        if not res['OK']:
          gLogger.notice( "Error getting files metadata", res['Message'] )
        else:
          metadata = res['Value']['Successful']
          runRAWFiles = set( lfn for lfn, meta in metadata.iteritems() if meta['EventType'] == evtType and meta['GotReplica'] == 'Yes' )
          badRAWFiles = set( lfn for lfn, meta in metadata.iteritems() if meta['EventType'] == evtType ) - runRAWFiles
          # print len( runRAWFiles ), 'RAW files'
          allAncestors = set()
          for paramValue in paramValues:
            ancFiles = self.pluginUtil.getRAWAncestorsForRun( runID, param, paramValue, getFiles = True )
            # print paramValue, len( ancFiles )
            allAncestors.update( ancFiles )
          missingFiles = runRAWFiles - allAncestors
          if missingFiles:
            gLogger.notice( "Missing RAW files:\n\t%s" % '\n\t'.join( sorted( missingFiles ) ) )
          else:
            if badRAWFiles:
              gLogger.notice( "Indeed %d RAW files have no replicas and therefore..." % len( badRAWFiles ) )
            else:
              gLogger.notice( "No RAW files are missing in the end and therefore..." )
            rawFiles = len( runRAWFiles )
            toFlush = True
    if toFlush:
      gLogger.notice( "Run %s flushed: %d RAW files and ancestors found" % ( 'correctly' if runStatus == 'Flush' else 'should be', rawFiles ) )
      if runStatus != 'Flush':
        if self.fixIt:
          res = self.transClient.setTransformationRunStatus( self.transID, runID, 'Flush' )
          if res['OK']:
            gLogger.notice( 'Run %d successfully flushed' % runID )
          else:
            gLogger.notice( "Error flushing run %d" % runID, res['Message'] )
        else:
          gLogger.notice( "Use --FixIt to flush the run" )
    if flushError:
      gLogger.notice( "More ancestors than RAW files (%d) for run %d ==> Problem!\n\t%s" \
                      % ( rawFiles, runID, prStr.replace( '; ', '\n\t' ) ) )

  def __checkWaitingTasks( self ):
    """
    Check waiting tasks:
    They can be really waiting (assigned files), Failed, Done or just orphan (no files)
    """
    res = self.transClient.getTransformationTasks( {'TransformationID': self.transID, 'ExternalStatus':'Waiting'} )
    if not res['OK']:
      gLogger.notice( 'Error getting waiting tasks:', res['Message'] )
      return
    tasks = res['Value']
    taskStatuses = {}
    gLogger.notice( 'Found %d waiting tasks' % len( tasks ) )
    for task in tasks:
      fileDicts = self.transClient.getTransformationFiles( {'TransformationID':self.transID, 'TaskID':task['TaskID']} ).get( 'Value', [] )
      if not fileDicts:
        status = 'Orphan'
      else:
        statuses = sorted( set( fileName['Status'] for fileName in fileDicts ) )
        if statuses == ['Processed']:
          status = 'Done'
        elif statuses == ['Failed']:
          status = 'Failed'
        else:
          status = None
      if status:
        taskStatuses.setdefault( status, [] ).append( ( task['TaskID'], int( task['ExternalID'] ) ) )
    if not taskStatuses:
      gLogger.notice( "All tasks look OK" )
      return
    for status in taskStatuses:
      gLogger.notice( '%d tasks are indeed %s' % ( len( taskStatuses[status] ), status ) )
      if self.kickRequests:
        fixed = 0
        ids = taskStatuses[status]
        if status == 'Orphan':
          status = 'Failed'
        for taskID, requestID in ids:
          requestName = self.__getRequestName( requestID )
          if requestName:
            res = self.transClient.setTaskStatus( self.transID, taskID, status )
            if not res['OK']:
              gLogger.notice( "Error setting task %s to %s" % ( requestID, status ), res['Message'] )
            res = self.reqClient.peekRequest( requestID )
            if res['OK']:
              request = res['Value']
              request.Status = status
              res = self.reqClient.putRequest( request )
              if res['OK']:
                fixed += 1
            if not res['OK']:
              gLogger.notice( "Error setting %s to %s" % ( requestID, status ), res['Message'] )
        gLogger.notice( '\t%d requests set to status %s' % ( fixed, status ) )
    if not self.kickRequests:
      gLogger.notice( 'Use --KickRequests to fix them' )


  def debugTransformation( self, dmScript, infoList, statusList ):

    verbose = False
    byFiles = False
    byRuns = False
    byTasks = False
    byJobs = False
    dumpFiles = False
    status = []
    taskList = []
    seList = []
    runList = None
    justStats = False
    fixRun = False
    allTasks = False
    checkFlush = False
    checkWaitingTasks = False
    checkLogs = False
    jobList = []

    switches = Script.getUnprocessedSwitches()
    for opt, val in switches:
      if opt == 'Info' :
        infos = val.split( ',' )
        for val in infos:
          val = val.lower()
          if val not in infoList:
            gLogger.notice( "Unknown information... Select in %s" % str( infoList ) )
            DIRAC.exit( 0 )
          elif val == "files":
            byFiles = True
          elif val == "runs":
            byRuns = True
          elif val == "tasks":
            byTasks = True
          elif val == "jobs":
            byJobs = True
          elif val == "alltasks":
            allTasks = True
          elif val == 'flush':
            byRuns = True
            checkFlush = True
          elif val == 'log':
            checkLogs = True
      elif opt == 'Status':
        status = val.split( ',' )
        val = set( status ) - set( statusList )
        if val:
          gLogger.notice( "Unknown status %s... Select in %s" % ( sorted( val ), str( statusList ) ) )
          DIRAC.exit( 1 )
      elif opt == 'Runs' :
        runList = val.split( ',' )
      elif opt == 'SEs':
        seList = val.split( ',' )
      elif opt in ( 'v', 'Verbose' ):
        verbose = True
      elif opt == 'Tasks':
        taskList = [int( x ) for x in val.split( ',' )]
      elif opt == 'KickRequests':
        self.kickRequests = True
      elif opt == 'DumpFiles':
        dumpFiles = True
      elif opt == 'Statistics':
        justStats = True
      elif opt == 'FixIt':
        self.fixIt = True
      elif opt == 'FixRun':
        fixRun = True
        runList = ['0']
      elif opt == 'CheckWaitingTasks':
        checkWaitingTasks = True
      elif opt == 'Jobs':
        jobList = [int( job ) for job in val.split( ',' ) if job.isdigit()]
        byTasks = True
        byFiles = True


    lfnList = dmScript.getOption( 'LFNs', [] )
    if lfnList:
      byFiles = True
    if dumpFiles:
      byFiles = True
    if allTasks:
      byTasks = True
    if byJobs:
      allTasks = True
      byTasks = False
    if fixRun and not status:
      status = 'Unused'

    transList = self.__getTransformations( Script.getPositionalArgs() ) if not jobList else []

    improperJobs = []
    # gLogger.setLevel( 'INFO' )

    transSep = ''
    if jobList:
      res = self.transClient.getTransformationTasks( {'ExternalID': jobList} )
      if not res['OK']:
        gLogger.notice( "Error getting jobs:", res['Message'] )
      else:
        transList = {}
        for task in res['Value']:
          transList.setdefault( task['TransformationID'], [] ).append( task['TaskID'] )
    for transID in transList:
      self.transID = transID
      if isinstance( transList, dict ):
        taskList = transList[transID]
      problematicReplicas = {}
      failedFiles = []
      nbReplicasProblematic = {}
      taskType, queryFileTypes = self.__getTransformationInfo( transSep )
      if taskType is None:
        continue
      transSep = '==============================\n'
      dmFileStatusComment = {"Replication":"missing", "Removal":"remaining"}.get( self.transType, "absent" )
      if not transID:
        continue
      #####################
      # If just statistics are requested
      if justStats:
        improperJobs += self.__justStats( status, seList )
        continue
      #####################
      # If only checking waiting tasks
      if checkWaitingTasks:
        self.__checkWaitingTasks()
        continue

      self.pluginUtil = PluginUtilities( self.transPlugin, transClient = self.transClient, dataManager = self.dm,
                                    bkClient = self.bkClient, debug = verbose, transID = transID )  ################
      # Select runs, or all
      runsDictList = self.__getRuns( runList, byRuns, seList, status )
      if runList and [run['RunNumber'] for run in runsDictList] == [None]:
        gLogger.notice( "None of the requested runs was found, exit" )
        DIRAC.exit( 0 )
      if status and byRuns and not runList:
        if not runsDictList:
          gLogger.notice( 'No runs found...' )
        else:
          gLogger.notice( '%d runs found: %s' % ( len( runsDictList ), ','.join( str( runDict['RunNumber'] ) for runDict in runsDictList ) ) )
      SEStat = {"Total":0}
      allFiles = []
      toBeKicked = 0

      # Loop over all requested runs or just all in one go (runID == None)
      runsInTable = {}
      for runDict in runsDictList:
        runID = runDict['RunNumber']
        SEs = runDict.get( 'SelectedSite', 'None' ).split( ',' )
        runStatus = runDict.get( 'Status' )

        # Get all files from TransformationDB
        transFilesList = sorted( self.__getFilesForRun( runID = runID, status = status,
                                                   lfnList = lfnList, seList = seList, taskList = taskList ) )
        if jobList and allTasks:
          taskList = []
        if lfnList:
          notFoundFiles = [lfn for lfn in lfnList if lfn not in [fileDict['LFN'] for fileDict in transFilesList]]
          if notFoundFiles:
            gLogger.notice( "Some requested files were not found in transformation (%d):" % len( notFoundFiles ) )
            gLogger.notice( '\n\t'.join( notFoundFiles ) )

        # No files found in transDB
        if not transFilesList:
          if not byRuns:
            gLogger.notice( "No files found with given criteria" )
          continue

        # Run display
        if ( byRuns and runID ) or verbose:
          files, processed = self.__filesProcessed( runID )
          if runID:
            prString = "Run: %d" % runID
          else:
            prString = 'No run'
          if runStatus:
            prString += " (%s)" % runStatus
          prString += " - %d files (SelectedSite: %s), %d processed, status: %s" % ( files, SEs, processed, runStatus )
          gLogger.notice( prString )

        if checkFlush or ( ( byRuns and runID ) and status == 'Unused' and 'WithFlush' in self.transPlugin ) :
          if runStatus != 'Flush':
            # Check if the run should be flushed
            lfn = transFilesList[0]['LFN']
            res = self.pluginUtil.getBookkeepingMetadata( lfn, 'EventType' )
            if res['OK']:
              evtType = res['Value'][lfn]
            else:
              evtType = 90000000
            self.__checkRunsToFlush( runID, transFilesList, runStatus, evtType = evtType, fileTypes = queryFileTypes )
          else:
            gLogger.notice( 'Run %d is already flushed' % runID )

        prString = "%d files found" % len( transFilesList )
        if status:
          prString += " with status %s" % status
        if runID:
          prString += ' in run %d' % runID
        gLogger.notice( prString + '\n' )

        # Extract task list
        filesWithRunZero = []
        filesWithNoRunTable = []
        problematicFiles = []
        taskDict = {}
        for fileDict in transFilesList:
          if not allTasks:
            taskDict.setdefault( fileDict['TaskID'], [] ).append( fileDict['LFN'] )
            if 'Problematic' in status and not fileDict['TaskID']:
              problematicFiles.append( fileDict['LFN'] )
          else:
            # Get all tasks associated to that file
            res = self.transClient.getTableDistinctAttributeValues( 'TransformationFileTasks', ['TaskID'], {'TransformationID': transID, 'FileID' : fileDict['FileID']} )
            if not res['OK']:
              gLogger.notice( "Error when getting tasks for file %s" % fileDict['LFN'] )
            else:
              for taskID in res['Value']['TaskID']:
                taskDict.setdefault( taskID, [] ).append( fileDict['LFN'] )
          fileRun = fileDict['RunNumber']
          fileLfn = fileDict['LFN']
          if byFiles and not taskList:
            gLogger.notice( "%s - Run: %s - Status: %s - UsedSE: %s - ErrorCount %s" % ( fileLfn, fileRun, fileDict['Status'], fileDict['UsedSE'], fileDict['ErrorCount'] ) )
          if not fileRun and '/MC' not in fileLfn:
            filesWithRunZero.append( fileLfn )
          if fileRun:
            runInTable = runsInTable.get( fileRun )
            if not runInTable:
              runInTable = self.__getRuns( [str( fileRun )], True )[0].get( 'RunNumber' )
              runsInTable[fileRun] = runInTable
            if not runInTable:
              filesWithNoRunTable.append( fileLfn )

        # Files with run# == 0
        transWithRun = self.transType != 'Removal' and \
                       self.transPlugin not in ( 'LHCbStandard', 'ReplicateDataset', 'ArchiveDataset', 'LHCbMCDSTBroadcastRandom', 'ReplicateToLocalSE', 'BySize', 'Standard' )
        if filesWithRunZero and transWithRun:
          self.__fixRunNumber( filesWithRunZero, fixRun )
        if filesWithNoRunTable and transWithRun:
          self.__fixRunNumber( filesWithNoRunTable, fixRun, noTable = True )

        # Problematic files
        if problematicFiles:
          self.__checkReplicasForProblematic( problematicFiles, self.__getReplicas( problematicFiles ), nbReplicasProblematic, problematicReplicas )

        # Check files with missing FC
        if status:
          self.__checkFilesMissingInFC( transFilesList, status )

        ####################
        # Now loop on all tasks
        jobsForLfn = {}
        if verbose:
          gLogger.notice( "Tasks:", ','.join( str( taskID ) for taskID in sorted( taskDict ) ) )
        for taskID in sorted( taskList ) if taskList else sorted( taskDict ):
          if taskID not in taskDict:
            gLogger.notice( 'Task %s not found in the transformation files table' % taskID )
            lfnsInTask = []
          else:
            lfnsInTask = taskDict[taskID]
          task = self.__getTask( taskID )
          if not task:
            continue
          # Analyse jobs
          if byJobs and taskType == 'Job':
            job = task['ExternalID']
            lfns = set( lfnsInTask if lfnsInTask else [''] ) & set( fileDict['LFN'] for fileDict in transFilesList )
            jobsForLfn.setdefault( ','.join( sorted( lfns ) ), [] ).append( job )
            if not byFiles and not byTasks:
              continue
          nfiles = len( lfnsInTask )
          allFiles += lfnsInTask
          if self.transType in self.dmTransTypes:
            replicas = self.__getReplicas( lfnsInTask )
          else:
            replicas = {}
          targetSE = task.get( 'TargetSE', None )
          # Accounting per SE
          listSEs = targetSE.split( ',' )
          # If a list of LFNs is provided, we may not have all files in the task, set to False
          taskCompleted = not lfnList

          # Check problematic files
          if 'Problematic' in status:
            self.__checkReplicasForProblematic( lfnsInTask, replicas, nbReplicasProblematic, problematicReplicas )

          # Collect statistics per SE
          for lfn in replicas:
            taskCompleted = self.__fillStatsPerSE( SEStat, replicas[lfn], listSEs ) and taskCompleted

          # Print out task's information
          if byTasks:
            # print task
            prString = "TaskID: %s (created %s, updated %s) - %d files" % ( taskID, task['CreationTime'], task['LastUpdateTime'], nfiles )
            if byFiles and lfnsInTask:
              sep = ',' if sys.stdout.isatty() else '\n'
              prString += " (" + sep.join( lfnsInTask ) + ")"
            prString += "- %s: %s - Status: %s" % ( taskType, task['ExternalID'], task['ExternalStatus'] )
            if targetSE:
              prString += " - TargetSE: %s" % targetSE
            gLogger.notice( prString )

            # More information from Request tasks
            if taskType == "Request":
              toBeKicked += self.__printRequestInfo( task, lfnsInTask, taskCompleted, status, dmFileStatusComment )

            gLogger.notice( "" )
        if byJobs and jobsForLfn:
          self.__checkJobs( jobsForLfn, byFiles, checkLogs )
      if 'Problematic' in status and nbReplicasProblematic:
        self.__checkProblematicFiles( nbReplicasProblematic, problematicReplicas, failedFiles )
      if toBeKicked:
        if self.kickRequests:
          gLogger.notice( "%d requests have been kicked" % toBeKicked )
        else:
          gLogger.notice( "%d requests are eligible to be kicked (use option --KickRequests)" % toBeKicked )

      ###########
      # Print out statistics of SEs if relevant (DMS)
      if SEStat["Total"] and self.transType in self.dmTransTypes:
        gLogger.notice( "%d files found in tasks" % SEStat["Total"] )
        SEStat.pop( "Total" )
        if None in SEStat:
          gLogger.notice( "Found without replicas: %d files" % SEStat[None] )
          SEStat.pop( None )
        gLogger.notice( "Statistics per %s SE:" % dmFileStatusComment )
        SEs = SEStat.keys()
        SEs.sort()
        found = False
        for se in SEs:
          gLogger.notice( "%s %d files" % ( se, SEStat[se] ) )
          found = True
        if not found:
          gLogger.notice( "... None ..." )
      elif self.transType == "Removal" and ( not status or not ( 'MissingLFC' in status or 'MissingInFC' in status ) ):
        gLogger.notice( "All files have been successfully removed!" )

      # All files?
      if dumpFiles and allFiles:
        gLogger.notice( "List of files found:" )
        gLogger.notice( "\n".join( allFiles ) )

    if improperJobs:
      gLogger.notice( "List of %d jobs in improper status:" % len( improperJobs ) )
      gLogger.notice( ' '.join( str( j ) for j in sorted( improperJobs ) ) )
