#!/usr/bin/env python
"""
Debug files status for a (list of) transformations
It is possible to do minor fixes to those files, using options
"""

__RCSID__ = "$transID: dirac-transformation-debug.py 61232 2013-01-28 16:29:21Z phicharp $"

import sys
from LHCbDIRAC.DataManagementSystem.Client.DMScript import DMScript

def __getFilesForRun( transID, runID = None, status = None, lfnList = None, seList = None ):
  #print transID, runID, status, lfnList
  selectDict = {}
  if runID != None:
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
  selectDict['TransformationID'] = transID
  res = transClient.getTransformationFiles( selectDict )
  if res['OK']:
    return res['Value']
  else:
    print "Error getting TransformationFiles:", res['Message']
  return []

def __filesProcessed( transID, runID ):
  transFilesList = __getFilesForRun( transID, runID, None )
  files = 0
  processed = 0
  for fileDict in transFilesList:
      files += 1
      if fileDict['Status'] == "Processed":
          processed += 1
  return ( files, processed )

def __getRuns( transID, runList, byRuns, seList, status = None ):
  runs = []
  if status and byRuns and not runList:
    files = __getFilesForRun( transID, status = status )
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
          for run in range( int( runRange[0] ), int( runRange[1] ) + 1 ):
            runs.append( run )
      selectDict = {'TransformationID':transID, 'RunNumber': runs}
      if runs == [0]:
        runs = [{'RunNumber':0}]
      else:
        if seList:
          selectDict['SelectedSite'] = seList
        res = transClient.getTransformationRuns( selectDict )
        if res['OK']:
          if not len( res['Value'] ):
            print "No runs found, set to 0"
            runs = [{'RunNumber':None}]
          else:
            runs = res['Value']
  elif not byRuns:
    # No run selection
    runs = [{'RunNumber': None}]
  else:
    # All runs selected explicitly
    selectDict = {'TransformationID':transID}
    if seList:
      selectDict['SelectedSite'] = seList
    res = transClient.getTransformationRuns( selectDict )
    if res['OK']:
      if not len( res['Value'] ):
        print "No runs found, set to 0"
        runs = [{'RunNumber':None}]
      else:
        runs = res['Value']
  return runs

def __justStats( transID, status, seList ):
  improperJobs = []
  if not status:
    status = "Assigned"
  transFilesList = __getFilesForRun( transID, None, status, [], seList )
  if not transFilesList:
    return improperJobs
  statsPerSE = {}
  #print transFilesList
  statusList = [ 'Received', 'Checking', 'Staging', 'Waiting', 'Running', 'Stalled']
  if status == 'Processed':
    statusList += [ 'Done', 'Completed', 'Failed']
  taskList = [fileDict['TaskID'] for fileDict in transFilesList]
  res = transClient.getTransformationTasks( {'TransformationID':transID, "TaskID":taskList} )
  if not res['OK']:
    print "Could not get the list of tasks (%s)..." % res['Message']
    DIRAC.exit( 2 )
  for task in res['Value']:
    #print task
    targetSE = task['TargetSE']
    stat = task['ExternalStatus']
    if stat not in statusList:
      statusList.append( stat )
    statsPerSE[targetSE][stat] = statsPerSE.setdefault( targetSE, dict.fromkeys( statusList, 0 ) ).setdefault( stat, 0 ) + 1
    if status == 'Processed' and stat not in ( 'Done', 'Completed', 'Stalled', 'Failed', 'Killed', 'Running' ):
      improperJobs.append( task['ExternalID'] )

  shift = 0
  for se in statsPerSE:
    shift = max( shift, len( se ) + 2 )
  prString = 'SE'.ljust( shift )
  for stat in statusList:
    prString += stat.ljust( 10 )
  print prString
  for se in sorted( statsPerSE ):
    prString = se.ljust( shift )
    for stat in statusList:
      prString += str( statsPerSE[se].get( stat, 0 ) ).ljust( 10 )
    print prString
  return improperJobs

def __getTransformationInfo( transID, transSep ):
  res = transClient.getTransformation( transID, extraParams = False )
  if not res['OK']:
    print "Couldn't find transformation", transID
    return None, None, None, None
  else:
    transName = res['Value']['TransformationName']
    transStatus = res['Value']['Status']
    transType = res['Value']['Type']
    transBody = res['Value']['Body']
    transPlugin = res['Value']['Plugin']
    strPlugin = transPlugin
    if transType in ( 'Merge', 'DataStripping', 'MCStripping' ):
      strPlugin += ', GroupSize: %s' % str( res['Value']['GroupSize'] )
    if transType in dmTransTypes:
      taskType = "Request"
    else:
      taskType = "Job"
    transGroup = res['Value']['TransformationGroup']
  print transSep, "Transformation", \
        transID, "(%s) :" % transStatus, transName, "of type", transType, "(plugin %s)" % strPlugin, "in", transGroup
  if transType == 'Removal':
    print "Transformation body:", transBody
  res = transClient.getBookkeepingQueryForTransformation( transID )
  if res['OK'] and res['Value']:
    print "BKQuery:", res['Value']
    queryProduction = res['Value'].get( 'ProductionID', res['Value'].get( 'Production' ) )
  else:
    print "No BKQuery for this transformation"
    queryProduction = None
  print ""
  return transID, transType, taskType, queryProduction, transPlugin

def __fixRunZero( filesWithRunZero, fixRun ):
  if not fixRun:
    print '%d files have run number 0, use --FixRun to get this fixed' % len( filesWithRunZero )
  else:
    fixedFiles = 0
    res = bkClient.getFileMetadata( filesWithRunZero )
    if res['OK']:
      runFiles = {}
      for lfn, metadata in res['Value']['Successful'].items():
        runFiles.setdefault( metadata['RunNumber'], [] ).append( lfn )
      for run in runFiles:
        if not run:
          print "%d files found in BK with run '%s': %s" % ( len( runFiles[run] ), str( run ), str( runFiles[run] ) )
          continue
        res = transClient.addTransformationRunFiles( transID, run, runFiles[run] )
        # print run, runFiles[run], res
        if not res['OK']:
          print "***ERROR*** setting %d files to run %d in transformation %d: %s" % ( len( runFiles[run] ), run, transID, res['Message'] )
        else:
          fixedFiles += len( runFiles[run] )
      if fixedFiles:
        print "Successfully fixed run number for %d files" % fixedFiles
      else:
        print "There were no files for which to fix the run number"
    else:
      print "***ERROR*** getting metadata for %d files: %s" % ( len( filesWithRunZero ), res['Message'] )

def __getTransformations( args ):
  if not len( args ):
    print "Specify transformation number..."
    Script.showHelp()
  else:
      ids = args[0].split( "," )
      transList = []
      for transID in ids:
          r = transID.split( ':' )
          if len( r ) > 1:
              for i in range( int( r[0] ), int( r[1] ) + 1 ):
                  transList.append( i )
          else:
              transList.append( int( r[0] ) )
  return transList

def __checkFilesMissingInFC( transFilesList, status, fixIt ):
  if 'MissingLFC' in status or 'MissingInFC' in status:
    lfns = [fileDict['LFN'] for fileDict in transFilesList]
    res = rm.getReplicas( lfns )
    if res['OK']:
      replicas = res['Value']['Successful']
      notMissing = len( [lfn for lfn in lfns if lfn in replicas] )
      if notMissing:
        if not kickRequests:
          print "%d files are %s but indeed are in the LFC - Use --KickRequests to reset them Unused" % ( notMissing, status )
        else:
          res = transClient.setFileStatusForTransformation( transID, 'Unused', [lfn for lfn in lfns if lfn in replicas] )
          if res['OK']:
            print "%d files were %s but indeed are in the LFC - Reset to Unused" % ( notMissing, status )
      else:
        res = bkClient.getFileMetadata( lfns )
        if not res['OK']:
          print "ERROR getting metadata from BK"
        else:
          metadata = res['Value']['Successful']
          lfnsWithReplicaFlag = [lfn for lfn in metadata if metadata[lfn]['GotReplica'] == 'Yes']
          if lfnsWithReplicaFlag:
            print "All files are really missing in LFC"
            if not fixIt:
              print '%d files are not in the LFC but have a replica flag in BK, use --FixIt to fix' % len( lfnsWithReplicaFlag )
            else:
              res = bkClient.removeFiles( lfnsWithReplicaFlag )
              if not res['OK']:
                print "ERROR removing replica flag:", res['Message']
              else:
                print "Replica flag removed from %d files" % len( lfnsWithReplicaFlag )
          else:
            print "All files are really missing in LFC and BK"

def __getReplicas( transType, lfns ):
  replicas = {}
  if transType in dmTransTypes:
    for lfnChunk in breakListIntoChunks( lfns, 200 ):
      res = rm.getReplicas( lfnChunk )
      if res['OK']:
        replicas.update( res['Value']['Successful'] )
  return replicas

def __getTask( transID, taskID ):
  res = transClient.getTransformationTasks( {'TransformationID':transID, "TaskID":taskID} )
  if not res['OK'] or not res['Value']:
    return None
  return res['Value'][0]

def __fillStatsPerSE( rep, listSEs ):
  SEStat["Total"] += 1
  completed = True
  if not rep:
    SEStat[None] = SEStat.setdefault( None, 0 ) + 1
  for se in listSEs:
    if transType == "Replication":
      if se not in rep:
        SEStat[se] = SEStat.setdefault( se, 0 ) + 1
        completed = False
    elif transType == "Removal":
      if se in rep:
        SEStat[se] = SEStat.setdefault( se, 0 ) + 1
        completed = False
    else:
      if se not in rep:
        SEStat[se] = SEStat.setdefault( se, 0 ) + 1
  return completed

def __printRequestInfo( transID, task, lfnsInTask, taskCompleted, status, kickRequests ):
  requestID = int( task['ExternalID'] )
  res = reqClient.getRequestInfo( requestID )
  if res['OK']:
    requestName = res['Value'][2]
  else:
    requestName = None
  if not taskCompleted and task['ExternalStatus'] == 'Failed':
    if kickRequests:
      res = transClient.setFileStatusForTransformation( transID, 'Unused', lfnsInTask )
      if res['OK']:
        print "Task is failed: %d files reset Unused" % len( lfnsInTask )
    else:
      print "Task is failed: %d files could be reset Unused: use --KickRequests option" % len( lfnsInTask )
  if taskCompleted and ( task['ExternalStatus'] != 'Done' or status == 'Assigned' ):
    prString = "Task %s is completed: no %s replicas" % ( requestName, dmFileStatusComment )
    if kickRequests:
      if requestName:
        res = reqClient.setRequestStatus( requestName, 'Done' )
        if res['OK']:
          prString += ": request set to Done"
        else:
          prString += ": error setting request to Done (%s)" % res['Message']
      res = transClient.setFileStatusForTransformation( transID, 'Processed', lfnsInTask )
      if res['OK']:
        prString += " - %d files set Processed" % len( lfnsInTask )
      else:
        prString += " - Failed to set %d files Processed (%s)" % ( len( lfnsInTask ), res['Message'] )
    else:
        prString += " - To mark them done, use option --KickRequests"
    print prString
  res = reqClient.getRequestFileStatus( requestID, lfnsInTask )
  if res['OK']:
    reqFiles = res['Value']
    statFiles = {}
    for lfn, stat in reqFiles.items():
      statFiles[stat] = statFiles.setdefault( stat, 0 ) + 1
    stats = statFiles.keys()
    stats.sort()
    for stat in stats:
      print "%s: %d files" % ( stat, statFiles[stat] )
    if 'Failed' in stats and statFiles['Failed'] == len( reqFiles ):
      prString = "All transfers failed for that request"
      if not kickRequests:
        prString += ": it should be marked as Failed, use --KickRequests"
      else:
        failedFiles += reqFiles.keys()
        res = reqClient.setRequestStatus( requestName, 'Failed' )
        if res['OK']:
          prString += ": request set to Failed"
      print prString
  selectDict = { 'RequestID':requestID}
  toBeKicked = 0
  res = reqClient.getRequestSummaryWeb( selectDict, [], 0, 100000 )
  if res['OK']:
    params = res['Value']['ParameterNames']
    records = res['Value']['Records']
    for rec in records:
      subReqDict = {}
      subReqStr = ''
      conj = ''
      for i in range( len( params ) ):
        subReqDict.update( { params[i]:rec[i] } )
        subReqStr += conj + params[i] + ': ' + rec[i]
        conj = ', '

      if subReqDict['Status'] == 'Assigned' and subReqDict['LastUpdateTime'] < str( assignedReqLimit ):
        print subReqStr
        toBeKicked += 1
        if kickRequests:
          res = reqClient.setRequestStatus( subReqDict['RequestName'], 'Waiting' )
          if res['OK']:
            print 'Request %d reset Waiting' % requestID
  return toBeKicked

def __checkProblematicFiles( transID, nbReplicasProblematic, problematicReplicas, failedFiles, fixIt ):
  print "\nStatistics for Problematic files in FC:"
  existingReplicas = {}
  lfns = []
  lfnsInFC = []
  for n in sorted( nbReplicasProblematic ):
    print "   %d replicas in FC: %d files" % ( n, nbReplicasProblematic[n] )
  gLogger.setLevel( 'FATAL' )
  for se in problematicReplicas:
    lfns += [lfn for lfn in problematicReplicas[se] if lfn not in lfns]
    if se:
      lfnsInFC += [lfn for lfn in problematicReplicas[se] if lfn not in lfnsInFC]
      res = rm.getReplicaMetadata( problematicReplicas[se], se )
      if res['OK']:
        for lfn in res['Value']['Successful']:
          existingReplicas.setdefault( lfn, [] ).append( se )
  nbProblematic = len( lfns ) - len( existingReplicas )
  nbExistingReplicas = {}
  for lfn in existingReplicas:
    nbReplicas = len( existingReplicas[lfn] )
    nbExistingReplicas[nbReplicas] = nbExistingReplicas.setdefault( nbReplicas, 0 ) + 1
  nonExistingReplicas = {}
  if nbProblematic == len( lfns ):
      print "None of the %d problematic files actually have a replica" % len( lfns )
  else:
    strMsg = "Out of %d problematic files" % len( lfns )
    if nbProblematic:
      strMsg += ", only %d do not have a physical replica" % nbProblematic
    else:
      strMsg += ", all have a physical replica"
    print strMsg
    for n in sorted( nbExistingReplicas ):
      print "   %d physical replicas: %d files" % ( n, nbExistingReplicas[n] )
    for se in problematicReplicas:
      lfns = [lfn for lfn in problematicReplicas[se] if lfn not in existingReplicas or se not in existingReplicas[lfn]]
      str2Msg = ''
      if len( lfns ):
        nonExistingReplicas.setdefault( se, [] ).extend( lfns )
        if not fixIt:
          str2Msg = ' Use --FixIt to remove them'
        else:
          str2Msg = ' Will be removed from FC'
        strMsg = '%d' % len( lfns )
      else:
        strMsg = 'none'
      print "   %s : %d replicas of problematic files in FC, %s physically missing.%s" % ( se.ljust( 15 ), len( problematicReplicas[se] ), strMsg, str2Msg )
    lfns = [lfn for lfn in existingReplicas if lfn in failedFiles]
    if lfns:
      prString = "Failed transfers but existing replicas"
      if fixIt:
        prString += '. Use --FixIt to fix it'
      else:
        for lfn in lfns:
          res = transClient.setFileStatusForTransformation( transID, 'Unused', lfns )
          if res['OK']:
            prString += " - %d files reset Unused" % len( lfns )
      print prString
  filesInFCNotExisting = [lfn for lfn in lfnsInFC if lfn not in existingReplicas]
  if filesInFCNotExisting:
    prString = '%d files are in the FC but are not physically existing. ' % len( filesInFCNotExisting )
    if fixIt:
      prString += 'Removing them now from FC...'
    else:
      prString += 'Use --FixIt to remove them'
    print prString
    if fixIt:
      res = rm.removeFile( filesInFCNotExisting )
      if res['OK']:
        print "Successfully removed %d files from FC" % len( filesInFCNotExisting )
      else:
        print "ERROR when removing files from FC:", res['Message']
  if fixIt and nonExistingReplicas:
    nRemoved = 0
    failures = {}
    for se in nonExistingReplicas:
      lfns = [lfn for lfn in nonExistingReplicas[se] if lfn not in filesInFCNotExisting]
      res = rm.removeReplica( se, lfns )
      if not res['OK']:
        print "ERROR when removing replicas from FC at %s" % se, res['Message']
      else:
        failed = res['Value']['Failed']
        if failed:
          print "Failed to remove %d replicas at %s" % ( len( failed ), se )
          print '\n'.join( sorted( failed ) )
          for lfn in failed:
            failures.setdefault( failed[lfn], [] ).append( lfn )
        nRemoved += len( res['Value']['Successful'] )
    print "Successfully removed %s replicas from FC" % nRemoved
    if failures:
      print "Failures:"
      for error in failures:
        print "%s: %d replicas" % ( error, len( failures[error] ) )
  print ""

def __checkReplicasForProblematic( lfns, replicas ):
  for lfn in lfns:
    # Problematic files, let's see why
    realSEs = [se for se in replicas.get( lfn, [] ) if not se.endswith( '-ARCHIVE' )]
    nbSEs = len( realSEs )
    nbReplicasProblematic[nbSEs] = nbReplicasProblematic.setdefault( nbSEs, 0 ) + 1
    if not nbSEs:
      problematicReplicas.setdefault( None, [] ).append( lfn )
    for se in realSEs:
      problematicReplicas.setdefault( se, [] ).append( lfn )

def __checkJobs( jobsForLfn ):
  from DIRAC.Core.DISET.RPCClient                          import RPCClient
  monitoring = RPCClient( 'WorkloadManagement/JobMonitoring' )
  for lfn, jobs in jobsForLfn.items():
    jobs.sort()
    res = monitoring.getJobsStatus( jobs )
    if res['OK']:
      jobStatus = res['Value']
      res = monitoring.getJobsMinorStatus( jobs )
      if res['OK']:
        jobMinorStatus = res['Value']
        res = monitoring.getJobsApplicationStatus( jobs )
        if res['OK']:
          jobApplicationStatus = res['Value']
    if not res['OK']:
      print 'Error getting jobs statuses:', res['Message']
      return
    allStatus = {}
    for job in [int( j ) for j in jobs]:
      status = jobStatus.get( job, {} ).get( 'Status', 'Unknown' ) + '; ' + \
               jobMinorStatus.get( job, {} ).get( 'MinorStatus', 'Unknown' ) + '; ' + \
               jobApplicationStatus.get( job, {} ).get( 'ApplicationStatus', 'Unknown' )
      allStatus[job] = status
    print '\nLFN:', lfn, ': Status of corresponding %d jobs (ordered):' % len( jobs )
    print ' '.join( jobs )
    prevStatus = None
    allStatus[sys.maxint] = ''
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
      print prStr, prevStatus
      majorStatus, minorStatus, applicationStatus = prevStatus.split( '; ' )
      if majorStatus == 'Failed' and minorStatus == 'Job stalled: pilot not running':
        lastLine = ''
        # Now get last lines
        res = monitoring.getJobsSites( jobs )
        if res['OK']:
          jobSites = res['Value']
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
          print '\t%3d jobs' % len( jobs ), 'stalled with last line:', lastLine
          lastLine = line
          jobs = [job1]
      jobs = [job]
      prevStatus = status
  print ''


#====================================
if __name__ == "__main__":

  transSep = ''
  verbose = False
  byFiles = False
  byRuns = False
  byTasks = False
  byJobs = False
  dumpFiles = False
  status = None
  lfnList = []
  taskList = []
  seList = []
  runList = None
  kickRequests = False
  justStats = False
  fixRun = False
  allTasks = False
  fixIt = False
  from DIRAC.Core.Base import Script

  infoList = ( "files", "runs", "tasks", 'jobs', 'alltasks' )
  statusList = ( "Unused", "Assigned", "Done", "Problematic", "MissingLFC", "MissingInFC", "MaxReset", "Processed", "NotProcessed", "Removed" )
  dmScript = DMScript()
  dmScript.registerFileSwitches()
  Script.registerSwitch( '', 'Info=', "Specify what to print out from %s" % str( infoList ) )
  Script.registerSwitch( '', 'Status=', "Select files with a given status from %s" % str( statusList ) )
  Script.registerSwitch( '', 'Runs=', "Specify a (list of) runs" )
  Script.registerSwitch( '', 'SEs=', 'Specify a (list of) target SEs' )
  Script.registerSwitch( '', 'Tasks=', "Specify a (list of) tasks" )
  Script.registerSwitch( '', 'DumpFiles', 'Dump the list of LFNs on stdout' )
  Script.registerSwitch( '', 'Statistics', 'Get the statistics of tasks per status and SE' )
  Script.registerSwitch( '', 'FixRun', 'Fix the run number in transformation table' )
  Script.registerSwitch( '', 'FixIt', 'Fix the FC' )
  Script.registerSwitch( '', 'KickRequests', 'Reset old Assigned requests to Waiting' )
  Script.registerSwitch( 'v', 'Verbose', '' )
  Script.setUsageMessage( '\n'.join( [ __doc__,
                                       'Usage:',
                                       'dirac-transformation-debug [options] transID[,transID2[,transID3[,...]]]'] ) )


  Script.parseCommandLine( ignoreErrors = True )
  import DIRAC
  #from DIRAC.ConfigurationSystem.Client import PathFinder
  from DIRAC.Core.DISET.RPCClient import RPCClient

  switches = Script.getUnprocessedSwitches()
  for opt, val in switches:
    if opt == 'Info' :
      infos = val.split( ',' )
      for val in infos:
        val = val.lower()
        if val not in infoList:
          print "Unknown information... Select in %s" % str( infoList )
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
    elif opt == 'Status':
      if val not in statusList:
        print "Unknown status %s... Select in %s" % ( val, str( statusList ) )
        Script.showHelp()
      status = val
      if status in ( "MissingLFC", "MissingInFC" ):
        status = ["MissingLFC", "MissingInFC"]
    elif opt == 'Runs' :
      runList = val.split( ',' )
    elif opt == 'SEs':
      seList = val.split( ',' )
    elif opt in ( 'v', 'Verbose' ):
      verbose = True
    elif opt == 'Tasks':
      taskList = val.split( ',' )
    elif opt == 'KickRequests':
      kickRequests = True
    elif opt == 'DumpFiles':
      dumpFiles = True
    elif opt == 'Statistics':
      justStats = True
    elif opt == 'FixIt':
      fixIt = True
    elif opt == 'FixRun':
      fixRun = True
      runList = ['0']
      status = 'Unused'

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

  transList = __getTransformations( Script.getPositionalArgs() )

  from LHCbDIRAC.TransformationSystem.Client.TransformationClient           import TransformationClient
  from DIRAC.RequestManagementSystem.Client.RequestClient           import RequestClient
  from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager
  from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient  import BookkeepingClient
  from LHCbDirac.TransformationSystem.Client.Utilities import PluginUtilities
  from DIRAC.Core.Utilities.List                                         import breakListIntoChunks
  from DIRAC import gLogger
  import DIRAC
  import datetime

  bkClient = BookkeepingClient()
  transClient = TransformationClient()
  reqClient = RequestClient()
  rm = ReplicaManager()
  dmTransTypes = ( "Replication", "Removal" )
  assignedReqLimit = datetime.datetime.utcnow() - datetime.timedelta( hours = 2 )
  improperJobs = []
  pluginUtil = None

  transSep = ''
  for transID in transList:
    transID, transType, taskType, queryProduction, transPlugin = __getTransformationInfo( transID, transSep )
    transSep = '==============================\n'
    dmFileStatusComment = {"Replication":"missing", "Removal":"remaining"}.get( transType, "absent" )
    if not transID:
      continue
    #####################
    # If just statistics are requested
    if justStats:
      improperJobs += __justStats( transID, status, seList )
      continue
    ################
    # Select runs, or all
    runsDictList = __getRuns( transID, runList, byRuns, seList, status )
    SEStat = {"Total":0}
    allFiles = []
    toBeKicked = 0
    jobList = []

    # Loop over all requested runs or just all in one go (runID == None)
    for runDict in runsDictList:
      runID = runDict['RunNumber']
      SEs = runDict.get( 'SelectedSite', 'None' ).split( ',' )
      runStatus = runDict.get( 'Status' )

      # Get all files from TransformationDB
      transFilesList = sorted( __getFilesForRun( transID, runID, status, lfnList, seList ) )
      if lfnList:
        notFoundFiles = [lfn for lfn in lfnList if lfn not in [fileDict['LFN'] for fileDict in transFilesList]]
        if notFoundFiles:
          print "Some requested files were not found in transformation (%d):" % len( notFoundFiles )
          print '\n\t'.join( notFoundFiles )

      # No files found in transDB
      if not transFilesList:
        if not byRuns:
          print "No files found with given criteria"
        continue

      # Run display
      if ( byRuns and runID ) or verbose:
          files, processed = __filesProcessed( transID, runID )
          prString = "Run: %d" % runID
          if runStatus:
            prString += " (%s)" % runStatus
          prString += " - %d files (SelectedSite: %s), %d processed, status: %s" % ( files, SEs, processed, runStatus )
          print prString

      if ( byRuns and runID ) and status == 'Unused' and 'WithFlush' in transPlugin and runStatus != 'Flush':
        # Check if the run should be flushed
        if not pluginUtil:
          pluginUtil = PluginUtilities( transPlugin, transClient, rm, bkClient, None, None, False, {}, transID = transID )
        evtType = 90000000
        rawFiles = pluginUtil.getNbRAWInRun( runID, evtType )
        if 'FileType' in transPlugin:
          param = 'FileType'
        elif 'EventType' in transPlugin:
          param = 'EventType'
        else:
          param = ''
          paramValues = ['']
        if param:
          res = bkClient.getFileMetadata( [fileDict['LFN'] for fileDict in transFilesList] )
          if not res['OK']:
            print 'Error getting files metadata', res['Message']
            DIRAC.exit( 2 )
          paramValues = []
          for lfn in res['Value']['Successful']:
            val = res['Value']['Successful'][lfn].get( param )
            if val and val not in paramValues:
              paramValues.append( val )
        ancestors = {}
        for paramValue in paramValues:
          try:
            ancestors.setdefault( pluginUtil.getRAWAncestorsForRun( runID, param, paramValue ), [] ).append( paramValue )
          except Exception, e:
            print "Exception calling pluginUtilities:", e
        prStr = ''
        for anc in sorted( ancestors ):
          ft = ancestors[anc]
          if ft:
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
        if toFlush:
          print "Run should be flushed: %d RAW files and ancestors found" % ( rawFiles )
          if fixIt:
            res = transClient.setTransformationRunStatus( transID, runID, 'Flush' )
            if res['OK']:
              print 'Run %d successfully flushed' % runID
            else:
              print "Error flushing run %d" % runID, res['Message']
          else:
            print "Use --FixIt to flush the run"
        if flushError:
          print "More ancestors than RAW files (%d) for run %d ==> Problem!\n\t%s" \
            % ( rawFiles, runID, prStr, replace( '; ', '\n\t' ) )
        if not toFlush and not flushError:
          print "Run not flushed: %s while %d RAW files" \
            % ( prStr, rawFiles )

      prString = "%d files found" % len( transFilesList )
      if status:
        prString += " with status %s" % status
      if runID:
        prString += ' in run %d' % runID
      print prString + '\n'

      # Extract task list
      filesWithRunZero = []
      problematicFiles = []
      taskDict = {}
      for fileDict in transFilesList:
        if not allTasks:
          taskDict.setdefault( fileDict['TaskID'], [] ).append( fileDict['LFN'] )
          if status == 'Problematic' and not fileDict['TaskID']:
            problematicFiles.append( fileDict['LFN'] )
        else:
          # Get all tasks associated to that file
          res = transClient.getTableDistinctAttributeValues( 'TransformationFileTasks', ['TaskID'], {'TransformationID': transID, 'FileID' : fileDict['FileID']} )
          if not res['OK']:
            print "Error when getting tasks for file %s" % fileDict['LFN']
          else:
            for taskID in res['Value']['TaskID']:
              taskDict.setdefault( taskID, [] ).append( fileDict['LFN'] )
        if byFiles and not taskList:
            print fileDict['LFN'], "- Run:", fileDict['RunNumber'], "- Status:", fileDict['Status'], "- UsedSE:", fileDict['UsedSE'], "- ErrorCount:", fileDict['ErrorCount']
        if not fileDict['RunNumber'] and fileDict['LFN'].find( '/MC' ) < 0:
          filesWithRunZero.append( fileDict['LFN'] )

      # Files with run# == 0
      if filesWithRunZero:
        __fixRunZero( filesWithRunZero, fixRun )

      # Problematic files
      problematicReplicas = {}
      nbReplicasProblematic = {}
      if problematicFiles:
        __checkReplicasForProblematic( problematicFiles, __getReplicas( transType, problematicFiles ) )

      # Check files with missing LFC
      if status:
        __checkFilesMissingInFC( transFilesList, status, fixIt )

      ####################
      # Now loop on all tasks
      failedFiles = []
      jobsForLfn = {}
      if verbose:
        print "Tasks:", ' '.join( sorted( taskDict ) )
      for taskID in sorted( taskList ) if taskList else sorted( taskDict ):
        if taskID not in taskDict:
          print 'Task %s not found in the transformation files table' % taskID
          lfnsInTask = []
        else:
          lfnsInTask = taskDict[taskID]
        task = __getTask( transID, taskID )
        if not task: continue
        if byJobs and taskType == 'Job':
          job = task['ExternalID']
          jobsForLfn.setdefault( ','.join( taskDict.get( taskID, [''] ) ), [] ).append( job )
          if job not in jobList:
            jobList.append( job )
          if not byFiles and not byTasks:
            continue
        nfiles = len( lfnsInTask )
        allFiles += lfnsInTask
        replicas = __getReplicas( transType, lfnsInTask )
        targetSE = task.get( 'TargetSE', None )
        # Accounting per SE
        listSEs = targetSE.split( ',' )
        taskCompleted = True

        # Check problematic files
        if status == 'Problematic':
          __checkReplicasForProblematic( lfnsInTask, replicas )

        #Collect statistics per SE
        for lfn in replicas:
          taskCompleted = __fillStatsPerSE( replicas[lfn], listSEs ) and taskCompleted

        # Print out task's information
        if byTasks:
          #print task
          prString = "TaskID: %s (created %s, updated %s) - %d files" % ( taskID, task['CreationTime'], task['LastUpdateTime'], nfiles )
          if byFiles and lfnsInTask:
            prString += " (" + str( lfnsInTask ) + ")"
          prString += "- %s: %s - Status: %s" % ( taskType, task['ExternalID'], task['ExternalStatus'] )
          if targetSE:
            prString += " - TargetSE: %s" % targetSE
          print prString

          # More information from Request tasks
          if taskType == "Request":
            toBeKicked = __printRequestInfo( transID, task, lfnsInTask, taskCompleted, status, kickRequests )

          print ""
      if byJobs and jobsForLfn:
        __checkJobs( jobsForLfn )
    if status == 'Problematic':
      __checkProblematicFiles( transID, nbReplicasProblematic, problematicReplicas, failedFiles, fixIt )
    if toBeKicked:
      if kickRequests:
        print "%d requests have been kicked" % toBeKicked
      else:
        print "%d requests are eligible to be kicked (use option --KickRequests)" % toBeKicked

    ###########
    # Print out statistics of SEs if relevant (DMS)
    if SEStat["Total"] and transType in dmTransTypes:
      print "%d files found in tasks" % SEStat["Total"]
      SEStat.pop( "Total" )
      if None in SEStat:
        print "Found without replicas:", SEStat[None], "files"
        SEStat.pop( None )
      print "Statistics per %s SE:" % dmFileStatusComment
      SEs = SEStat.keys()
      SEs.sort()
      found = False
      for se in SEs:
        print se, SEStat[se], "files"
        found = True
      if not found:
        print "... None ..."
    elif transType == "Removal" and ( not status or not ( 'MissingLFC' in status or 'MissingInFC' in status ) ):
      print "All files have been successfully removed!"

    # All files?
    if dumpFiles and allFiles:
      print "List of files found:"
      print "\n".join( allFiles )
      newStatus = None
      if newStatus:
        res = transClient.setFileStatusForTransformation( transID, newStatus, allFiles )
        if res['OK']:
          print 'Of %d files, %d set to %s' % ( len( allFiles ), len( res['Value']['Successful'] ), newStatus )
        else:
          print "Failed to set status %s" % newStatus

  if improperJobs:
    print "List of %d jobs in improper status:" % len( improperJobs )
    print ' '.join( [str( j ) for j in sorted( improperJobs )] )
