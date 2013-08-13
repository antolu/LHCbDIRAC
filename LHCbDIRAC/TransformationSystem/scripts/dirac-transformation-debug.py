#!/usr/bin/env python
"""
Debug files status for a (list of) transformations
It is possible to do minor fixes to those files, using options
"""

__RCSID__ = "$transID: dirac-transformation-debug.py 61232 2013-01-28 16:29:21Z phicharp $"

import sys, os
from LHCbDIRAC.DataManagementSystem.Client.DMScript import DMScript

def __getFilesForRun( transID, runID = None, status = None, lfnList = None, seList = None ):
  # print transID, runID, status, lfnList
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
          print "No runs found, set to None"
          runs = [{'RunNumber':None}]
        else:
          runs = res['Value']
  elif not byRuns:
    # No run selection
    runs = [{'RunNumber': None}]
  elif not status:
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
  # print transFilesList
  statusList = set( [ 'Received', 'Checking', 'Staging', 'Waiting', 'Running', 'Stalled'] )
  if status == 'Processed':
    statusList.update( [ 'Done', 'Completed', 'Failed'] )
  taskList = [fileDict['TaskID'] for fileDict in transFilesList]
  res = transClient.getTransformationTasks( {'TransformationID':transID, "TaskID":taskList} )
  if not res['OK']:
    print "Could not get the list of tasks (%s)..." % res['Message']
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
    return None, None, None, None, None
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
          filesToReset = [lfn for lfn in lfns if lfn in replicas]
          res = transClient.setFileStatusForTransformation( transID, 'Unused', filesToReset, force = True )
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
        print "Task is failed: not all the %d files were reset Unused" % len( lfnsInTask )
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
      if se:
        print "   %s : %d replicas of problematic files in FC, %s physically missing.%s" % ( str( se ).ljust( 15 ), len( problematicReplicas[se] ), strMsg, str2Msg )
      else:
        print "   %s : %d files are not in FC.%s" % ( ''.ljust( 15 ), len( problematicReplicas[se] ), str2Msg )
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
        nRemoved, transRemoved = __removeFilesFromTS( filesInFCNotExisting )
        if nRemoved:
          print 'Successfully removed %d files from transformations %s' % ( nRemoved, ','.join( transRemoved ) )
      else:
        print "ERROR when removing files from FC:", res['Message']
  if fixIt and nonExistingReplicas:
    nRemoved = 0
    failures = {}
    # If SE == None, the file is not in the FC
    notInFC = nonExistingReplicas.get( None )
    if notInFC:
      nonExistingReplicas.pop( None )
      nRemoved, transRemoved = __removeFilesFromTS( notInFC )
      if nRemoved:
        print 'Successfully removed %d files from transformations %s' % ( nRemoved, ','.join( transRemoved ) )
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
    if nRemoved:
      print "Successfully removed %s replicas from FC" % nRemoved
    if failures:
      print "Failures:"
      for error in failures:
        print "%s: %d replicas" % ( error, len( failures[error] ) )
  print ""

def __removeFilesFromTS( lfns ):
  res = transClient.getTransformationFiles( {'LFN':lfns} )
  if not res['OK']:
    print "Error getting %d files in the TS" % len( lfns ), res['Message']
    return
  transFiles = {}
  removed = 0
  for fd in res['Value']:
    transFiles.setdefault( fd['TransformationID'], [] ).append( fd['LFN'] )
  for transID, lfns in transFiles.items():
    res = transClient.setFileStatusForTransformation( transID, 'Removed', lfns, force = True )
    if not res['OK']:
      print 'Error setting %d files Removed' % len( lfns ), res['Message']
    else:
      removed += len( lfns )
  return removed, transFiles.keys()


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

def __getLog( urlBase, logFile, debug = False ):
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
      # If hte line matches the requested URL
      if fnmatch.fnmatch( l, '*' + logFile + '*' ):
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
  if ".tgz" in url:
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
    for file in mn:
      if fnmatch.fnmatch( file, logFile + '*' ):
        if debug: print "Found ", logFile, " in tar object ", file
        if '.gz' in file:
          # file is again a gzip file!!
          tmp1 = os.path.join( os.environ.get( "TMPDIR", "/tmp" ), "logFile-1.tmp" )
          if os.path.exists( tmp1 ):
            os.remove( tmp1 )
          if debug: print "Extract", file, "into", tmp1, "and open it"
          tf.extract( file, tmp1 )
          tmp1 += file
          f = gzip.GzipFile( tmp1, 'r' )
        else:
          f = tf.extractfile( file )
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
      if debug: print "Reading the file now... %d lines" % len( c )
      c = c.split( "\n" )
    else:
      c = ''
  if tf:
    tf.close()
  if tmp:
    os.remove( tmp )
  if tmp1:
    os.remove( tmp1 )
  return c

def __getSandbox( job, logFile, debug = False ):
  from DIRAC.WorkloadManagementSystem.Client.SandboxStoreClient     import SandboxStoreClient
  import fnmatch
  sbClient = SandboxStoreClient()
  tmpDir = os.path.join( os.environ.get( "TMPDIR", "/tmp" ), "sandBoxes/" )
  if not os.path.exists( tmpDir ):
    os.mkdir( tmpDir )
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
  except:
    print 'Exception while getting sandbox'
    return ''
  finally:
    if f:
      f.close()
    for lf in files:
      os.remove( os.path.join( tmpDir, lf ) )
    os.rmdir( tmpDir )


def __checkXMLSummary( job, logURL ):
  xmlFile = __getLog( logURL, 'summary*.xml', debug = False )
  if not xmlFile:
    xmlFile = __getSandbox( job, 'summary*.xml', debug = False )
  lfns = {}
  if xmlFile:
    for line in xmlFile:
      if 'status="part"' in line and 'LFN:' in line:
        lfns.update( {line.split( 'LFN:' )[1].split( '"' )[0] : 'Partial'} )
      elif 'status="fail"' in line and 'LFN:' in line:
        lfns.update( {line.split( 'LFN:' )[1].split( '"' )[0] : 'Failed'} )
    if not lfns:
      lfns = {None:'No errors found in XML summary'}
  return lfns

def __checkJobs( jobsForLfn, byFiles = False ):
  from DIRAC.Core.DISET.RPCClient                          import RPCClient
  monitoring = RPCClient( 'WorkloadManagement/JobMonitoring' )
  failedLfns = {}
  for lfnStr, allJobs in jobsForLfn.items():
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
      print 'Error getting jobs statuses:', res['Message']
      return
    allStatus = {}
    for job in [int( j ) for j in allJobs]:
      status = jobStatus.get( job, {} ).get( 'Status', 'Unknown' ) + '; ' + \
               jobMinorStatus.get( job, {} ).get( 'MinorStatus', 'Unknown' ) + '; ' + \
               jobApplicationStatus.get( job, {} ).get( 'ApplicationStatus', 'Unknown' )
      allStatus[job] = status
    if byFiles or len( lfnList ) < 3:
      print '\n %d LFNs:' % len( lfnList ), lfnList, ': Status of corresponding %d jobs (ordered):' % len( allJobs )
    else:
      print '\n %d LFNs:' % len( lfnList ), 'Status of corresponding %d jobs (ordered):' % len( allJobs )
    print ' '.join( allJobs )
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
      if majorStatus == 'Failed' and 'Exited With Status' in applicationStatus:
        exitedJobs += jobs
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
          for xx in ( 'LbLogin.sh', 'SetupProject.sh' ):
            if xx in lastLine:
              lastLine = lastLine.split()[0] + ' Executing %s' % xx + lastLine.split( xx )[1].split( '&&' )[0]
              break
          print '\t%3d jobs' % len( jobs ), 'stalled with last line:', lastLine
          lastLine = line
          jobs = [job1]
      jobs = [job]
      prevStatus = status
      if exitedJobs:
        badLfns = {}
        for lastJob in sorted( exitedJobs, reverse = True )[0:10]:
          res = monitoring.getJobParameter( int( lastJob ), 'Log URL' )
          if res['OK']:
            logURL = res['Value']['Log URL'].split( '"' )[1] + '/'
            lfns = __checkXMLSummary( lastJob, logURL )
            if lfns:
              badLfns.update( {lastJob: lfns} )
          # break
        if not badLfns:
          print "No logfiles found for any of the jobs..."
        else:
          # lfnsFound is an AND of files found bad in all jobs
          lfnsFound = set( badLfns.values()[0] )
          for lfns in badLfns.values():
            lfnsFound &= set( [lfn for lfn in lfns if lfn] )
          if lfnsFound:
            for lfn, job, reason in [( lfn, job, badLfns[job][lfn] ) for job in badLfns for lfn in set( badLfns[job] ) & lfnsFound]:
              failedLfns.setdefault( ( lfn, reason ), [] ).append( job )
          else:
            print "No error was found in XML summary files"
  if failedLfns:
    print "\nSummary of failures due to: Application Exited with non-zero status"
    for ( lfn, reason ), jobs in failedLfns.items():
      print "ERROR ==> %s was %s during processing from jobs %s: " % ( lfn, reason, jobs )
  print ''

def __checkRunsToFlush( pluginUtils, runID, transFilesList, runStatus, evtType = 90000000 ):
  """
  Check whether the run is flushed and if not, why it was not
  """
  if not runID:
    print "Cannot check flush status for run", runID
    return
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
  if toFlush:
    print "Run %s flushed: %d RAW files and ancestors found" % ( 'correctly' if runStatus == 'Flush' else 'should be', rawFiles )
    if runStatus != 'Flush':
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
      % ( rawFiles, runID, prStr.replace( '; ', '\n\t' ) )
  if not toFlush and not flushError:
    print "Run %s flushed: %s while %d RAW files" \
      % ( 'should not be' if runStatus == 'Flush' else 'not', prStr, rawFiles )


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
  checkFlush = False
  from DIRAC.Core.Base import Script

  infoList = ( "files", "runs", "tasks", 'jobs', 'alltasks', 'flush' )
  statusList = ( "Unused", "Assigned", "Done", "Problematic", "MissingLFC", "MissingInFC", "MaxReset", "Processed", "NotProcessed", "Removed", 'ProbInFC' )
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
  # from DIRAC.ConfigurationSystem.Client import PathFinder
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
        elif val == 'flush':
          byRuns = True
          checkFlush = True
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
  from LHCbDIRAC.TransformationSystem.Client.Utilities import PluginUtilities
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
  gLogger.setLevel( 'INFO' )

  transSep = ''
  problematicReplicas = {}
  nbReplicasProblematic = {}
  failedFiles = []
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
    pluginUtil = PluginUtilities( transPlugin, transClient, rm, bkClient, None, None, verbose, {}, transID = transID )  ################
    # Select runs, or all
    runsDictList = __getRuns( transID, runList, byRuns, seList, status )
    if runList and [run['RunNumber'] for run in runsDictList] == [None]:
      print "None of the requested runs was found, exit"
      DIRAC.exit( 0 )
    if status and byRuns and not runList:
      if not runsDictList:
        print 'No runs found...'
      else:
        print '%d runs found: %s' % ( len( runsDictList ), ','.join( [str( runDict['RunNumber'] ) for runDict in runsDictList] ) )
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

      if checkFlush or ( ( byRuns and runID ) and status == 'Unused' and 'WithFlush' in transPlugin and runStatus != 'Flush' ):
        # Check if the run should be flushed
        __checkRunsToFlush( pluginUtil, runID, transFilesList, runStatus )

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
        if not fileDict['RunNumber'] and '/MC' not in fileDict['LFN']:
          filesWithRunZero.append( fileDict['LFN'] )

      # Files with run# == 0
      if filesWithRunZero and transType != 'Removal':
        __fixRunZero( filesWithRunZero, fixRun )

      # Problematic files
      if problematicFiles:
        __checkReplicasForProblematic( problematicFiles, __getReplicas( transType, problematicFiles ) )

      # Check files with missing LFC
      if status:
        __checkFilesMissingInFC( transFilesList, status, fixIt )

      ####################
      # Now loop on all tasks
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

        # Collect statistics per SE
        for lfn in replicas:
          taskCompleted = __fillStatsPerSE( replicas[lfn], listSEs ) and taskCompleted

        # Print out task's information
        if byTasks:
          # print task
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
        __checkJobs( jobsForLfn, byFiles )
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
