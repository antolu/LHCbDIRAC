#!/usr/bin/env python

__RCSID__ = "$Id$"

import sys

def getFilesForRun( id, runID, status=None, lfnList=None ):
    selectDict = {'TransformationID':id}
    if runID:
        selectDict["RunNumber"] = runID
    if status:
        selectDict['Status'] = status
    if lfnList:
        selectDict['LFN'] = lfnList
    #print selectDict
    res = transClient.getTransformationFiles( selectDict )
    #print res
    if res['OK']:
        return res['Value']
    return []

def filesProcessed( id, runID ):
    filesList = getFilesForRun( id, runID, None )
    files = 0
    processed = 0
    for fileDict in filesList:
        files += 1
        if fileDict['Status'] == "Processed":
            processed += 1
    return ( files, processed )

#====================================
verbose = False
byFiles = False
byRuns = False
byTasks = False
byJobs = False
dumpFiles = False
status = None
lfnList = []
taskList = []
resetRuns = None
runList = None
kickRequests = False
justStats = False
fixIt = False
from DIRAC.Core.Base import Script

infoList = ["Files", "Runs", "Tasks", 'Jobs']
statusList = ["Unused", "Assigned", "Done", "Problematic", "MissingLFC", "MaxReset"]
Script.registerSwitch( 'i:', 'Info=', "Specify what to print out from %s" % str( infoList ) )
Script.registerSwitch( '', 'Status=', "Select files with a given status from %s" % str( statusList ) )
Script.registerSwitch( 'l:', 'LFNs=', "Specify a (list of) LFNs" )
Script.registerSwitch( '', 'Runs=', "Specify a (list of) runs" )
Script.registerSwitch( '', 'Tasks=', "Specify a (list of) tasks" )
Script.registerSwitch( '', 'ResetRuns', "Reset runs in Active status (use with care!)" )
Script.registerSwitch( '', 'KickRequests', 'Reset old Assigned requests to Waiting' )
Script.registerSwitch( '', 'DumpFiles', 'Dump the list of LFNs on stdout' )
Script.registerSwitch( '', 'Statistics', 'Get the statistics of tasks per status and SE' )
Script.registerSwitch( '', 'FixIt', 'Fix the run number in transformation table' )
Script.registerSwitch( 'v', 'Verbose', '' )

Script.parseCommandLine( ignoreErrors=True )
import DIRAC
#from DIRAC.ConfigurationSystem.Client import PathFinder
from DIRAC.Core.DISET.RPCClient import RPCClient

switches = Script.getUnprocessedSwitches()
for switch in switches:
    opt = switch[0].lower()
    val = switch[1]
    if opt in ( 'i', 'info' ):
        infos = val.split( ',' )
        for val in infos:
            val = val.capitalize()
            if val not in infoList:
                print "Unknown information... Select in %s" % str( infoList )
                DIRAC.exit( 0 )
            elif val == "Files":
                byFiles = True
            elif val == "Runs":
                byRuns = True
            elif val == "Tasks":
                byTasks = True
            elif val == "Jobs":
                byJobs = True
    elif opt == 'status':
        if val not in statusList:
            print "Unknown status %s... Select in %s" % ( val, str( statusList ) )
            DIRAC.exit( 0 )
        status = val
    elif opt in ( 'l', 'lfns' ):
        lfnList = val.split( ',' )
    elif opt in ( 'l', 'runs' ):
        runList = val.split( ',' )
    elif opt == 'resetruns':
        resetRuns = "Active"
        byRuns = True
    elif opt in ( 'v', 'verbose' ):
        verbose = True
    elif opt == 'tasks':
        taskList = val.split( ',' )
    elif opt == 'kickrequests':
        kickRequests = True
    elif opt == 'dumpfiles':
        dumpFiles = True
    elif opt == 'statistics':
        justStats = True
    elif opt == 'fixit':
      fixIt = True
      from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient  import BookkeepingClient
      bkClient = BookkeepingClient()

if lfnList:
    byFiles = True
if dumpFiles:
    byFiles = True

args = Script.getPositionalArgs()

if not len( args ):
    print "Specify transformation number..."
    DIRAC.exit( 0 )
else:
    ids = args[0].split( "," )
    idList = []
    for id in ids:
        r = id.split( ':' )
        if len( r ) > 1:
            for i in range( int( r[0] ), int( r[1] ) + 1 ):
                idList.append( i )
        else:
            idList.append( int( r[0] ) )


from LHCbDIRAC.TransformationSystem.Client.TransformationClient           import TransformationClient
from DIRAC.RequestManagementSystem.Client.RequestClient           import RequestClient
from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager
from DIRAC.Core.Utilities.List                                         import breakListIntoChunks
from DIRAC.Core.Utilities.List import sortList
from DIRAC import gLogger
import DIRAC
import datetime

transClient = TransformationClient()
reqClient = RequestClient()
rm = ReplicaManager()
dmTransTypes = ( "Replication", "Removal" )
now = datetime.datetime.utcnow()
timeLimit = now - datetime.timedelta( hours=2 )

for id in idList:
    res = transClient.getTransformation( id, extraParams=False )
    if not res['OK']:
        print "Couldn't find transformation", id
        continue
    else:
        transName = res['Value']['TransformationName']
        transType = res['Value']['Type']
        transBody = res['Value']['Body']
        transPlugin = res['Value']['Plugin']
        strPlugin = transPlugin
        if transType in ( 'Merge', 'DataStripping' ):
            strPlugin += ', GroupSize: %s' % str( res['Value']['GroupSize'] )
        if transType in dmTransTypes:
            taskType = "Request"
        else:
            taskType = "Job"
        transGroup = res['Value']['TransformationGroup']
    print "\n==============================\nTransformation", id, ":", transName, "of type", transType, "(plugin %s)" % strPlugin, "in", transGroup
    #if verbose:
        #print "Transformation body:", transBody
    res = transClient.getBookkeepingQueryForTransformation( id )
    if res['OK'] and res['Value']:
        print "BKQuery:", res['Value']
        queryProduction = res['Value'].get( 'ProductionID' )
    else:
        print "No BKQuery for this transformation"
        queryProduction = None

    # If just statistics are requested
    if justStats:
        filesList = getFilesForRun( id, None, 'Assigned', [] )
        statsPerSE = {}
        #print filesList
        statusList = ( 'Checking', 'Staging', 'Waiting', 'Running', 'Stalled' )
        taskList = [fileDict['TaskID'] for fileDict in filesList]
        res = transClient.getTransformationTasks( {'TransformationID':id, "TaskID":taskList} )
        if not res['OK']:
            print "Could not get the list of tasks..."
            DIRAC.exit( 2 )
        for task in res['Value']:
            #print task
            targetSE = task['TargetSE']
            status = task['ExternalStatus']
            if status in statusList:
                statsPerSE[targetSE][status] = statsPerSE.setdefault( targetSE, dict.fromkeys( statusList, 0 ) )[status] + 1
        prString = 'SE'.ljust( 15 )
        for status in statusList:
            prString += status.ljust( 10 )
        print prString
        for se in statsPerSE:
            prString = se.ljust( 15 )
            for status in statusList:
                prString += str( statsPerSE[se][status] ).ljust( 10 )
            print prString
        continue

    ################
    if runList:
        runs = []
        for runRange in runList:
            runRange = runRange.split( ':' )
            if len( runRange ) == 1:
                runs.append( {'RunNumber':int( runRange[0] )} )
            else:
                for run in range( int( runRange[0] ), int( runRange[1] ) + 1 ):
                    runs.append( {'RunNumber':run} )
    elif not byRuns:
        runs = [{'RunNumber': 0}]
    else:
        res = transClient.getTransformationRuns( {'TransformationID':id} )
        if res['OK']:
            if not len( res['Value'] ):
                print "No runs found, set to 0"
                runs = [{'RunNumber':0}]
            else:
                runs = res['Value']
    SEStat = {"Total":0}
    allFiles = []
    toBeKicked = 0
    jobList = []
    for runDict in runs:
        runID = runDict['RunNumber']
        SEs = runDict.get( 'SelectedSite', 'None' ).split( ',' )
        runStatus = runDict.get( 'Status' )
        if verbose and byRuns: print '\nRun:', runID, 'SelectedSite:', SEs, 'Status:', runStatus
        filesList = getFilesForRun( id, runID, status, lfnList )
        filesList.sort()
        if lfnList and len( lfnList ) != len( filesList ):
            foundFiles = [fileDict['LFN'] for fileDict in filesList]
            print "Some requested files were not found in transformation (%d):" % ( len( lfnList ) - len( filesList ) )
            for lfn in lfnList:
                if lfn not in foundFiles:
                    print '\t', lfn
        if len( filesList ):
            if runID or verbose:
                ( files, processed ) = filesProcessed( id, runID )
                print "Run:", runID, "- %d files, %d processed" % ( files, processed )
                if queryProduction:
                    ( filesAncestor, processedAncestor ) = filesProcessed( queryProduction, runID )
                    print "For ancestor production", queryProduction, "- %d files, %d processed" % ( filesAncestor, processedAncestor )
                    if resetRuns and files != filesAncestor:
                        res = transClient.setTransformationRunStatus( id, runID, resetRuns )
                        if res['OK']:
                            print "Successfully reset run %d to Active" % runID
                        else:
                            print "Failed to reset run %d to Active" % runID
                    if resetRuns: continue
            taskDict = {}
            prString = "%d files found" % len( filesList )
            if status:
                prString += " with status %s" % status
            print prString + '\n'
            filesToBeFixed = []
            for fileDict in filesList:
                taskID = fileDict['TaskID']
                taskDict.setdefault( taskID, [] ).append( fileDict['LFN'] )
                if byFiles and not taskList:
                    print "LFN:", fileDict['LFN'], "- Run:", fileDict['RunNumber'], "- Status:", fileDict['Status'], "- UsedSE:", fileDict['UsedSE'], "- ErrorCount:", fileDict['ErrorCount']
                if fileDict['RunNumber'] == 0 and fileDict['LFN'].find( '/MC' ) < 0:
                  filesToBeFixed.append( fileDict['LFN'] )
            if filesToBeFixed:
              if not fixIt:
                print '%d files have run number 0, use --FixIt to get this fixed' % len( filesToBeFixed )
              else:
                fixedFiles = 0
                res = bkClient.getFileMetadata( filesToBeFixed )
                if res['OK']:
                  runFiles = {}
                  for lfn, metadata in res['Value'].items():
                    runFiles.setdefault( metadata['RunNumber'], [] ).append( lfn )
                  for run in runFiles:
                    res = transClient.addTransformationRunFiles( id, run, runFiles[run] )
                    # print run, runFiles[run], res
                    if not res['OK']:
                      print "Failed to set %d files to run %d in transformation %d" % ( len( runFiles[run] ), run, id )
                    else:
                      fixedFiles += len( runFiles[run] )
                print "Successfully fixed run number for %d files" % fixedFiles
            if status == 'MissingLFC':
                lfns = [fileDict['LFN'] for fileDict in filesList]
                res = rm.getReplicas( lfns )
                if res['OK']:
                    replicas = res['Value']['Successful']
                    notMissing = len( [lfn for lfn in lfns if lfn in replicas] )
                    if notMissing:
                        if not kickRequest:
                            print "%d files are %s but indeed are in the LFC - Use --KickRequest to reset them Unused" % ( notMissing, status )
                        else:
                            res = transClient.setFileStatusForTransformation( id, 'Unused', [lfn for lfn in lfns if lfn in replicas] )
                            if res['OK']:
                                print "%d files were %s but indeed are in the LFC - Reset to Unused" % ( notMissing, status )
                    else:
                        print "All files are really missing in LFC"
            if verbose: print "Tasks:", taskDict.keys()
            nbReplicasProblematic = {}
            problematicReplicas = {}
            failedFiles = []
            if not taskList:
                taskList1 = [t for t in taskDict]
            else:
              taskList1 = taskList
            for taskID in taskList1:
                res = transClient.getTransformationTasks( {'TransformationID':id, "TaskID":taskID} )
                #print res
                if res['OK'] and res['Value']:
                    task = res['Value'][0]
                    if byJobs and taskType == 'Job':
                        jobList.append( task['ExternalID'] )
                        if not byFiles and not byTasks:
                            continue
                    if taskID not in taskDict:
                        print 'Task %s not found in the transformation files table' % taskID
                        lfns = []
                    else:
                        lfns = taskDict[taskID]
                    allFiles += lfns
                    replicas = {}
                    if transType in dmTransTypes:
                        for lfnChunk in breakListIntoChunks( lfns, 200 ):
                            res = rm.getReplicas( lfnChunk )
                            if res['OK']:
                                replicas.update( res['Value']['Successful'] )
                    nfiles = len( lfns )
                    targetSE = task.get( 'TargetSE', None )
                    # Accounting per SE
                    listSEs = targetSE.split( ',' )
                    statComment = ''
                    taskCompleted = True
                    for rep in replicas:
                        if status == 'Problematic':
                            # Problematic files, let's see why
                            realSEs = [se for se in replicas[rep] if not se.endswith( '-ARCHIVE' )]
                            nbSEs = len( realSEs )
                            nbReplicasProblematic[nbSEs] = nbReplicasProblematic.setdefault( nbSEs, 0 ) + 1
                            for se in realSEs:
                                problematicReplicas.setdefault( se, [] ).append( rep )
                        SEStat["Total"] += 1
                        if not replicas[rep]:
                            SEStat[None] = SEStat.setdefault( None, 0 ) + 1
                        for se in listSEs:
                            if transType == "Replication":
                                statComment = "missing"
                                if se not in replicas[rep]:
                                    SEStat[se] = SEStat.setdefault( se, 0 ) + 1
                                    taskCompleted = False
                            elif transType == "Removal":
                                statComment = "remaining"
                                if se in replicas[rep]:
                                    SEStat[se] = SEStat.setdefault( se, 0 ) + 1
                                    taskCompleted = False
                            else:
                                statComment = "absent"
                                if se not in replicas[rep]:
                                    SEStat[se] = SEStat.setdefault( se, 0 ) + 1
                    if byTasks:
                        prString = "TaskID: %s (created %s, updated %s) - %d files" % ( taskID, task['CreationTime'], task['LastUpdateTime'], nfiles )
                        if byFiles and lfns:
                            prString += " (" + str( taskDict[taskID] ) + ")"
                        prString += "- %s: %s - Status: %s" % ( taskType, task['ExternalID'], task['ExternalStatus'] )
                        if targetSE:
                            prString += " - TargetSE: %s" % targetSE
                        print prString
                        if taskType == "Request":
                            requestID = int( task['ExternalID'] )
                            res = reqClient.getRequestInfo( requestID )
                            if res['OK']:
                                requestName = res['Value'][2]
                            else:
                                requestName = None
                            if not taskCompleted and task['ExternalStatus'] == 'Failed':
                                if kickRequests:
                                    res = transClient.setFileStatusForTransformation( id, 'Unused', lfns )
                                    if res['OK']:
                                        print "Task is failed: %d files reset Unused" % len( lfns )
                                else:
                                    print "Task is failed: %d files could be reset Unused: use --KickRequest option" % len( lfns )
                            if taskCompleted and task['ExternalStatus'] != 'Done':
                                prString = "Task %s is completed: no %s files" % ( requestName, statComment )
                                if kickRequests:
                                  if requestName:
                                    res = reqClient.setRequestStatus( requestName, 'Done' )
                                    if res['OK']:
                                      prString += ": request set to Done"
                                    else:
                                      prString += ": error setting request to Done (%s)" % res['Message']
                                  res = transClient.setFileStatusForTransformation( id, 'Processed', lfns )
                                  if res['OK']:
                                    prString += " - %d files set Processed" % len( lfns )
                                  else:
                                    prString += " - Failed to set %d files Processed (%s)" % ( len( lfns ), res['Message'] )
                                else:
                                    prString += " - To mark them done, use option --KickRequests"
                                print prString
                            res = reqClient.getRequestFileStatus( requestID, taskDict[taskID] )
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
                                        prString += ": it should be marked as Failed, use --KickRequest"
                                    else:
                                        failedFiles += reqFiles.keys()
                                        res = reqClient.setRequestStatus( requestName, 'Failed' )
                                        if res['OK']:
                                            prString += ": request set to Failed"
                                    print prString
                            selectDict = { 'RequestID':requestID}
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

                                    if subReqDict['Status'] == 'Assigned' and subReqDict['LastUpdateTime'] < str( timeLimit ):
                                        print subReqStr
                                        toBeKicked += 1
                                        if kickRequests:
                                            res = reqClient.setRequestStatus( subReqDict['RequestName'], 'Waiting' )
                                            if res['OK']:
                                                print 'Request %d reset Waiting' % requestID

                        print ""
        elif not byRuns:
            print "No files found with given criteria"

    if status == 'Problematic':
        print "Statistics for Problematic files:"
        existingReplicas = {}
        lfns = []
        for n in sortList( nbReplicasProblematic.keys() ):
            print "   %d replicas in LFC: %d files" % ( n, nbReplicasProblematic[n] )
        gLogger.setLevel( 'FATAL' )
        for se in problematicReplicas:
            lfns += [lfn for lfn in problematicReplicas[se] if lfn not in lfns]
            res = rm.getReplicaAccessUrl( problematicReplicas[se], se )
            if res['OK']:
                for lfn in res['Value']['Successful']:
                    existingReplicas.setdefault( lfn, [] ).append( se )
        nbProblematic = len( lfns ) - len( existingReplicas )
        nbExistingReplicas = {}
        for lfn in existingReplicas:
            nbReplicas = len( existingReplicas[lfn] )
            nbExistingReplicas[nbReplicas] = nbExistingReplicas.setdefault( nbReplicas, 0 ) + 1
        if nbProblematic == len( lfns ):
            print "None of the %d problematic files actually have a replica" % len( lfns )
        else:
            print "Out of %d problematic replicas, only %d do not have a replica" % ( len( lfns ), nbProblematic )
            for n in sortList( nbExistingReplicas.keys() ):
                print "   %d replicas: %d files" % ( n, nbExistingReplicas[n] )
            for se in problematicReplicas:
                lfns = [lfn for lfn in problematicReplicas[se] if lfn not in existingReplicas or se not in existingReplicas[lfn]]
                if len( lfns ):
                    strMsg = '%d' % len( lfns )
                else:
                    strMsg = 'No'
                print "   %s : %d replicas of problematic files, %s missing replicas" % ( se.ljust( 15 ), len( problematicReplicas[se] ), strMsg )
            lfns = [lfn for lfn in existingReplicas if lfn in failedFiles]
            if lfns:
                prString = "Failed transfers but existing replicas"
                for lfn in lfns:
                    res = transClient.setFileStatusForTransformation( id, 'Unused', lfns )
                    if res['OK']:
                        prString += " - %d files reset Unused" % len( lfns )
        print ""
    if toBeKicked:
        if kickRequests:
            print "%d requests have been kicked" % toBeKicked
        else:
            print "%d requests are eligible to be kicked (use option --KickRequests)" % toBeKicked

    if SEStat["Total"] and transType in dmTransTypes:
        print "%d files found in tasks" % SEStat["Total"]
        SEStat.pop( "Total" )
        if None in SEStat:
            print "Found without replicas:", SEStat[None], "files"
            SEStat.pop( None )
        print "Statistics per %s SE:" % statComment
        SEs = SEStat.keys()
        SEs.sort()
        found = False
        for se in SEs:
            print se, SEStat[se], "files"
            found = True
        if not found:
            print "... None ..."
    elif transType == "Removal":
        print "All files have been successfully removed!"

    if dumpFiles and allFiles:
        print "List of files found:"
        print " ".join( allFiles )
        newStatus = None
        if newStatus:
            res = transClient.setFileStatusForTransformation( id, newStatus, allFiles )
            if res['OK']:
                print 'Of %d files, %d set to %s' % ( len( allFiles ), len( res['Value']['Successful'] ), newStatus )
            else:
                print "Failed to set status %s" % newStatus

    if byJobs and jobList:
        print "List of jobs found:"
        print " ".join( jobList )
