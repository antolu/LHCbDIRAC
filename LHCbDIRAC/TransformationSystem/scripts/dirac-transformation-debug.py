#!/usr/bin/env python
import sys

def getFilesForRun( id, runID, status = None, lfnList = None ):
    #print id,runID,status
    selectDict = {'TransformationID':id}
    if runID:
        selectDict["RunNumber"] = runID
    if status:
        selectDict['Status'] = status
    if lfnList:
        selectDict['LFN'] = lfnList
    res = transClient.getTransformationFiles( selectDict )
    #print res
    return res['Value']

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
listFiles = False
byRun = False
byTasks = False
dumpFiles = False
status = None
lfnList = None
taskList = None
resetRuns = None
runList = None
kickRequests = False
from DIRAC.Core.Base import Script

infoList = ["Files", "Runs", "Tasks"]
statusList = ["Unused", "Assigned", "Done", "Problematic", "MissingLFC"]
Script.registerSwitch( 'i:', 'Info=', "Specify what to print out from %s" % str( infoList ) )
Script.registerSwitch( '', 'Status=', "Select files with a given status from %s" % str( statusList ) )
Script.registerSwitch( 'l:', 'LFNs=', "Specify a (list of) LFNs" )
Script.registerSwitch( '', 'Runs=', "Specify a (list of) runss" )
Script.registerSwitch( '', 'Tasks=', "Specify a (list of) tasks" )
Script.registerSwitch( '', 'ResetRuns', "Reset runs in Active status (use with care!)" )
Script.registerSwitch( '', 'KickRequests', 'Reset old Assigned requests to Waiting' )
Script.registerSwitch( '', 'DumpFiles', 'Dump the list of LFNs in a file' )

Script.parseCommandLine( ignoreErrors = True )
import DIRAC
from DIRAC.ConfigurationSystem.Client import PathFinder
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
                listFiles = True
            elif val == "Runs":
                byRun = True
            elif val == "Tasks":
                byTasks = True
    elif opt == 'status':
        if val not in statusList:
            print "Unknown status %s... Select in %s" % ( val, str( statusList ) )
            DIRAC.exit( 0 )
        status = val.capitalize()
    elif opt in ( 'l', 'lfns' ):
        lfnList = val.split( ',' )
    elif opt in ( 'l', 'runs' ):
        runList = val.split( ',' )
    elif opt == 'resetruns':
        resetRuns = "Active"
        byRun = True
    elif opt in ( 'v', 'verbose' ):
        verbose = True
    elif opt == 'tasks':
        taskList = val.split( ',' )
    elif opt == 'kickrequests':
        kickRequests = True
    elif opt == 'dumpfiles':
        dumpFiles = True

if lfnList:
    listFiles = True
if dumpFiles:
    listFiles = True

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
import DIRAC
import datetime

transClient = TransformationClient()
reqClient = RequestClient()
rm = ReplicaManager()
dmTransTypes = ( "Replication", "Removal" )
now = datetime.datetime.utcnow()
timeLimit = now - datetime.timedelta( hours = 2 )

for id in idList:
    res = transClient.getTransformation( id )
    if not res['OK']:
        print "Couldn't find transformation", id
        continue
    else:
        transName = res['Value']['TransformationName']
        transType = res['Value']['Type']
        transBody = res['Value']['Body']
        transPlugin = res['Value']['Plugin']
        if transType in dmTransTypes:
            taskType = "Request"
        else:
            taskType = "Job"
        transGroup = res['Value']['TransformationGroup']
    print "\n==============================\nTransformation", id, ":", transName, "of type", transType, "(plugin", transPlugin, ") in", transGroup
    if verbose:
        print "Transformation body:", transBody
    res = transClient.getBookkeepingQueryForTransformation( id )
    if res['OK'] and res['Value']:
        print "BKQuery:", res['Value']
        queryProduction = res['Value'].get( 'ProductionID' )
    else:
        print "No BKQuery for this transformation"
        queryProduction = None
    if runList:
        runs = []
        for run in runList:
            runs.append( {'RunNumber':run} )
    elif not byRun:
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
    for runDict in runs:
        runID = runDict['RunNumber']
        SEs = runDict.get( 'SelectedSite', 'None' ).split( ',' )
        runStatus = runDict.get( 'Status' )
        if verbose and byRun: print '\nRun:', runID, 'SelectedSite:', SEs, 'Status:', runStatus
        filesList = getFilesForRun( id, runID, status, lfnList )
        filesList.sort()
        if lfnList and len( lfnList ) != len( filesList ):
            foundFiles = [fileDict['LFN'] for fileDict in filesList]
            print "Some files were not found in transformation (%d):" % ( len( lfnList ) - len( filesList ) )
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
            for fileDict in filesList:
                taskID = fileDict['TaskID']
                taskDict.setdefault( taskID, [] ).append( fileDict['LFN'] )
                if listFiles and not taskList:
                    print "LFN:", fileDict['LFN'], "- Status:", fileDict['Status'], "- UsedSE:", fileDict['UsedSE'], "- ErrorCount:", fileDict['ErrorCount']
            if verbose: print "Tasks:", taskDict.keys()
            for taskID in [t for t in taskDict if not taskList or t in taskList]:
                res = transClient.getTransformationTasks( {'TransformationID':id, "TaskID":taskID} )
                if res['OK'] and res['Value']:
                    task = res['Value'][0]
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
                        if listFiles:
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
                            if task['ExternalStatus'] == 'Failed':
                                if kickRequests:
                                    res = transClient.setFileStatusForTransformation( id, 'Unused', lfns )
                                    if res['OK']:
                                        print "Task is failed: %d files reset Unused" % len( lfns )
                                else:
                                    print "Task is failed: %d files could be reset Unused: use --KickRequest option" % len( lfns )
                            if taskCompleted and requestName:
                                prString = "Task %s is completed: no %s files" % ( requestName, statComment )
                                if kickRequests and requestName:
                                    res = reqClient.setRequestStatus( requestName, 'Done' )
                                    if res['OK']:
                                        prString += ": request set to Done"
                                        res = transClient.setFileStatusForTransformation( id, 'Processed', lfns )
                                        if res['OK']:
                                            prString += " - %d files set Processed" % len( lfns )
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
        elif not byRun:
            print "No files found with given criteria"

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
