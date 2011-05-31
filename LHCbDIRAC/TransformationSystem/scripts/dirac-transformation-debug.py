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
from DIRAC.Core.Base import Script

infoList = ["Files", "Runs", "Tasks"]
statusList = ["Unused", "Assigned", "Done", "Problematic", "MissingLFC"]
Script.registerSwitch( 'i:', 'Info=', "Specify what to print out from %s" % str( infoList ) )
Script.registerSwitch( '', 'Status=', "Select files with a given status from %s" % str( statusList ) )
Script.registerSwitch( 'l:', 'LFNs=', "Specify a list of LFNs" )
Script.registerSwitch( '', 'ResetRuns', "Reset runs in Active status (use with care!)" )

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
        val = val.capitalize()
        if val not in statusList:
            print "Unknown status... Select in %s" % str( statusList )
            DIRAC.exit( 0 )
        status = val.capitalize()
    elif opt in ( 'l', 'lfns' ):
        lfnList = val.split( ',' )
    elif opt == 'resetruns':
        resetRuns = "Active"
        byRun = True
    elif opt in ( 'v', 'verbose' ):
        verbose = True

if lfnList:
    listFiles = True

args = Script.getPositionalArgs()

if not len( args ):
    print "Specify transformation number..."
    DIRAC.exit( 0 )
else:
    ids = args[0].split( ":" )
    id1 = int( ids[0] )
    if len( ids ) > 1:
        id2 = int( ids[1] )
    else:
        id2 = id1

#print id1,id2,status

from LHCbDIRAC.TransformationSystem.Client.TransformationClient           import TransformationClient
from DIRAC.RequestManagementSystem.Client.RequestClient           import RequestClient
from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager
from DIRAC.Core.Utilities.List                                         import breakListIntoChunks
import DIRAC

transClient = TransformationClient()
reqClient = RequestClient()
rm = ReplicaManager()

for id in range( id1, id2 + 1 ):
    res = transClient.getTransformation( id )
    if not res['OK']:
        print "Couldn't find transformation", id
        continue
    else:
        transName = res['Value']['TransformationName']
        transType = res['Value']['Type']
        transBody = res['Value']['Body']
        transPlugin = res['Value']['Plugin']
        if transType in ["Replication", "Removal"]:
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
    if not byRun:
        res = {'OK':True}
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
    for runDict in runs:
        runID = runDict['RunNumber']
        SEs = runDict.get( 'SelectedSite', 'None' ).split( ',' )
        runStatus = runDict.get( 'Status' )
        if verbose and byRun: print '\nRun:', runID, 'SelectedSite:', SEs, 'Status:', runStatus
        filesList = getFilesForRun( id, runID, status, lfnList )
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
            print prString
            for fileDict in filesList:
                taskID = fileDict['TaskID']
                if not taskDict.has_key( taskID ):
                    taskDict[taskID] = []
                taskDict[taskID].append( fileDict['LFN'] )
                if listFiles:
                    print "LFN:", fileDict['LFN'], "- Status:", fileDict['Status'], "- UsedSE:", fileDict['UsedSE'], "- ErrorCount:", fileDict['ErrorCount']
            if verbose: print "Tasks:", taskDict.keys()
            for taskID in taskDict.keys():
                res = transClient.getTransformationTasks( {'TransformationID':id, "TaskID":taskID} )
                if res['OK'] and res['Value']:
                    task = res['Value'][0]
                    lfns = taskDict[taskID]
                    allFiles += lfns
                    replicas = {}
                    for lfnChunk in breakListIntoChunks( lfns, 200 ):
                        res = rm.getReplicas( lfnChunk )
                        if res['OK']:
                            replicas.update( res['Value']['Successful'] )
                    nfiles = len( lfns )
                    targetSE = task.get( 'TargetSE', None )
                    # Accounting per SE
                    listSEs = targetSE.split( ',' )
                    statComment = ''
                    for rep in replicas.keys():
                        SEStat["Total"] += 1
                        if not replicas[rep]:
                            if not SEStat.has_key( None ):
                                SEStat[None] = 0
                            SEStat[None] += 1
                        for se in listSEs:
                            if transType == "Replication":
                                statComment = "missing"
                                if se not in replicas[rep]:
                                    if not SEStat.has_key( se ):
                                        SEStat[se] = 0
                                    SEStat[se] += 1
                            elif transType == "Removal":
                                statComment = "remaining"
                                if se in replicas[rep]:
                                    if not SEStat.has_key( se ):
                                        SEStat[se] = 0
                                    SEStat[se] += 1
                            else:
                                if not SEStat.has_key( se ):
                                    SEStat[se] = 0
                                SEStat[se] += 1

                    if byTasks:
                        prString = "TaskID: %s (created %s, updated %s) - %d files" % ( taskID, task['CreationTime'], task['LastUpdateTime'], nfiles )
                        if listFiles:
                            prString += " (" + str( taskDict[taskID] ) + ")"
                        prString += "- %s: %s - Status: %s" % ( taskType, task['ExternalID'], task['ExternalStatus'] )
                        if targetSE:
                            prString += " - TargetSE: %s" % targetSE
                        print prString
                        if taskType == "Request":
                            res = reqClient.getRequestStatus( int( task['ExternalID'] ) )
                            if res['OK']:
                                print "Request status:", res['Value']
                            res = reqClient.getRequestInfo( int( task['ExternalID'] ) )
                            if res['OK']:
                                print "Request info:", res['Value']
                            res = reqClient.getRequestFileStatus( int( task['ExternalID'] ), taskDict[taskID] )
                            if res['OK']:
                                reqFiles = res['Value']
                                statFiles = {}
                                for lfn, stat in reqFiles.items():
                                    if not statFiles.has_key( stat ):
                                        statFiles[stat] = 0
                                    statFiles[stat] += 1
                                stats = statFiles.keys()
                                stats.sort()
                                for stat in stats:
                                    print "%s: %d files" % ( stat, statFiles[stat] )

    if SEStat["Total"]:
        print "%d files found in tasks" % SEStat["Total"]
        SEStat.pop( "Total" )
        if SEStat.has_key( None ):
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
