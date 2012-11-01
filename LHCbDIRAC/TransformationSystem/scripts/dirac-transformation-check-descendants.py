#!/usr/bin/env python

__RCSID__ = "$Id: dirac-transformation-check-descendants.py 42384 2011-09-07 13:31:45Z phicharp $"

import sys, os, time

import DIRAC
from DIRAC import gLogger

from DIRAC.Core.Base import Script
from DIRAC.Core.Utilities.List import breakListIntoChunks, sortList
from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager
from LHCbDIRAC.TransformationSystem.Client.TransformationClient import TransformationClient
from LHCbDIRAC.BookkeepingSystem.Client.BKQuery import BKQuery
from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient

extension = ''
Script.registerSwitch( '', 'Runs=', 'Specify the run range' )
Script.registerSwitch( '', 'RunsFromProduction=', 'Productions number where to get the list of Active runs' )
Script.registerSwitch( '', 'Extension=', 'Specify the descendants file extension [%s]' % extension )
Script.registerSwitch( '', 'FixIt', 'Fix the files in transformation table' )
Script.registerSwitch( '', 'Verbose', 'Verbose mode' )

Script.parseCommandLine( ignoreErrors=True )

transClient = TransformationClient()
bk = BookkeepingClient()
rm = ReplicaManager()

def _getRunsList( fromProd, runList ):
  
  if fromProd:
    if runList:
      gLogger.warn( "List of runs given as parameters ignored, superseded with runs from productions" )
    runList = []
    # Get the list of Active runs in that production list
    for prod in fromProd:
      res = transClient.getTransformationRuns( {'TransformationID': prod } )
      if not res['OK']:
        gLogger.warn( "Runs %s not found for transformation" %prod )
      else:
        runList += [str( run['RunNumber'] ) for run in res['Value'] if run['Status'] in ( 'Active', 'Flush' )]
    gLogger.info( "Active runs in productions %s: %s" % ( str( fromProd ), str( runList ) ) )

  runList.sort()
  
  return runList
    


def _getTransfParams():
  res = transClient.getTransformation( id )
  if not res['OK']:
    gLogger.warn(res['Message'])
    raise ValueError, res['Message']
  else:
    transType = res['Value']['Type']
    transStatus = res['Value']['Status']
  
  return transType, transStatus



if __name__ == '__main__':
  
  runList = []
  fixIt = False
  fromProd = []
  verbose = False
  for switch in Script.getUnprocessedSwitches():
    if switch[0] == 'Runs':
      runList = switch[1].split( ',' )
    if switch[0] == 'RunsFromProduction':
      fromProd = switch[1].split( ',' )
    if switch[0] == 'Extension':
      extension = switch[1].lower()
    elif switch[0] == 'FixIt':
      fixIt = True
    elif switch[0] == 'Verbose':
      verbose = True
  
  args = Script.getPositionalArgs()
  if not len( args ):
    glogger.error( "Specify transformation number..." )
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
  
  runsList = _getRunsList(fromProd, runList)
    
  resetExtension = False
  
  
  for id in idList:
  
    try:
      transType, transStatus = _getTransfParams(id)
    except ValueError:
      continue
    
    gLogger.info( "Processing %s production %d, in status %s" % ( transType, id, transStatus ) )
    
    if resetExtension:
      extension = ''
  
    checkTransFiles = ( transStatus in ( 'Active', 'Stopped', 'Completed' ) )
    res = transClient.getBookkeepingQueryForTransformation( id )
    if not res['OK']:
      gLogger.error("Problem getting the BK query: %s", res['Message'])
      DIRAC.exit( 1 )
    else:
      if res['Value']:
        gLogger.verbose( "Production BKQuery: %s" %str(res['Value']) )
        if not extension and transType == 'Merge':
          resetExtension = True
          extension = res['Value'].get( 'FileType', '' ).lower()
          gLogger.info( 'Extension set to ', extension )
        if not extension:
          print "No extension provided, please use --Extension <ext>"
          DIRAC.exit( 0 )
      else:
        gLogger.warn( "No BKQuery for transformation %d" %id )
        DIRAC.exit( 0 )
    query = res['Value']
    startRun = int( query.get( 'StartRun', 0 ) )
    endRun = int( query.get( 'EndRun', sys.maxint ) )
    runs = []
    for run in runList:
      if int( run ) >= startRun and int( run ) <= endRun:
        runs.append( run )
    bkQuery = BKQuery( res['Value'], runs=runs, visible=False )
    if not bkQuery.getQueryDict():
      gLogger.error( "Invalid query: ", bkquery )
      DIRAC.exit( 1 )
    gLogger.info( "Executing BK query:" )
    if runList:
      print bkQuery
    startTime = time.time()
    lfns = bkQuery.getLFNs( printOutput=False )
    if runList:
      runStr = ', runs %s' % str( runs )
    else:
      runStr = ''
    gLogger.info( "Input dataset from BK for production %d%s: %d files (executed in %.3f s)" % ( id, runStr, 
                                                                                                 len( lfns ), 
                                                                                                 time.time() - startTime ) )
    # Now get the transformation files
    selectDict = { 'TransformationID': id}
    if runList:
      selectDict[ 'RunNumber'] = runs
    transFiles = {}
    if checkTransFiles:
      res = transClient.getTransformationFiles( selectDict )
      if not res['OK']:
        gLogger.warn( "Failed to get files for transformation %d" % id )
      else:
        transFiles = res['Value']
    # Check if all files from BK are in the transformation table
    transLFNs = [fileDict['LFN'] for fileDict in transFiles]
    lfnsNotInTrans = [lfn for lfn in lfns if lfn not in transLFNs]
    if checkTransFiles and lfnsNotInTrans:
      gLogger.info( "Found %d files that are in the BK query and not in the transformation table" % len( lfnsNotInTrans ) )
    lfnsNotInBK = [fileDict['LFN'] for fileDict in transFiles if fileDict['LFN'] not in lfns]
    if lfnsNotInBK:
      gLogger.info( "Found %d files that are in the transformation table but not in the BK, will be added to the list" % len( lfnsNotInBK ) )
      lfns += lfnsNotInBK
    lfnsWithoutDescendants = []
    descendants = {}
    metadata = {}
    nbDescendants = {}
    chunkSize = 200
    sys.stdout.write( "Now checking descendants (one dot per %d files) : " % chunkSize )
    sys.stdout.flush()
    startTime = time.time()
    lfnChunks = breakListIntoChunks( lfns, chunkSize )
    for lfnChunk in lfnChunks:
      res = bk.getFileDescendants( lfnChunk, depth=1, production=id, checkreplica=False )
      if res['OK']:
        descChunk = res['Value']['Successful']
      else:
        gLogger.error( "\nError getting descendants for %d files" % len( lfnChunk ) )
        continue
      files = []
      for lfn in descChunk:
        #print descChunk[lfn]
        #print ['.'.join(os.path.basename( d ).split('.')[1:]) for d in descChunk[lfn]], extension
        descendants[lfn] = [d for d in descChunk[lfn] if '.'.join( os.path.basename( d ).split( '.' )[1:] ).lower() == extension.lower()]
        #print descendants[lfn]
        files += descendants[lfn]
      res = bk.getFileMetadata( files )
      if not res['OK']:
        gLogger.error( "Error getting the metadata: %s" %res['Value'] )
        continue
      metadata.update( res['Value'] )
      sys.stdout.write( '.' )
      sys.stdout.flush()
    gLogger.info( "nDescendants checked in %.3f s" % ( time.time() - startTime ) )
  
    startTime = time.time()
    noReplicas = {}
    multiDescendants = {}
    status = {}
    missingReplicaFlag = []
    withReplicaFlag = []
    for lfn in descendants:
      stat = [f['Status'] for f in transFiles if f['LFN'] == lfn]
      if stat:
        status[lfn] = stat[0]
      else:
        status[lfn] = ''
      noReplicas[lfn] = [d for d in descendants[lfn] if metadata.get( d, {} ).get( 'GotReplica', 'No' ) != 'Yes']
      descendants[lfn] = [d for d in descendants[lfn] if metadata.get( d, {} ).get( 'GotReplica', 'No' ) == 'Yes']
      if len( descendants[lfn] ) > 1:
        multiDescendants[lfn] = descendants[lfn]
      missingReplicaFlag += noReplicas[lfn]
      withReplicaFlag += descendants[lfn]
    if missingReplicaFlag:
      replicas = []
      if len( withReplicaFlag ) > len( missingReplicaFlag ):
        gLogger.info( "Checking LFC for %d files without replica  flag" % len( missingReplicaFlag ) )
        res = rm.getReplicas( missingReplicaFlag )
        if res['OK']:
          replicas = res['Value']['Successful']
      else:
        dirs = []
        for lfn in missingReplicaFlag:
          dir = os.path.dirname( lfn )
          if dir not in dirs:
            dirs.append( dir )
        dirs.sort()
        gLogger.info( "Checking LFC for %d directories containing files without replica flag" % len( dirs ))
  #      gLogger.setLevel( 'FATAL' )
        res = rm.getFilesFromDirectory( dirs )
  #      gLogger.setLevel( 'WARNING' )
        if not res['OK']:
          gLogger.error( "Error getting files from directories %s: " % dirs, res['Message'] )
          continue
        if res['Value']:
          res = rm.getReplicas( res['Value'] )
          if res['OK']:
            replicas = res['Value']['Successful']
      for lfn in [lfn for lfn in noReplicas if noReplicas[lfn]]:
        reps = [ r for r in noReplicas[lfn] if r in replicas]
        if reps:
          for rep in reps:
            gLogger.warn( rep, " doesn't have the replica flag while it has %d replicas" % len( replicas[rep] ) )
            noReplicas[lfn].remove( rep )
            descendants[lfn].append( rep )
          res = bk.addFiles( reps )
          if res['OK']:
            print "Successfully added replica flag to %d files" % len( replicas )
  
  
    lfnsWithoutDescendants = [lfn for lfn in lfns if not descendants.get( lfn ) and not noReplicas.get( lfn )]
    lfnsNotProcessed = [lfn for lfn in status if status[lfn] != 'Processed' and descendants[lfn]]
    for lfn in descendants:
      nb = len( descendants[lfn] )
      nbDescendants[nb] = nbDescendants.setdefault( nb, 0 ) + 1
    nbNoAndReplicas = len( [lfn for lfn in noReplicas if noReplicas[lfn] and descendants[lfn]] )
    nbNoReplicas = len( [lfn for lfn in noReplicas if noReplicas[lfn] and not descendants[lfn]] )
  
    print "Replicas checked in %.3f s" % ( time.time() - startTime )
  
    if multiDescendants:
      print '\n%d file(s) with more than one descendant:' % len( multiDescendants )
      for lfn in multiDescendants:
        print '\t%s: %s' % ( lfn, multiDescendants[lfn] )
  
    if len( lfnsWithoutDescendants ):
      print "\n%d files have no descendants in production %d" % ( len( lfnsWithoutDescendants ), id )
      lfnsToAdd = []
      lfnsStatus = {}
      for lfn in lfnsWithoutDescendants:
        status = [fileDict['Status'] for fileDict in transFiles if fileDict['LFN'] == lfn]
        if not status:
          lfnsToAdd.append( lfn )
        else:
          lfnsStatus[lfn] = status
      if lfnsToAdd:
        if not fixIt:
          print "==> Files can be added with option --FixIt\n"
          print lfnsToAdd
        else:
          nbFiles = 0
          res = transClient.addFilesToTransformation( id, lfnsToAdd )
          addedLfns = []
          if res['OK']:
            addedLfns = [lfn for ( lfn, status ) in res['Value']['Successful'].items() if status == 'Added']
            if addedLfns:
              res = bk.getFileMetadata( addedLfns )
              if res['OK']:
                runFiles = {}
                for lfn in res['Value']:
                  runID = res['Value'][lfn].get( 'RunNumber', None )
                  if runID:
                    runFiles.setdefault( runID, [] ).append( lfn )
                for runID in runFiles:
                  res = transClient.addTransformationRunFiles( id, runID, runFiles[runID] )
                  if res['OK']:
                    nbFiles += len( runFiles[runID] )
          print "%d files successfully added to production %d" % ( nbFiles, id )
      if lfnsStatus:
        print "%d files have no descendants but are in the transformation table" % len( lfnsStatus )
        if verbose:
          for lfn, status in lfnsStatus.items():
            print '\t%s : %s' % ( lfn, status )
    else:
      print "All files have descendants in production %d" % id
  
    if checkTransFiles:
      if len( lfnsNotProcessed ):
        print "\nThe following %d files have descendants but don't have status 'Processed' in production %d" % ( len( lfnsNotProcessed ), id )
        for lfn in lfnsNotProcessed:
          status = [fileDict['Status'] for fileDict in transFiles if fileDict['LFN'] == lfn]
          if status:
              status = status[0]
          else:
              status = 'Not in transformation files'
          print lfn, status
        if not fixIt:
          print "==> Files can be set Processed with option --FixIt\n"
        else:
          res = transClient.setFileStatusForTransformation( id, 'Processed', lfnsNotProcessed )
          if res['OK']:
            prStr = 'Succeeded'
          else:
            prStr = 'Failed'
          prStr += " setting the %d files as Processed in production %d" % ( len( lfnsNotProcessed ), id )
          print prStr
      else:
        print "All files with descendants have status 'Processed' in production %d" % id
    else:
      print "Transformation is %s, cannot check the transformations table" % transStatus
  
  
    print "Number of files per number of descendants with replicas:"
    for nb in sortList( nbDescendants ):
      print "%2d descendants: %d files" % ( nb, nbDescendants[nb] )
    if nbNoReplicas:
      print "%2d files have descendants with no replicas" % nbNoReplicas
    if nbNoAndReplicas:
      print "%2d files have descendants with and without replicas" % nbNoAndReplicas
  
    separator = '=' * 50 + '\n'
