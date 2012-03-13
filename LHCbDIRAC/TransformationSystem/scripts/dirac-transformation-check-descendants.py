#!/usr/bin/env python

__RCSID__ = "$Id: dirac-transformation-check-descendants.py 42384 2011-09-07 13:31:45Z phicharp $"

from DIRAC.Core.Base import Script

extension = ''
Script.registerSwitch( '', 'Runs=', 'Specify the run range' )
Script.registerSwitch( '', 'RunsFromProduction=', 'Productions number where to get the list of Active runs' )
Script.registerSwitch( '', 'Extension=', 'Specify the descendants file extension [%s]' % extension )
Script.registerSwitch( '', 'FixIt', 'Fix the files in transformation table')

Script.parseCommandLine( ignoreErrors = True )
import DIRAC
import sys, os

runList = []
fixIt = False
fromProd = None
for switch in Script.getUnprocessedSwitches():
    if switch[0] == 'Runs':
        runList = switch[1].split( ',' )
    if switch[0] == 'RunsFromProduction':
        fromProd = switch[1].split( ',' )
    if switch[0] == 'Extension':
        extension = switch[1].lower()
    elif switch[0] == 'FixIt':
      fixIt = True

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
from LHCbDIRAC.DataManagementSystem.Client.DMScript                       import BKQuery
from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient              import BookkeepingClient
from DIRAC.Core.Utilities.List                                            import breakListIntoChunks, sortList
from DIRAC.DataManagementSystem.Client.ReplicaManager                     import ReplicaManager
import time

transClient = TransformationClient()
bk = BookkeepingClient()
rm = ReplicaManager()

if fromProd:
  if runList:
    print "List of runs given as parameters ignored, superseded with runs from productions", fromProd
  runList = []
  # Get the list of Active runs in that production list
  for prod in fromProd:
      res = transClient.getTransformationRuns( {'TransformationID': prod } )
      if not res['OK']:
          print "Runs not found for transformation", prod
      else:
          runList += [str(run['RunNumber']) for run in res['Value'] if run['Status'] in ('Active','Flush')]
  print "Active runs in productions %s:" % str( fromProd ), runList

runList.sort()
resetExtension = False
for id in idList:
  if resetExtension:
    extension = ''
  print "Processing production", id
  res = transClient.getBookkeepingQueryForTransformation( id )
  if res['OK'] and res['Value']:
    print "Production BKQuery:", res['Value']
    if not extension:
      resetExtension = True
      extension = res['Value'].get('FileType', '').lower()
      print 'Extension set to',extension
    if not extension:
      print "No extension provided, please use --Extension <ext>"
      DIRAC.exit(0)
  else:
    print "No BKQuery for this transformation"
    DIRAC.exit( 0 )
  query = res['Value']
  startRun = int( query.get( 'StartRun', 0 ) )
  endRun = int( query.get( 'EndRun', sys.maxint ) )
  runs = []
  for run in runList:
      if int( run ) >= startRun and int( run ) <= endRun:
          runs.append( run )
  bkQuery = BKQuery( res['Value'], runs = runs, visible=False )
  bkQuery.setOption( 'ReplicaFlag', 'All')
  print "Executing BK query:"
  if runList:
    print bkQuery
  startTime = time.time()
  lfns = bkQuery.getLFNs( printOutput = False )
  if runList:
      runStr = ', runs %s' % str( runs )
  else:
      runStr = ''
  print "Input dataset for production %d%s: %d files (executed in %.3f s)" % ( id, runStr, len( lfns ), time.time() - startTime )
  # Now get the transformation files
  selectDict = { 'TransformationID': id}
  if runList:
    selectDict[ 'RunNumber'] = runs
  res = transClient.getTransformationFiles( selectDict )
  if not res['OK']:
    print "Failed to get files for transformation", id
    transFiles = {}
  else:
    transFiles = res['Value']
  lfnsWithoutDescendants = []
  descendants = {}
  metadata = {}
  nbDescendants = {}
  nbNoReplicas = 0
  nbNoAndReplicas = 0
  chunkSize = 200
  sys.stdout.write( "Now checking descendants (one dot per %d files) : " % chunkSize )
  startTime = time.time()
  lfnChunks = breakListIntoChunks( lfns, chunkSize )
  sys.stdout.write( '.'*len( lfnChunks ) )
  sys.stdout.flush()
  for lfnChunk in lfnChunks:
    res = bk.getFileDescendents( lfnChunk, depth = 1, production = id )
    if res['OK']:
      descChunk = res['Value']['Successful']
    else:
      print "\nError getting descendants for %d files" % len( lfnChunk )
      continue
    files = []
    for lfn in descChunk:
      #print descChunk[lfn]
      #print ['.'.join(os.path.basename( d ).split('.')[1:]) for d in descChunk[lfn]], extension
      descendants[lfn] = [d for d in descChunk[lfn] if '.'.join(os.path.basename( d ).split('.')[1:]).lower() == extension.lower()]
      #print descendants[lfn]
      files += descendants[lfn]
    res = bk.getFileMetadata( files )
    if not res['OK']:
        print "Error getting the metadata"
        continue
    metadata.update( res['Value'] )
    sys.stdout.write( "\b \b" )
    sys.stdout.flush()

  print ""
  noReplicas = {}
  status = {}
  for lfn in descendants:
      stat = [f['Status'] for f in transFiles if f['LFN'] == lfn]
      if stat:
        status[lfn] = stat[0]
      else:
        status[lfn] = ''
      noReplicas[lfn] = [d for d in descendants[lfn] if metadata.get( d, {} ).get( 'GotReplica', 'No' ) != 'Yes']
      descendants[lfn] = [d for d in descendants[lfn] if metadata.get( d, {} ).get( 'GotReplica', 'No' ) == 'Yes']
      if noReplicas[lfn]:
          res = rm.getReplicas( noReplicas[lfn] )
          if res['OK']:
              replicas = res['Value']['Successful']
              if replicas:
                  for rep in replicas:
                      print rep, "doesn't have the replica flag while it has %d replicas" % len( replicas[rep] )
                      noReplicas[lfn].remove( rep )
                      descendants[lfn].append( rep )
                  res = bk.addFiles( replicas.keys() )
                  if res['OK']:
                      print "Successfully added replica flag to %d files" % len( replicas )

  lfnsWithoutDescendants = [lfn for lfn in lfns if not descendants[lfn]]
  lfnsNotProcessed = [lfn for lfn in status if status[lfn] != 'Processed' and descendants[lfn]]
  for lfn in descendants:
    nb = len( descendants[lfn] )
    nbDescendants[nb] = nbDescendants.setdefault( nb, 0 ) + 1
  for lfn in [lfn for lfn in noReplicas if noReplicas[lfn]]:
      if descendants[lfn]:
          nbNoAndReplicas += 1
      else:
          nbNoReplicas += 1
  print "Descendants checked in %.3f s" % ( time.time() - startTime )
  if len( lfnsWithoutDescendants ):
    res = transClient.getTransformationFiles( {'TransformationID':id, 'LFN':lfnsWithoutDescendants} )
    if res['OK']:
        fileList = res['Value']
    else:
        fileList = {}
    print "\nThe following %d files have no descendants in production %d" % ( len( lfnsWithoutDescendants ), id )
    lfnsToAdd = []
    for lfn in lfnsWithoutDescendants:
      status = [fileDict['Status'] for fileDict in fileList if fileDict['LFN'] == lfn]
      if status:
          status = status[0]
      else:
          status = 'Not in transformation files'
          lfnsToAdd.append(lfn)
      print lfn, status
    if lfnsToAdd:
      if not fixIt:
        print "==> Files can be added with option --FixIt\n"
      else:
        nbFiles = 0
        res = transClient.addFilesToTransformation( id, lfnsToAdd)
        addedLfns = []
        if res['OK']:
          addedLfns = [lfn for (lfn,status) in res['Value']['Successful'].items() if status == 'Added']
          if addedLfns:
            res = bk.getFileMetadata(addedLfns)
            if res['OK']:
              runFiles = {}
              for lfn in res['Value']:
                runID = res['Value'][lfn].get('RunNumber', None)
                if runID:
                  runFiles.setdefault(runID,[]).append(lfn)
              for runID in runFiles:
                res = transClient.addTransformationRunFiles( id, runID, runFiles[runID])
                if res['OK']:
                  nbFiles += len(runFiles[runID])
        print "%d files successfully added to production %d" %(nbFiles, id)

  else:
    print "All files have descendants in production %d" % id

  if len( lfnsNotProcessed ):
    res = transClient.getTransformationFiles( {'TransformationID':id, 'LFN':lfnsNotProcessed} )
    if res['OK']:
        fileList = res['Value']
    else:
        fileList = {}
    print "\nThe following %d files have descendants but are not Processed in production %d" % ( len( lfnsNotProcessed ), id )
    for lfn in lfnsNotProcessed:
      status = [fileDict['Status'] for fileDict in fileList if fileDict['LFN'] == lfn]
      if status:
          status = status[0]
      else:
          status = 'Not in transformation files'
      print lfn, status
    if not fixIt:
      print "==> Files can be set Processed with option --FixIt\n"
    else:
      res = transClient.setFileStatusForTransformation( id, 'Processed', lfnsNotProcessed)
      if res['OK']:
        prStr = 'Succeeded'
      else:
        prStr = 'Failed'
      prStr += " setting the %d files as Processed in production %d" % ( len( lfnsNotProcessed ), id )
      print prStr
  else:
    print "All files with descendants are set Processed in production %d" % id


  print "Number of files for number of descendants:"
  for nb in sortList( nbDescendants ):
      print "%2d : %d" % ( nb, nbDescendants[nb] )
  if nbNoReplicas:
      print "%2d files have descendants with no replicas" % nbNoReplicas
  if nbNoAndReplicas:
      print "%2d files have descendants with and without replicas" % nbNoAndReplicas

  print '='*50+'\n'
