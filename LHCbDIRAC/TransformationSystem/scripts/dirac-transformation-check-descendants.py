#!/usr/bin/env python

__RCSID__ = "$Id: dirac-transformation-check-descendants.py 42384 2011-09-07 13:31:45Z phicharp $"

from DIRAC.Core.Base import Script

extension = '.sdst'
Script.registerSwitch( '', 'Runs=', 'Specify the run range' )
Script.registerSwitch( '', 'RunsFromProduction=', 'Productions number where to get the list of Active runs' )
Script.registerSwitch( '', 'Extension=', 'Specify the descendants file extension [%s]' % extension )

Script.parseCommandLine( ignoreErrors = True )
import DIRAC
import sys, os

runList = []
fromProd = None
for switch in Script.getUnprocessedSwitches():
    if switch[0] == 'Runs':
        runList = switch[1].split( ',' )
    if switch[0] == 'RunsFromProduction':
        fromProd = switch[1].split( ',' )
    if switch[0] == 'Extension':
        extension = switch[1].replace( '.', '' )

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
from LHCbDIRAC.NewBookkeepingSystem.Client.BookkeepingClient              import BookkeepingClient
from DIRAC.Core.Utilities.List                                            import breakListIntoChunks, sortList
from DIRAC.DataManagementSystem.Client.ReplicaManager                     import ReplicaManager
import time

transClient = TransformationClient()
bk = BookkeepingClient()
rm = ReplicaManager()

if fromProd:
  runList = []
  # Get the list of Active runs in that production list
  for prod in fromProd:
      res = transClient.getTransformationRuns( {'TransformationID': prod } )
      if not res['OK']:
          print "Runs not found for transformation", prod
      else:
          runList += [run['RunNumber'] for run in res['Value'] if run['Status'] == 'Active']
  print "Active runs in productions %s:" % str( fromProd ), runList

runList.sort()
for id in idList:
  print "Processing production", id
  res = transClient.getBookkeepingQueryForTransformation( id )
  if res['OK'] and res['Value']:
    print "BKQuery:", res['Value']
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
  bkQuery = BKQuery( res['Value'], runs = runs )
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
    res = bk.getAllDescendents( lfnChunk, depth = 2, production = id )
    if res['OK']:
      descChunk = res['Value']['Successful']
    else:
      print "\nError getting descendants for %d files" % len( lfnChunk )
      continue
    files = []
    for lfn in descChunk:
      descendants[lfn] = [d for d in descChunk[lfn] if os.path.splitext( d )[1] == extension]
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
  for lfn in descendants:
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
        fileList = []
    print "The following %d files have no descendants in production %d" % ( len( lfnsWithoutDescendants ), id )
    for lfn in lfnsWithoutDescendants:
      status = [fileDict['Status'] for fileDict in fileList if fileDict['LFN'] == lfn]
      if status:
          status = status[0]
      else:
          status = 'Not in files'
      print lfn, status
  else:
    print "All files have descendants in production %d" % id

  print "Number of files for number of descendants:"
  for nb in sortList( nbDescendants ):
      print "%2d : %d" % ( nb, nbDescendants[nb] )
  if nbNoReplicas:
      print "%2d files have descendants with no replicas" % nbNoReplicas
  if nbNoAndReplicas:
      print "%2d files have descendants with and without replicas" % nbNoAndReplicas
