"""
  Utilities for preparing and testing BK queries for transformations
"""

from LHCbDIRAC.NewBookkeepingSystem.Client.BookkeepingClient  import BookkeepingClient
from DIRAC.Core.DISET.RPCClient                               import RPCClient
from DIRAC.TransformationSystem.Client.TransformationClient   import TransformationClient
from DIRAC.Core.Utilities.List                                import sortList

__RCSID__ = "$Id$"

def getDirsForBKQuery( query, printOutput = False ):
  ( lfns, dirs ) = makeBKQuery( query, "Removal", printOutput = printOutput )
  return dirs

def testBKQuery( query, type, printOutput = True ):
  ( lfns, dirs ) = makeBKQuery( query, type, printOutput = printOutput )
  return lfns

def makeBKQuery( transBKQuery, transType, printOutput = True ):
  bk = BookkeepingClient()
  res = bk.getFilesWithGivenDataSets( transBKQuery )
  if not res['OK']:
    print "**** ERROR in BK query ****"
    print res['Message']
    return None
  else:
    lfns = res['Value']
    lfns.sort()
    dirs = {}
    import os
    for lfn in lfns:
      dir = os.path.dirname( lfn )
      if not dirs.has_key( dir ):
        dirs[dir] = 0
      dirs[dir] += 1
    transBKQuery.update( {"FileSize":True} )
    res = bk.getFilesWithGivenDataSets( transBKQuery )
    lfnSize = 0
    if res['OK'] and type( res['Value'] ) == type( [] and res['Value'][0] ):
      if res['Value'][0]:
        lfnSize = res['Value'][0] / 1000000000000.
    if printOutput:
      print "\n%d files (%.1f TB) in directories:" % ( len( lfns ), lfnSize )
      for dir in dirs.keys():
        print dir, dirs[dir], "files"

    if printOutput and transType == "Removal":
      rpc = RPCClient( 'DataManagement/StorageUsage' )
      totalUsage = {}
      totalSize = 0
      for dir in dirs.keys():
        res = rpc.getStorageSummary( dir, '', '', [] )
        if res['OK']:
          for se in [se for se in res['Value'].keys() if not se.endswith( "-ARCHIVE" )]:
            if not totalUsage.has_key( se ):
              totalUsage[se] = 0
            totalUsage[se] += res['Value'][se]['Size']
            totalSize += res['Value'][se]['Size']
      ses = totalUsage.keys()
      ses.sort()
      totalUsage['Total'] = totalSize
      ses.append( 'Total' )
      print "\n%s %s" % ( "SE".ljust( 20 ), "Size (TB)" )
      for se in ses:
        print "%s %s" % ( se.ljust( 20 ), ( '%.1f' % ( totalUsage[se] / 1000000000000. ) ) )
  return ( lfns, dirs.keys() )

def buildBKQuery( bkQuery, prods, fileType, runs ):
  bkFields = [ "ConfigName", "ConfigVersion", "DataTakingConditions", "ProcessingPass", "EventType", "FileType" ]
  transBKQuery = {'Visible': 'Yes'}
  requestID = None
  if runs:
    if runs[0]:
      transBKQuery['StartRun'] = runs[0]
    if runs[1]:
      transBKQuery['EndRun'] = runs[1]

  if prods:
    if not fileType:
      fileType = ['All']
    if prods[0].upper() != 'ALL':
      transBKQuery['ProductionID'] = prods
      if len( prods ) == 1:
        res = TransformationClient().getTransformation( int( prods[0] ) )
        if res['OK']:
          requestID = int( res['Value']['TransformationFamily'] )
    if fileType[0].upper() != 'ALL':
      transBKQuery['FileType'] = fileType
  elif not bkQuery:
    if fileType:
      transBKQuery['FileType'] = fileType
    else:
      return ( None, None )
  else:
    bkQuery = bkQuery.replace( "RealData", "Real Data" )
    if bkQuery[0] == '/':
      bkQuery = bkQuery[1:].replace( "RealData", "Real Data" )
    bk = bkQuery.split( '/' )
    try:
      bkNodes = [bk[0], bk[1], bk[2], '/' + '/'.join( bk[3:-2] ), bk[-2], bk[-1]]
    except:
      print "Incorrect BKQuery...\nSyntax: %s" % '/'.join( bkFields )
      return ( None, None )
    if bkNodes[0] == "MC" or bk[-2][0] != '9':
      bkFields[2] = "SimulationConditions"
    for i in range( len( bkFields ) ):
      if not bkNodes[i].upper().endswith( 'ALL' ):
        transBKQuery[bkFields[i]] = bkNodes[i]

  fileType = transBKQuery.get( 'FileType' )
  if fileType:
    if type( fileType ) == type( [] ):
      fileTypes = fileType
    else:
      fileTypes = fileType.split( ',' )
    expandedTypes = []
    for fileType in fileTypes:
      if fileType.lower().find( "all." ) == 0:
        ext = '.' + fileType.split( '.' )[1]
        fileType = []
        bk = BookkeepingClient()
        res = bk.getAvailableFileTypes()

        if res['OK']:
          dbresult = res['Value']
          for record in dbresult['Records']:
            if record[0].endswith( ext ):
              expandedTypes.append( record[0] )
      else:
        expandedTypes.append( fileType )
    if len( expandedTypes ) == 1:
      expandedTypes = expandedTypes[0]
    transBKQuery['FileType'] = expandedTypes

  return ( transBKQuery, requestID )
