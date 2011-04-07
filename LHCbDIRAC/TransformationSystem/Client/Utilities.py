"""
  Utilities for preparing and testing BK queries for transformations
"""

from LHCbDIRAC.NewBookkeepingSystem.Client.BookkeepingClient  import BookkeepingClient
from DIRAC.Core.DISET.RPCClient                               import RPCClient
from DIRAC.TransformationSystem.Client.TransformationClient   import TransformationClient
from DIRAC.Core.Utilities.List                                import sortList

__RCSID__ = "$Id: $"

def testBKQuery( transBKQuery, transType ):
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
    if res['OK']:
      lfnSize = res['Value'][0] / 1000000000000.
    print "\n%d files (%.1f TB) in directories:" % ( len( lfns ), lfnSize )
    for dir in dirs.keys():
      print dir, dirs[dir], "files"

    if transType == "Removal":
      rpc = RPCClient( 'DataManagement/StorageUsage' )
      totalUsage = {}
      totalSize = 0
      for dir in dirs.keys():
        res = rpc.getStorageSummary( dir, '', '', [] )
        if res['OK']:
          for se in sortList( res['Value'].keys() ):
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
  return lfns

def buildBKQuery( bkQuery, prods, fileType, runs ):
  bkFields = ( "ConfigName", "ConfigVersion", "DataTakingConditions", "ProcessingPass", "EventType", "FileType" )
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
    if len( prods ) == 1:
      s = 's'
    else:
      s = ''
  else:
    if not bkQuery:
      return ( None, None )
    if bkQuery[0] == '/':
      bkQuery = bkQuery[1:]
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

  if not transBKQuery.has_key( "FileType" ) and fileType:
    # Add file types
    transBKQuery['FileType'] = fileType

  fileType = transBKQuery.get( 'FileType' )
  if fileType and fileType.lower().find( "all." ) == 0:
    ext = '.' + fileType.split( '.' )[1]
    fileType = []
    bk = BookkeepingClient()
    res = bk.getAvailableFileTypes()

    if res['OK']:
      dbresult = res['Value']
      for record in dbresult['Records']:
        if record[0].endswith( ext ):
          fileType.append( record[0] )
    transBKQuery['FileType'] = fileType

  return ( transBKQuery, requestID )
