#! /usr/bin/env python
"""
   In a transformation, flush runs that are flushed in the transformation used in BKQuery
"""

import DIRAC
from DIRAC.Core.Base import Script

if __name__ == "__main__":
  Script.registerSwitch( '', 'NoAction', '   No action taken, just give stats' )
  Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                       'Usage:',
                                       '  %s [option|cfgfile] <TransID>[,<TransID2>...]' % Script.scriptName, ] ) )
  Script.parseCommandLine()

  args = Script.getPositionalArgs()
  action = True
  for switch, val in Script.getUnprocessedSwitches():
    if switch == 'NoAction':
      action = False

  if len( args ) != 1:
    print "Specify transformation number..."
    Script.showHelp()
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

  from DIRAC.TransformationSystem.Client.TransformationClient     import TransformationClient
  transClient = TransformationClient()

  for transID in idList:
    res = transClient.getBookkeepingQuery( transID )
    if not res['OK']:
      print "Error getting BK query for transformation %s" % transID, res['Message']
      DIRAC.exit( 1 )
    queryProd = res['Value'].get( 'ProductionID' )
    if not queryProd:
      print "Transformation is not based on another one"
      DIRAC.exit( 0 )

    res = transClient.getTransformationRuns( {'TransformationID':transID} )
    if not res['OK']:
      print "Error getting runs for transformation %s" % transID, res['Message']
      DIRAC.exit( 1 )
    runs = res['Value']
    runs.sort( cmp = ( lambda r1, r2: int( r1['RunNumber'] - r2['RunNumber'] ) ) )

    res = transClient.getTransformationRuns( {'TransformationID':queryProd} )
    if not res['OK']:
      print "Error getting runs for transformation %s" % queryProd, res['Message']
      DIRAC.exit( 1 )
    queryRuns = res['Value']
    flushedRuns = [run['RunNumber'] for run in queryRuns if run['Status'] == 'Flush']

    toBeFlushed = []
    for run in [run['RunNumber'] for run in runs if run['Status'] != 'Flush' and run['RunNumber'] in flushedRuns]:
      missing = -1
      res = transClient.getTransformationFiles( {'TransformationID': queryProd, 'RunNumber': run} )
      if not res['OK']:
        print "Error getting files for run", run, res['Message']
      else:
        runFiles = res['Value']
        missing = 0
        for rFile in runFiles:
          if rFile['Status'] in ( 'Unused', 'Assigned', 'MaxReset' ):
            missing += 1
        if not missing:
          toBeFlushed.append( run )

    ok = 0
    print "Runs %s flushed in transformation %s:" % ( 'being' if action else 'to be', transID ), toBeFlushed
    for run in toBeFlushed:
      res = transClient.setTransformationRunStatus( transID, run, 'Flush' ) if action else {'OK':True}
      if not res['OK']:
        print "Error setting run %s to Flush in transformation %s" % ( run, transID )
      else:
        ok += 1
    print ok, 'runs set to Flush in transformation', transID

