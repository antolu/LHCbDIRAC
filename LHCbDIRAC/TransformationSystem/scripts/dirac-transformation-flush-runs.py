#! /usr/bin/env python
"""
   In a transformation, flush a list of runs or runs that are flushed in the transformation used in BKQuery
"""

import DIRAC
from DIRAC import gLogger
from DIRAC.Core.Base import Script

if __name__ == "__main__":
  Script.registerSwitch( '', 'Runs=', '   list of runs to flush (comma separated, ranges r1:r2)' )
  Script.registerSwitch( '', 'NoAction', '   No action taken, just give stats' )
  Script.setUsageMessage( '\n'.join( [ __doc__,
                                       'Usage:',
                                       '  %s [option|cfgfile] <TransID>[,<TransID2>...]' % Script.scriptName, ] ) )
  Script.parseCommandLine()

  args = Script.getPositionalArgs()
  action = True
  runList = []
  for switch, val in Script.getUnprocessedSwitches():
    if switch == 'NoAction':
      action = False
    elif switch == 'Runs':
      try:
        runs = val.split( ',' )
        for run in runs:
          if run.isdigit():
            runList.append( int( run ) )
          else:
            runRange = run.split( ':' )
            if len( runRange ) == 2 and runRange[0].isdigit() and runRange[1].isdigit():
              runList += xrange( int( runRange[0] ), int( runRange[1] ) + 1 )
      except Exception as x:
        gLogger.exception( 'Bad run parameter', lException = x )

  if len( args ) != 1:
    gLogger.fatal( "Specify transformation number..." )
    Script.showHelp()
    DIRAC.exit( 0 )
  else:
    ids = args[0].split( "," )
    idList = []
    for id in ids:
      r = id.split( ':' )
      if len( r ) > 1:
        for i in xrange( int( r[0] ), int( r[1] ) + 1 ):
          idList.append( i )
      else:
        idList.append( int( r[0] ) )

  from DIRAC.TransformationSystem.Client.TransformationClient     import TransformationClient
  transClient = TransformationClient()

  for transID in idList:
    res = transClient.getTransformationRuns( {'TransformationID':transID} )
    if not res['OK']:
      gLogger.fatal( "Error getting runs for transformation %s" % transID, res['Message'] )
      DIRAC.exit( 1 )
    runs = res['Value']
    runs.sort( cmp = ( lambda r1, r2: int( r1['RunNumber'] - r2['RunNumber'] ) ) )

    if not runList:
      res = transClient.getBookkeepingQuery( transID )
      if not res['OK']:
        gLogger.fatal( "Error getting BK query for transformation %s" % transID, res['Message'] )
        DIRAC.exit( 1 )
      queryProd = res['Value'].get( 'ProductionID' )
      if not queryProd:
        gLogger.fatal( "Transformation is not based on another one" )
        DIRAC.exit( 0 )

      res = transClient.getTransformationRuns( {'TransformationID':queryProd} )
      if not res['OK']:
        gLogger.fatal( "Error getting runs for transformation %s" % queryProd, res['Message'] )
        DIRAC.exit( 1 )
      queryRuns = res['Value']
      flushedRuns = [run['RunNumber'] for run in queryRuns if run['Status'] == 'Flush']

      toBeFlushed = []
      for run in [run['RunNumber'] for run in runs if run['Status'] != 'Flush' and run['RunNumber'] in flushedRuns]:
        missing = -1
        res = transClient.getTransformationFiles( {'TransformationID': queryProd, 'RunNumber': run} )
        if not res['OK']:
          gLogger.fatal( "Error getting files for run %d" % run, res['Message'] )
        else:
          runFiles = res['Value']
          missing = 0
          for rFile in runFiles:
            if rFile['Status'] in ( 'Unused', 'Assigned', 'MaxReset' ):
              missing += 1
          if not missing:
            toBeFlushed.append( run )

    else:
      toBeFlushed = sorted( set( runList ) & set( [run['RunNumber'] for run in runs] ) )

    ok = 0
    gLogger.always( "Runs %s flushed in transformation %s:" % ( 'being' if action else 'to be', transID ), ','.join( [str( run ) for run in toBeFlushed] ) )
    for run in toBeFlushed:
      res = transClient.setTransformationRunStatus( transID, run, 'Flush' ) if action else {'OK':True}
      if not res['OK']:
        gLogger.fatal( "Error setting run %s to Flush in transformation %s" % ( run, transID ), res['Message'] )
      else:
        ok += 1
    gLogger.always( '%d runs set to Flush in transformation %d' % ( ok, transID ) )

