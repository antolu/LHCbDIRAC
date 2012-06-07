#! /usr/bin/env python
"""
   Flush runs in a transformation
"""

import DIRAC
from DIRAC.Core.Base import Script

import re, time, types, string, signal, sys, os, cmd
from LHCbDIRAC.DataManagementSystem.Client.DMScript import DMScript

if __name__ == "__main__":

  dmScript = DMScript()
  dmScript.registerFileSwitches()
  Script.registerSwitch( '', 'NoAction', '   No action taken, just give stats' )
  Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                       'Usage:',
                                       '  %s [option|cfgfile] <TransID> <Status>' % Script.scriptName, ] ) )
  Script.parseCommandLine()

  args = Script.getPositionalArgs()
  action = True
  switches = Script.getUnprocessedSwitches()
  for switch in switches:
    if switch[0] == 'NoAction':
      action = False

  if len( args ) < 1:
    Script.showHelp()
    DIRAC.exit( 0 )

  transID = args[0]

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

  from DIRAC.TransformationSystem.Client.TransformationClient     import TransformationClient
  transClient = TransformationClient()

  for transID in idList:
    res = transClient.getTransformationRuns( {'TransformationID':transID} )
    if not res['OK']:
      print "Error getting runs for transformation %s" % transID, res['Message']
      DIRAC.exit( 1 )

    runs = res['Value']
    runs.sort( cmp=( lambda r1, r2: int( r1['RunNumber'] - r2['RunNumber'] ) ) )

    res = transClient.getBookkeepingQueryForTransformation( transID )
    if not res['OK']:
      print "Error getting BK query for transformation %s" % transID, res['Message']
      DIRAC.exit( 1 )

    queryProd = res['Value'].get( 'ProductionID' )
    if not queryProd:
      print "Transformation is not based on another one"
      DIRAC.exit( 0 )
    res = transClient.getTransformationRuns( {'TransformationID':queryProd} )
    if not res['OK']:
      print "Error getting runs for transformation %s" % queryProd, res['Message']
      DIRAC.exit( 1 )

    queryRuns = res['Value']

    toBeFlushed = []
    for runDict in runs:
      if not runDict['Status'] == 'Flush':
        run = runDict['RunNumber']
        for queryDict in queryRuns:
          if queryDict['RunNumber'] == run:
            missing = -1
            if queryDict['Status'] == 'Flush':
              res = transClient.getTransformationFiles( {'TransformationID': queryProd, 'RunNumber': run} )
              if not res['OK']:
                print "Error getting files for run", run, res['Message']
              else:
                runFiles = res['Value']
                missing = 0
                for file in runFiles:
                  if file['Status'] in ( 'Unused', 'Assigned', 'MaxReset' ):
                    missing += 1
            if not missing:
              toBeFlushed.append( run )
              #print "Run", run, 'should be flushed'
            #else:
              #print "Run", run, 'cannot be flushed (%d)' % missing
            break

    ok = 0
    print "Runs %s flushed in transformation %s:" % ( 'being' if action else 'to be', transID ), toBeFlushed
    for run in toBeFlushed:
      res = transClient.setTransformationRunStatus( transID, run, 'Flush' ) if action else {'OK':True}
      if not res['OK']:
        print "Error setting run %s to Flush in transformation %s" % ( run, transID )
      else:
        ok += 1
    print ok, 'runs set to Flush in transformation', transID

