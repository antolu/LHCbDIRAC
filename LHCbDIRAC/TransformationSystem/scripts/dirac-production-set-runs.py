#!/usr/bin/env python
"""
  Set the Start or End Run for a given Transformation of add a set runs.
"""
__RCSID__ = '$Id: dirac-production-set-run.py$'
"""
Examples of Usage :
  with 1234 = ProdID
  with 99000 = RunNumber
  dirac-production-set-run 1234 --list        (list of runs.)
  dirac-production-set-run 1234 --add 99000   (only for list of runs.)
  dirac-production-set-run 1234 --end 99000   (change endrun.)
  dirac-production-set-run 1234 --start 99000 (change startrun.)

"""

import DIRAC
from DIRAC.Core.Base import Script
Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ... Prod -Option [RunNumber|RunList]' % Script.scriptName,
                                     'Arguments:',
                                     '  Prod:      DIRAC Production Id',
                                     '  RunNumber: New Start or End run',
                                     '  RunList: List of Runs to be added',
                                     'Examples:\n',
                                     'dirac-production-set-run.py 92 --list                        (show the list of runs for production 92)\n',
                                     'dirac-production-set-run.py 92 --add 98200,98201             (add some discrete run to production 92)\n',
                                     'dirac-production-set-run.py 92 --add 98200,98201,99000:99100 (add some discrete run and a range of runs to production 92)\n',
                                     'dirac-production-set-run.py 92 --start 98200                 (change the start run for production 92)\n',
                                     'dirac-production-set-run.py 92 --end   98200                 (change the end run  for production 92)\n'
                                     ] ) )

Script.registerSwitch( '', 'end=', "Specify endrun for the production" )
Script.registerSwitch( '', 'start=', "Specify startrun for the production" )
Script.registerSwitch( '', 'add=', "add List of runs to the production" )
Script.registerSwitch( '', 'End=', "Specify endrun for the production" )
Script.registerSwitch( '', 'Start=', "Specify startrun for the production" )
Script.registerSwitch( '', 'Add=', "add List of runs to the production" )
Script.registerSwitch( '', 'list', "List the runs for the production" )
Script.registerSwitch( '', 'List', "List the runs for the production" )

Script.parseCommandLine ( ignoreErrors = True )

args = Script.getPositionalArgs()
try:
  prodId = int( args[0] )
except:
  print 'Invalid transformation number'
  DIRAC.exit( 1 )

settings = {}
for switch, val in Script.getUnprocessedSwitches():
  opt = switch.lower()
  if opt in ( 'start', 'end' ):
    try:
      settings[opt] = int( val )
    except:
      print "Invalid run number:", val
      DIRAC.exit( 1 )
  elif opt == 'add':
    try:
      settings[opt] = [int( runID ) for runID in val.strip( ',' )]
    except:
      print "Invalid run list", val
      DIRAC.exit( 1 )
  elif opt == 'list':
    settings[opt] = True
if 'add' in settings and ( 'start' in settings or 'end' in settings ):
  print 'Incompatible requests, cannot set run list and start/end run'
  DIRAC.exit( 1 )

from LHCbDIRAC.TransformationSystem.Client.TransformationClient import TransformationClient
client = TransformationClient()
try:
  res = client.getBookkeepingQuery( prodId )
  bkDict = res['Value']
except:
  print "Error retrieving BKQuery for Production %s" % prodId
  DIRAC.exit( 2 )
startRun = bkDict.get( 'StartRun' )
endRun = bkDict.get( 'EndRun' )
runNumbers = bkDict.get( 'RunNumbers' )

if ( 'start' in settings or 'end' in settings ) and runNumbers:
  print "Production %d has RunNumbers key" % prodId
  settings = {'list':True}

if 'add' in settings and ( startRun or endRun ):
  print "Production %d has start run or end run: %s:%s" % ( prodId, str( startRun ), str( endRun ) )
  settings = {'list':True}

if 'add' in settings and ( not runNumbers or runNumbers == 'All' ):
  print "Production %d doesn't have RunNumbers key or set to All" % prodId
  settings = {'list':True}

changed = False
if 'start' in settings:
  changed = True
  runId = settings['start']
  res = client.setBookkeepingQueryStartRunForTransformation( prodId, runId )
  if res['OK']:
    print "Start run of production %d is now %d" % ( prodId, runId )
    startRun = runId
  else:
    print "Error setting start run", res['Message']

if 'end' in settings:
  changed = True
  runId = settings['end']
  res = client.setBookkeepingQueryEndRunForTransformation( prodId, runId )
  if res['OK']:
    print "End run of production %d is now %d" % ( prodId, runId )
    endRun = runId
  else:
    print "Error setting end run", res['Message']

if 'add' in settings:
  changed = True
  runList = [run for run in settings['add'] if run not in runNumbers]
  res = client.addBookkeepingQueryRunListTransformation( prodId, runList )
  if res['OK']:
    print "Run list modified for production %d" % prodId
    runNumbers += runList
  else:
    print "Error modifying run list:", res['Message']

if 'list' in settings:
  print '%sRun selection settings for production %d:' % ( '\n' if changed else '', prodId )
  if runNumbers:
    print "List of runs for: %s" % \
          ','.join( [str( run ) for run in sorted( runNumbers )] )
  else:
    if startRun:
      print "Start run is %s" % startRun
    else:
      print "No start run defined "
    if endRun:
      print "End run is %s" % endRun
    else:
      print "No end run defined "

