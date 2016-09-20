#!/usr/bin/env python
########################################################################
# File :    dirac-bookkeeping-run-informations
# Author :  Zoltan Mathe
########################################################################
"""
  Retrieve from Bookkeeping information for a given run
"""
__RCSID__ = "$Id$"
import DIRAC
from DIRAC import gLogger, S_OK
from DIRAC.Core.Base import Script

Script.registerSwitch( '', 'Production=', '   <prodID>, get the run list from a production' )
Script.registerSwitch( '', 'Active', '   only get Active runs' )
Script.registerSwitch( '', 'Information=', '   <item> returns only the relevant information item' )
Script.registerSwitch( '', 'ByValue', '   if set, the information is a list of runs for each value of the information item' )
Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ... Run' % Script.scriptName,
                                     'Arguments:',
                                     '  Run:      Run Number' ] ) )
Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()

runSet = set()
for arg in args:
  try:
    runSet.update( [int( run ) for run in arg.split( ',' )] )
  except ( ValueError, IndexError ) as e:
    gLogger.exception( "Invalid run number", arg, lException = e )
    DIRAC.exit( 1 )

production = None
item = None
byValue = False
active = False
for switch in Script.getUnprocessedSwitches():
  if switch[0] == 'Production':
    try:
      production = [int( prod ) for prod in switch[1].split( ',' )]
    except ValueError as e:
      gLogger.exception( 'Bad production ID', lException = e )
      DIRAC.exit( 1 )
  elif switch[0] == 'Information':
    item = switch[1]
  elif switch[0] == 'ByValue':
    byValue = True
  elif switch[0] == 'Active':
    active = True


from LHCbDIRAC.DataManagementSystem.Client.DMScript import printDMResult, ProgressBar
from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
bk = BookkeepingClient()

if production:
  from LHCbDIRAC.TransformationSystem.Client.TransformationClient import TransformationClient
  trClient = TransformationClient()
  condDict = {'TransformationID': production}
  if active:
    conDict['Status'] = 'Active'
  res = trClient.getTransformationRuns( condDict )
  if res['OK']:
    runSet.update( run['RunNumber'] for run in res['Value'] )
  else:
    gLogger.fatal( "Error getting production runs", res['Message'] )
    DIRAC.exit( 2 )
  gLogger.notice( "Found %d runs" % len( runSet ) )

sep = ''
if item:
  result = {"Successful":{}, "Failed":{}}
  success = result['Successful']
  failed = result['Failed']
  if item == 'Finished':
    # We can get it in a single call
    res = bk.getRunStatus( list( runSet ) )
    failed.update( res['Value']['Failed'] )
    for run in res['Value']['Successful']:
      finished = {'N':'No', 'Y':'Yes'}.get( res['Value']['Successful'].get( run, {} ).get( 'Finished' ), 'Unknown' )
      if byValue:
        success.setdefault( finished, [] ).append( run )
      else:
        success[run] = finished
    if byValue:
      for val in success:
        success[val] = ','.join( [str( run ) for run in success[val]] )
    printDMResult( S_OK( result ), empty = 'None', script = 'dirac-bookkeeping-run-information' )
    DIRAC.exit( 0 )

  progressBar = ProgressBar( len( runSet ), title = "Getting %s for %d runs" % ( item, len( runSet ) ), step = 10 )

for run in sorted( runSet ):
  if item:
    progressBar.loop()
  res = bk.getRunStatus( [run] )
  if res['OK']:
    finished = {'N':'No', 'Y':'Yes'}.get( res['Value']['Successful'].get( run, {} ).get( 'Finished' ), 'Unknown' )
    if item == 'Finished':
      if byValue:
        success.setdefault( finished, [] ).append( run )
      else:
        success[run] = finished
      continue
  elif item == 'Finished':
    result['Failed'][run] = res['Message']
    continue
  else:
    finished = 'Unknown'

  res = bk.getRunInformations( run )

  if res['OK']:
    val = res['Value']
    if item:
      if item not in val:
        gLogger.error( "Item not found", "\n Valid items: %s" % str( sorted( val ) ) )
        DIRAC.exit( 3 )
      itemValue = val.get( item, "Unknown" )
      if byValue:
        success.setdefault( itemValue, [] ).append( run )
      else:
        result['Successful'][run] = itemValue
      continue
    runstart = val.get( 'RunStart', 'Unknown' )
    runend = val.get( 'RunEnd', 'Unknown' )
    configname = val.get( 'Configuration Name', 'Unknown' )
    configversion = val.get( 'Configuration Version', 'Unknown' )
    fillnb = val.get( 'FillNumber', 'Unknown' )
    datataking = val.get( 'DataTakingDescription', 'Unknown' )
    processing = val.get( 'ProcessingPass', 'Unknown' )
    stream = val.get( 'Stream', 'Unknown' )
    fullstat = val.get( 'FullStat', 'Unknown' )
    nbofe = val.get( 'Number of events', 'Unknown' )
    nboff = val.get( 'Number of file', 'Unknown' )
    fsize = val.get( 'File size', 'Unknown' )
    totalLuminosity = val.get( "TotalLuminosity", 'Unknown' )
    tck = val.get( 'Tck', 'Unknown' )

    if sep:
      print sep
    print "Run  Informations for run %d: " % run
    print "Run Start:".ljust( 30 ), str( runstart )
    print "Run End:".ljust( 30 ), str( runend )
    print "Total luminosity:".ljust( 30 ), str( totalLuminosity )
    print "  Configuration Name:".ljust( 30 ), configname
    print "  Configuration Version:".ljust( 30 ), configversion
    print "  FillNumber:".ljust( 30 ), fillnb
    print "  Finished:".ljust( 30 ), finished
    print "  Data taking description:".ljust( 30 ), datataking
    print "  Processing pass:".ljust( 30 ), processing
    print "  TCK:".ljust( 30 ), tck
    print "  Stream:".ljust( 30 ), stream
    just = len( str( fsize ) ) + 3
    print "  FullStat:".ljust( 30 ), str( fullstat ).ljust( just ), " Total: ".ljust( 10 ) + str( sum( fullstat ) )
    print "  Number of events:".ljust( 30 ), str( nbofe ).ljust( just ), " Total:".ljust( 10 ) + str( sum( nbofe ) )
    print "  Number of files:".ljust( 30 ), str( nboff ).ljust( just ), " Total: ".ljust( 10 ) + str( sum( nboff ) )
    print "  File size:".ljust( 30 ), str( fsize ).ljust( just ), " Total: ".ljust( 10 ) + str( sum( fsize ) )
    sep = 20 * '='
  else:
    if item:
      result['Failed'][run] = res['Message']
      continue
    DIRAC.gLogger.fatal( "ERROR for run %s:" % run, res['Message'] )
    DIRAC.exit( 1 )

if item:
  progressBar.endLoop()
  if byValue:
    for val in success:
      success[val] = ','.join( [str( run ) for run in success[val]] )
  printDMResult( S_OK( result ), empty = 'None', script = 'dirac-bookkeeping-run-information' )
