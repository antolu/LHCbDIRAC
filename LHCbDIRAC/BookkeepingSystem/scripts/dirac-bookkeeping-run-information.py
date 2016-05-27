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
from DIRAC.Core.Base import Script

Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ... Run' % Script.scriptName,
                                     'Arguments:',
                                     '  Run:      Run Number' ] ) )
Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()

if len( args ) < 1:
  Script.showHelp()

exitCode = 0

try:
  run = int( args[0] )
except ( ValueError, IndexError ) as e:
  DIRAC.gLogger.exception( "Invalid run number", lException = e )
  Script.showHelp()

from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
bk = BookkeepingClient()
res = bk.getRunInformations( run )

if res['OK']:
  val = res['Value']
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

  res = bk.getRunStatus( [run] )
  if res['OK']:
    finished = {'N':'No', 'Y':'Yes'}.get( res['Value']['Successful'].get( run, {} ).get( 'Finished' ), 'Unknown' )
  else:
    finished = res['Message']

  print "Run  Informations: "
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

else:
  print "ERROR %s: %s" % ( str( run ), res['Message'] )
  exitCode = 2
DIRAC.exit( exitCode )

