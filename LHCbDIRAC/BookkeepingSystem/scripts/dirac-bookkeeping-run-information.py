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

from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
bk = BookkeepingClient()
run = int( args[0] )

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
  totalLuminosity = val.get( "TotalLuminosity", 0 )

  print "Run  Informations: "
  print "Run Start:".ljust( 50 ), str( runstart )
  print "Run End:".ljust( 50 ), str( runend )
  print "Total luminosity:".ljust( 50 ), str( totalLuminosity )
  print "  Configuration Name:".ljust( 50 ), configname
  print "  Configuration Version:".ljust( 50 ), configversion
  print "  FillNumber:".ljust( 50 ), fillnb
  print "  Data taking description: ".ljust( 50 ), datataking
  print "  Processing pass: ".ljust( 50 ), processing
  print "  Stream: ".ljust( 50 ), stream
  print "  FullStat: ".ljust( 50 ) + str( fullstat ).ljust( 50 ) + " Total: ".ljust( 10 ) + str( sum( fullstat ) )
  print "  Number of events: ".ljust( 50 ) + str( nbofe ).ljust( 50 ) + " Total:".ljust( 10 ) + str( sum( nbofe ) )
  print "  Number of file: ".ljust( 50 ) + str( nboff ).ljust( 50 ) + " Total: ".ljust( 10 ) + str( sum( nboff ) )
  print "  File size: ".ljust( 50 ) + str( fsize ).ljust( 50 ) + " Total: ".ljust( 10 ) + str( sum( fsize ) )

else:
  print "ERROR %s: %s" % ( str( run ), res['Message'] )
  exitCode = 2
DIRAC.exit( exitCode )

