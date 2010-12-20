#!/usr/bin/env python
########################################################################
# $HeadURL$
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

from LHCbDIRAC.NewBookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
bk = BookkeepingClient()
run = int( args[0] )

res = bk.getRunInformations( run )

if res['OK']:
    val = res['Value']
    print "Run  Informations: "
    print "Run Start:".ljust( 50 ), str( val['RunStart'] )
    print "Run End:".ljust( 50 ), str( val['RunEnd'] )
    print "  Configuration Name:".ljust( 50 ), val['Configuration Name']
    print "  Configuration Version:".ljust( 50 ), val['Configuration Version']
    print "  FillNumber:".ljust( 50 ), val['FillNumber']
    print "  Data taking description: ".ljust( 50 ), val['DataTakingDescription']
    print "  Processing pass: ".ljust( 50 ), val['ProcessingPass']
    print "  Stream: ".ljust( 50 ), val['Stream']
    print "  FullStat: ".ljust( 50 ) + str( val['FullStat'] ).ljust( 50 ) + " Total: ".ljust( 10 ) + str( sum( val['FullStat'] ) )
    print "  Number of events: ".ljust( 50 ) + str( val['Number of events'] ).ljust( 50 ) + " Total:".ljust( 10 ) + str( sum( val['Number of events'] ) )
    print "  Number of file: ".ljust( 50 ) + str( val['Number of file'] ).ljust( 50 ) + " Total: ".ljust( 10 ) + str( sum( val['Number of file'] ) )
    print "  File size: ".ljust( 50 ) + str( val['File size'] ).ljust( 50 ) + " Total: ".ljust( 10 ) + str( sum( val['File size'] ) )

else:
    print "ERROR %s: %s" % ( str( run ), res['Message'] )
    exitCode = 2
DIRAC.exit( exitCode )
