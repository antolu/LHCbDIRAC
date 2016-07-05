#!/usr/bin/env python
########################################################################
# File :    dirac-bookkeeping-production-information
# Author :  Zoltan Mathe
########################################################################
"""
  Retrieve information from the Bookkeeping for a given production
"""
__RCSID__ = "$Id$"
import DIRAC
from DIRAC.Core.Base import Script

Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ... ProdID' % Script.scriptName,
                                     'Arguments:',
                                     '  ProdID:   Production ID' ] ) )
Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()
import types

if len( args ) < 1:
  Script.showHelp()

exitCode = 0

from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
bk = BookkeepingClient()
prod = long( args[0] )

res = bk.getProductionInformations( prod )

if res['OK']:
  val = res['Value']
  print "Production Info: "

  infs = val["Production informations"]
  if infs != None:
    for inf in infs:
      if inf[2] != None:
        print '    Configuration Name:', inf[0]
        print '    Configuration Version:', inf[1]
        print '    Event type:', inf[2]

  steps = val['Steps']

  if isinstance( steps, basestring ):
    print steps
  else:

    for i in steps:

      print "-----------------------"
      print " StepName: %s " % ( i[0] )
      print "    StepId             : %s" % ( i[7] )
      print "    ApplicationName    : %s" % ( i[1] )
      print "    ApplicationVersion : %s" % ( i[2] )
      print "    OptionFiles        : %s" % ( i[3] )
      print "    DDB                : %s" % ( i[4] )
      print "    CONDDB             : %s" % ( i[5] )
      print "    ExtraPackages      : %s" % ( i[6] )
      print "    Visible            : %s" % ( i[8] )
      print "-----------------------"
  print "Number of Steps  ", val["Number of jobs"][0][0]
  files = val["Number of files"]
  if len( files ) != 0:
    print "Total number of files:", files[0][2]
  else:
    print "Total number of files: 0"
  for file in files:
    print "         " + str( file[1] ) + ":" + str( file[0] )
  nbevent = val["Number of events"]
  if len( nbevent ) != 0:
    print "Number of events"
    print "File Type".ljust( 20 ) + "Number of events".ljust( 20 ) + "Event Type".ljust( 20 ) + "EventInputStat"
    for i in nbevent:
      print str( i[0] ).ljust( 20 ) + str( i[1] ).ljust( 20 ) + str( i[2] ).ljust( 20 ) + str( i[3] )
  else:
    print "Number of events", 0
  print 'Path: ', val['Path']
else:
  print "ERROR %s: %s" % ( str( prod ), res['Message'] )
  exitCode = 2
DIRAC.exit( exitCode )
