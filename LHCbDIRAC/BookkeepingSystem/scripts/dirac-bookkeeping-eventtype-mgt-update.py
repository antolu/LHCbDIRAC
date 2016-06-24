#!/usr/bin/env python
########################################################################
# File :    dirac-bookkeeping-eventtype-mgt-update
# Author :  Zoltan Mathe
########################################################################
"""
  This tool updates event types
    The "<File>" lists the event types on which to operate.
    Each line must have the following format:
    EVTTYPEID="<evant id>", DESCRIPTION="<description>", PRIMARY="<primary description>"
"""
__RCSID__ = "$Id$"

import re
import DIRAC
from DIRAC.Core.Base import Script
from DIRAC import gLogger

Script.setUsageMessage( '\n'.join( [ '\n'.join( __doc__.split( '\n' )[1:5] ),
                                     'Usage:',
                                     '  %s [option|cfgfile] ... File' % Script.scriptName,
                                     'Arguments:',
                                     '  File:     Name of the file including the description of the Types' ] ) )
Script.parseCommandLine( ignoreErrors = True )

from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
bk = BookkeepingClient()

args = Script.getPositionalArgs()

if len( args ) != 1:
  Script.showHelp()

exitCode = 0

fileName = args[0]


def process_event( eventline ):
  """process one event"""
  try:
    eventline.index( 'EVTTYPEID' )
    eventline.index( 'DESCRIPTION' )
    eventline.index( 'PRIMARY' )
  except ValueError:
    gLogger.error( '\nthe file syntax is wrong!!!\n' + eventline + '\n\n')
    Script.showHelp()
  parameters = eventline.split( ',' )
  result = {}
  ma = re.match( "^ *?((?P<id00>EVTTYPEID) *?= *?(?P<value00>[0-9]+)|(?P<id01>DESCRIPTION|PRIMARY) *?= *?\"(?P<value01>.*?)\") *?, *?((?P<id10>EVTTYPEID) *?= *?(?P<value10>[0-9]+)|(?P<id11>DESCRIPTION|PRIMARY) *?= *?\"(?P<value11>.*?)\") *?, *?((?P<id20>EVTTYPEID) *?= *?(?P<value20>[0-9]+)|(?P<id21>DESCRIPTION|PRIMARY) *?= *?\"(?P<value21>.*?)\") *?$", eventline )
  if not ma:
    gLogger.error("syntax error at: \n" + eventline)
    Script.showHelp()
  else:
    for i in range( 3 ):
      if ma.group( 'id' + str( i ) + '0' ):
        if ma.group( 'id' + str( i ) + '0' ) in result:
          gLogger.error( '\nthe parameter ' + ma.group( 'id' + str( i ) + '0' ) + ' cannot appear twice!!!\n' + eventline + '\n\n')
          Script.showHelp()
        else:
          result[ma.group( 'id' + str( i ) + '0' )] = ma.group( 'value' + str( i ) + '0' )
      else:
        if ma.group( 'id' + str( i ) + '1' ) in result:
          gLogger.error( '\nthe parameter ' + ma.group( 'id' + str( i ) + '1' ) + ' cannot appear twice!!!\n' + eventline + '\n\n')
          Script.showHelp()
        else:
          result[ma.group( 'id' + str( i ) + '1' )] = ma.group( 'value' + str( i ) + '1' )
  return result

eventtypes = []
try:
  with open( fileName ) as fd:
    for line in fd:
      evt = process_event( line )
      eventtypes.append( evt )
except IOError:
  gLogger.error( 'Cannot open file ' + fileName )
  DIRAC.exit( 2 )

result = bk.bulkupdateEventType( eventtypes )
if not result['OK']:
  gLogger.error( result['Message'] )
  exitCode = 2
else:
  if result['Value']['Failed']:
    gLogger.error( "Failed to update the following event types:" )
    for evt in result['Value']['Failed']:
      for i in evt.itervalues():
        gLogger.error( "%s : %s" % ( repr( i.get( 'EvtentType' ) ), i.get( 'Error' ) ) )
   
  if result['Value']['Successful']:
     gLogger.notice( "The following event types are updated: %s" % repr( result['Value']['Successful'] ) )
     
DIRAC.exit( exitCode )
