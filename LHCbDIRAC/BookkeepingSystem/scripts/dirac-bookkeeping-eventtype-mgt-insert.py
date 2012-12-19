#!/usr/bin/env python
########################################################################
# File :    dirac-bookkeeping-eventtype-mgt-insert
# Author :  Zoltan Mathe
########################################################################
"""
  This tool inserts new event types.
    The "<File>" lists the event types on which to operate.
    Each line must have the following format:
    EVTTYPEID="<evant id>", DESCRIPTION="<description>", PRIMARY="<primary description>"
"""
__RCSID__ = "$Id$"

import re
import DIRAC

from DIRAC.Core.Base import Script
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
  """process one event type"""
  try:
    eventline.index( 'EVTTYPEID' )
    eventline.index( 'DESCRIPTION' )
    eventline.index( 'PRIMARY' )
  except ValueError:
    print '\nthe file syntax is wrong!!!\n' + eventline + '\n\n'
    Script.showHelp()
  result = {}
  ma = re.match( "^ *?((?P<id00>EVTTYPEID) *?= *?(?P<value00>[0-9]+)|(?P<id01>DESCRIPTION|PRIMARY) *?= *?\"(?P<value01>.*?)\") *?, *?((?P<id10>EVTTYPEID) *?= *?(?P<value10>[0-9]+)|(?P<id11>DESCRIPTION|PRIMARY) *?= *?\"(?P<value11>.*?)\") *?, *?((?P<id20>EVTTYPEID) *?= *?(?P<value20>[0-9]+)|(?P<id21>DESCRIPTION|PRIMARY) *?= *?\"(?P<value21>.*?)\") *?$", eventline )
  if not ma:
    print "syntax error at: \n" + eventline
    Script.showHelp()
  else:
    for i in range( 3 ):
      if ma.group( 'id' + str( i ) + '0' ):
        if ma.group( 'id' + str( i ) + '0' ) in result:
          print '\nthe parameter ' + ma.group( 'id' + str( i ) + '0' ) + ' cannot appear twice!!!\n' + eventline + '\n\n'
          Script.showHelp()
        else:
          result[ma.group( 'id' + str( i ) + '0' )] = ma.group( 'value' + str( i ) + '0' )
      else:
        if ma.group( 'id' + str( i ) + '1' ) in result:
          print '\nthe parameter ' + ma.group( 'id' + str( i ) + '1' ) + ' cannot appear twice!!!\n' + eventline + '\n\n'
          Script.showHelp()
        else:
          result[ma.group( 'id' + str( i ) + '1' )] = ma.group( 'value' + str( i ) + '1' )
  return result

try:
  events = open( fileName )
except Exception, ex:
  print 'Cannot open file ' + fileName
  DIRAC.exit( 2 )


for line in events:
  res = process_event( line )
  result = bk.insertEventType( res['EVTTYPEID'], res['DESCRIPTION'], res['PRIMARY'] )
  if result["OK"]:
    print result['Value']
  else:
    print "ERROR:", result['Message']
    exitCode = 2

DIRAC.exit( exitCode )
