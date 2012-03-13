#!/usr/bin/env python

#
#  check if a service or an agent is stalled
#
import os, commands, sys
import DIRAC
from DIRAC.Core.Base import Script
Script.addDefaultOptionValue( 'LogLevel', 'verbose' )
Script.parseCommandLine( ignoreErrors = True )

from DIRAC.Core.Utilities.Time import fromString, date, second, dateTime, day, timeInterval
from DIRAC import gLogger


now = dateTime()
runit_dir = '/opt/dirac/startup'
logfile = 'log/current'
pollingtime = 60

gLogger.getSubLogger( "CheckStalledServices" )
gLogger.notice( 'The script ' + Script.scriptName + ' is running at ' + str( now ) )
#print 'The script is runing at '+str(now)

fd = os.listdir( runit_dir )
for dir in fd:
  status, result = commands.getstatusoutput( 'runsvstat ' + runit_dir + '/' + dir )
  filename = runit_dir + '/' + dir + '/' + logfile
  fl = open( filename, "r" )
  listLines = fl.readlines()
  fl.close()
  lastLine = listLines[-1]
  gLogger.info( 'Checking ----> ' + dir )
  for line in listLines:
    if line.find( "WARN: Server is not who" ):
      if line.find( "Polling time" ) != -1:
        try:
          pollingtime = line.split( ':' )[4].split( ' ' )[1].split( '.' )[0]
        except:
          gLogger.error( "    wrong format for Polling Time : " + line )
          break
      else:
        lastLine = line

  lastLineList = lastLine.split( ' ' )
#  print "    the PollingTime for "+dir+" is : "+str(pollingtime)+" s"
  gLogger.debug( "    the PollingTime for " + dir + " is : " + str( pollingtime ) + " s" )
  try:
    lastupdate = fromString( lastLineList[0] + ' ' + lastLineList[1] )
  except:
    gLogger.error( '    EXCEPT : ' + dir )
    pass
  if int( pollingtime ) < 59:
    pollingtime = 120

  interval = timeInterval( lastupdate, second * int( pollingtime ) )
  if not interval.includes( now ):
    gLogger.info( '    last update for ' + dir + ' was : ' + str( lastupdate ) )
    gLogger.info( '    Polling Time is ' + str( pollingtime ) + ' s' )
    gLogger.info( '    last known status' + result + '\n' )
    gLogger.debug( '   Last line is : \n' + lastLine )
