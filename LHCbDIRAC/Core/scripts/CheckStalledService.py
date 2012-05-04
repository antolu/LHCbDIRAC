#!/usr/bin/env python

#
"""  check if a service or an agent is stalled """

__RCSID__ = "$Id$"

#
import os, commands
from DIRAC.Core.Base import Script
Script.addDefaultOptionValue( 'LogLevel', 'verbose' )
Script.parseCommandLine( ignoreErrors = True )

from DIRAC.Core.Utilities.Time import fromString, second, dateTime, timeInterval
from DIRAC.Interfaces.API.DiracAdmin                         import DiracAdmin
from DIRAC import gLogger


now = dateTime()
runit_dir = '/opt/dirac/startup'
logfile = 'log/current'
pollingtime = 60
diracAdmin = DiracAdmin()
mailadress = 'lhcb-grid-shifter-oncall@cern.ch'
subject = 'CheckStalled'
msg = 'List of Services / agents which could be stalled \n\n'


def write_log( mesg ):
  """ create the log file """
  global msg
  gLogger.notice( mesg )
  msg += mesg + '\n'

gLogger.getSubLogger( "CheckStalledServices" )
write_log( 'The script ' + Script.scriptName + ' is running at ' + str( now ) )

fd = os.listdir( runit_dir )
for dirname in fd:
  status, result = commands.getstatusoutput( 'runsvstat ' + runit_dir + '/' + dirname )
  filename = runit_dir + '/' + dirname + '/' + logfile
  fl = open( filename, "r" )
  listLines = fl.readlines()
  fl.close()
  lastLine = listLines[-1]
  write_log( 'Checking ----> ' + dirname )
  for line in listLines:
    if line.find( "WARN: Server is not who" ):
      if line.find( "Polling time" ) != -1:
        try:
          pollingtime = line.split( ':' )[4].split( ' ' )[1].split( '.' )[0]
        except:
          write_log( "    wrong format for Polling Time : " + line )
          break
      else:
        lastLine = line

  lastLineList = lastLine.split( ' ' )
  try:
    lastupdate = fromString( lastLineList[0] + ' ' + lastLineList[1] )
  except:
    write_log( '    EXCEPT : ' + dirname )

  if int( pollingtime ) < 59:
    pollingtime = 120

  interval = timeInterval( lastupdate, second * int( pollingtime ) )
  if not interval.includes( now ):
    write_log( "    the PollingTime is : " + str( pollingtime ) + " s" )
    write_log( '    last update for ' + dirname + ' was : ' + str( lastupdate ) )
    write_log( '    Polling Time is ' + str( pollingtime ) + ' s' )
    write_log( '    last known status' + result + '\n' )
    write_log( '   Last line is : \n' + lastLine )

res = diracAdmin.sendMail( mailadress, subject, msg, fromAddress = 'joel.closier@cern.ch' )
if not res[ 'OK' ]:
  print 'The mail could not be sent'
