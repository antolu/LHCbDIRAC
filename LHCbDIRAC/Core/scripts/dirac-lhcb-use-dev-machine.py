#!/usr/bin/env python
"""
    Script that install a new version of LHCbDIRAC, based on DEV and the latest (or coming) version of DIRAC.
    Instructions can be found at
    http://lhcb-release-area.web.cern.ch/LHCb-release-area/DOC/lhcbdirac/rst/html/DevsGuide/DevsGuide.html#developers-installations
"""
__RCSID__ = "$Id"

import DIRAC
from DIRAC.Core.Base import Script
from DIRAC.Core.Security.ProxyInfo import getProxyInfo
from DIRAC.FrameworkSystem.Client.NotificationClient import NotificationClient
from DIRAC.FrameworkSystem.Client.SystemAdministratorClient import SystemAdministratorClient
from DIRAC import gLogger


Script.registerSwitch( 'D', 'days', "Days requested" )
Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] --RequestType <type>' % Script.scriptName, ] ) )
Script.parseCommandLine( ignoreErrors = True )
switches = Script.getUnprocessedSwitches()

daysRequested = '1'

for switch in switches:
  if switch[0].lower() == 'days' or switch[0].lower() == 'd':
    daysRequested = switch[1]

res = getProxyInfo()
if not res[ 'OK' ]:
  gLogger.error( 'Failed to get proxy information', res[ 'Message' ] )
  DIRAC.exit( 2 )

userName = res[ 'Value' ][ 'username' ]

client = SystemAdministratorClient( 'volhcb12.cern.ch' )
print "First, we'll update the software. This can take a while, please wait ..."
result = client.updateSoftware( 'DEV' )
if not result['OK']:
  gLogger.error( "Failed to update the software" )
  print result['Message']
else:
  print "Software successfully updated."
  print "Now, we'll restart the services to use the new software version."
  result = client.restartComponent( '*', '*' )
  if not result['OK']:
    print "All systems are restarted, connection to SystemAdministrator is lost"
  else:
    print "\nComponents started successfully, runit status:\n"
    for comp in result['Value']:
      print comp.rjust( 32 ), ':', result['Value'][comp]['RunitStatus']

subject = 'User ' + userName + ' is now using the development machine'
body = "The duration of the 'token' is set to be %s days.\n" % daysRequested
body = body + "Please don't interfere with %s work, unless stated otherwise." % userName

NotificationClient().sendMail( 'lhcb-dirac@cern.ch', subject, body, '%s@cern.ch' % userName )
DIRAC.exit( 0 )
