#!/usr/bin/env python

"""
Shows the status of a service
"""

__RCSID__ = "$Id$"

import socket
from DIRAC.FrameworkSystem.Client.ComponentMonitoringClient import ComponentMonitoringClient
from LHCbDIRAC.Core.Utilities import InstallTools

from DIRAC import gConfig, gLogger, S_OK, S_ERROR
InstallTools.exitOnError = True

from DIRAC.Core.Base import Script
from DIRAC import exit as DIRACexit

Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                    'Usage:',
                                    '  %s [option|cfgfile] ... Service' % Script.scriptName,
                                    'Arguments:',
                                    '  Service:  Name of the Service to check'] ) )

Script.parseCommandLine()
args = Script.getPositionalArgs()

if len( args ) < 1:
  Script.showHelp()
  DIRACexit( -1 )

service = args[0]

client = ComponentMonitoringClient()
result = client.getInstallations( { 'UninstallationTime': None }, { 'System': 'External', 'Module': service, 'Type': 'External' }, { 'HostName': socket.getfqdn() }, False )
if not result[ 'OK' ]:
  gLogger.error( 'Error: %s' % result[ 'Message' ] )
  DIRACexit( -1 )
elif  len( result[ 'Value' ] ) < 1:
  gLogger.error( 'Error: %s is not installed' % service )
  DIRACexit( -1 )

result = InstallTools.statusService( service )

if not result[ 'OK' ]:
  gLogger.error( 'Error: %s' % result[ 'Message' ] )
  DIRACexit( -1 )

gLogger.notice( result[ 'Value' ] )
