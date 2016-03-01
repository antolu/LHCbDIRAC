#!/usr/bin/env python

"""
Populates the database with the current installations of components
This script assumes that the InstalledComponentsDB, the
ComponentMonitoring service and the Notification service are installed and running
"""

__RCSID__ = "$Id$"

import sys
from datetime import datetime
from DIRAC import exit as DIRACexit
from DIRAC import S_OK, gLogger, gConfig
from DIRAC.ConfigurationSystem.Client.CSAPI import CSAPI
from DIRAC.ConfigurationSystem.Client.Helpers.CSGlobals import getSetup
from DIRAC.Core.Base import Script
from DIRAC.FrameworkSystem.Client.NotificationClient    import NotificationClient
from DIRAC.FrameworkSystem.Client.SystemAdministratorIntegrator \
  import SystemAdministratorIntegrator
from DIRAC.FrameworkSystem.Client.ComponentMonitoringClient \
  import ComponentMonitoringClient
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations

global excludedHosts
excludedHosts = []

def setExcludedHosts( value ):
  global excludedHosts

  excludedHosts = value.split( ',' )
  return S_OK()

Script.registerSwitch( "e:", "exclude=", "Comma separated list of hosts to be excluded from the scanning process", setExcludedHosts )


Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                        'Usage:',
                        '  %s [option|cfgfile] ... [debug]' % Script.scriptName ] ) )

Script.parseCommandLine( ignoreErrors = False )
args = Script.getPositionalArgs()

services = [ 'vcycle', 'squid' ]
componentType = ''

# Get my setup
mySetup = gConfig.getValue( 'DIRAC/Setup' )

monitoringClient = ComponentMonitoringClient()

# Retrieve information from all the hosts
client = SystemAdministratorIntegrator( exclude = excludedHosts )
resultAll = client.getOverallStatus()

notificationClient = NotificationClient()
for host in resultAll[ 'Value' ]:
  if not resultAll[ 'Value' ][ host ][ 'OK' ]:
    # If the host cannot be contacted, exclude it and send message
    excludedHosts.append( host )

    result = notificationClient.sendMail( Operations().getValue( 'EMail/Production', [] ), 'Unreachable host', '\ndirac-populate-component-db: Could not fill the database with the components from unreachable host %s\n' % host )
    if not result[ 'OK' ]:
      gLogger.error( 'Can not send unreachable host notification mail: %s' % result[ 'Message' ] )

if not resultAll[ 'OK' ]:
  gLogger.error( resultAll[ 'Message' ] )
  DIRACexit( -1 )
resultHosts = client.getHostInfo()
if not resultHosts[ 'OK' ]:
  gLogger.error( resultHosts[ 'Message' ] )
  DIRACexit( -1 )
resultInfo = client.getInfo()
if not resultInfo[ 'OK' ]:
  gLogger.error( resultInfo[ 'Message' ] )
  DIRACexit( -1 )
resultMySQL = client.getMySQLStatus()
if not resultMySQL[ 'OK' ]:
  gLogger.error( resultMySQL[ 'Message' ] )
  DIRACexit( -1 )
resultAllDB = client.getDatabases()
if not resultAllDB[ 'OK' ]:
  gLogger.error( resultAllDB[ 'Message' ] )
  DIRACexit( -1 )
resultAvailableDB = client.getAvailableDatabases()
if not resultAvailableDB[ 'OK' ]:
  gLogger.error( resultAvailableDB[ 'Message' ] )
  DIRACexit( -1 )

resultService = {}
for service in services:
  resultService[ service ] = client.statusService( service )
  if not resultService[ service ][ 'OK' ]:
    gLogger.error( resultService[ service ][ 'Message' ] )
    DIRACexit( -1 )

records = []
finalSet = list( set( resultAll[ 'Value' ] ) - set( excludedHosts ) )
for host in finalSet:
  hasMySQL = True
  result = resultAll[ 'Value' ][ host ]
  hostResult = resultHosts[ 'Value' ][ host ]
  infoResult = resultInfo[ 'Value' ][ host ]
  mySQLResult = resultMySQL[ 'Value' ][ host ]
  allDBResult = resultAllDB[ 'Value' ][ host ]
  availableDBResult = resultAvailableDB[ 'Value' ][ host ]

  if not result[ 'OK' ]:
    gLogger.error( 'Host %s: %s' % ( host, result[ 'Message' ] ) )
    continue
  if not hostResult[ 'OK' ]:
    gLogger.error( 'Host %s: %s' % ( host, hostResult[ 'Message' ] ) )
    continue
  if not infoResult[ 'OK' ]:
    gLogger.error( 'Host %s: %s' % ( host, infoResult[ 'Message' ] ) )
    continue
  if mySQLResult[ 'OK' ]:
    if not allDBResult[ 'OK' ]:
      gLogger.error( 'Host %s: %s' % ( host, allDBResult[ 'Message' ] ) )
      continue
    if not availableDBResult[ 'OK' ]:
      gLogger.error( 'Host %s: %s' % ( host, availableDBResult[ 'Message' ] ) )
      continue
  else:
    hasMySQL = False

  setup = infoResult[ 'Value' ][ 'Setup' ]
  if setup != mySetup:
    continue

  cpu = hostResult[ 'Value' ][ 'CPUModel' ].strip()
  rDict = result[ 'Value' ]
  # Components other than databases
  for compType in rDict:
    if componentType and componentType != compType:
      continue
    for system in rDict[ compType ]:
      components = rDict[ compType ][ system ].keys()
      components.sort()
      for component in components:
        record = { 'Installation': {}, 'Component': {}, 'Host': {} }
        if rDict[ compType ][ system ][ component ][ 'Installed' ] and \
            component != 'ComponentMonitoring':
          runitStatus = \
              str( rDict[ compType ][ system ][ component ][ 'RunitStatus' ] )
          if runitStatus != 'Unknown':
            module = \
                  str( rDict[ compType ][ system ][ component ][ 'Module' ] )
            record[ 'Component' ][ 'System' ] = system
            record[ 'Component' ][ 'Module' ] = module
            # Transform 'Services' into 'service', 'Agents' into 'agent' ...
            record[ 'Component' ][ 'Type' ] = compType.lower()[ :-1 ]
            record[ 'Host' ][ 'HostName' ] = host
            record[ 'Host' ][ 'CPU' ] = cpu
            record[ 'Installation' ][ 'Instance' ] = component
            record[ 'Installation' ][ 'InstallationTime' ] = datetime.utcnow()
            records.append( record )

  # Databases
  csClient = CSAPI()
  cfg = csClient.getCurrentCFG()[ 'Value' ]

  if hasMySQL:
    allDB = allDBResult[ 'Value' ]
    availableDB = availableDBResult[ 'Value' ]

    for db in allDB:
      # Check for DIRAC only databases
      if db in availableDB.keys() and db != 'InstalledComponentsDB':
        # Check for 'installed' databases
        isSection = cfg.isSection \
                      ( 'Systems/' + availableDB[ db ][ 'System' ] + '/' +
                        cfg.getOption( 'DIRAC/Setups/' + setup + '/' +
                        availableDB[ db ][ 'System' ] ) + '/Databases/' + db +
                       '/' )
        if isSection:
          record = { 'Installation': {}, 'Component': {}, 'Host': {} }
          record[ 'Component' ][ 'System' ] = availableDB[ db ][ 'System' ]
          record[ 'Component' ][ 'Module' ] = db
          record[ 'Component' ][ 'Type' ] = 'DB'
          record[ 'Host' ][ 'HostName' ] = host
          record[ 'Host' ][ 'CPU' ] = cpu
          record[ 'Installation' ][ 'Instance' ] = db
          record[ 'Installation' ][ 'InstallationTime' ] = datetime.utcnow()
          records.append( record )

  # Services
  serviceResult = {}
  for service in services:
    serviceResult[ service ] = resultService[ service ][ 'Value' ][ host ]
    if not serviceResult[ service ][ 'OK' ]:
      gLogger.warn( 'Host %s: %s' % ( host, serviceResult[ service ][ 'Message' ] ) )
      continue

    # Is the service already in the database?
    result = monitoringClient.installationExists \
                                  ( { 'UnInstallationTime': None,
                                      'Instance': service },
                                    { 'System': 'External',
                                      'Module': service,
                                      'Type': 'external' },
                                    { 'HostName': host,
                                      'CPU': cpu } )
    if not result[ 'OK' ]:
      gLogger.error( 'Host %s: %s' % ( host, result[ 'Message' ] ) )
    else:
      if result[ 'Value' ]:
        gLogger.warn( 'Host %s: Installation of %s already exists in database' % ( host, service ) )
      else:
        record = { 'Installation': {}, 'Component': {}, 'Host': {} }
        record[ 'Component' ][ 'System' ] = 'External'
        record[ 'Component' ][ 'Module' ] = service
        record[ 'Component' ][ 'Type' ] = 'external'
        record[ 'Host' ][ 'HostName' ] = host
        record[ 'Host' ][ 'CPU' ] = cpu
        record[ 'Installation' ][ 'Instance' ] = service
        record[ 'Installation' ][ 'InstallationTime' ] = datetime.utcnow()
        records.append( record )

# Add the installations to the database
for record in records:
  result = monitoringClient.addInstallation \
    ( record[ 'Installation' ], record[ 'Component' ], record[ 'Host' ], True )
  if not result[ 'OK' ]:
    gLogger.error( result[ 'Message' ] )
