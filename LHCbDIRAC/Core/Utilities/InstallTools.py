########################################################################
# $HeadURL$
########################################################################

"""
Collection of Tools for installation of DIRAC components:
MySQL, DB's, Services's, Agents
"""

__RCSID__ = "$Id$"

import os
import subprocess
import DIRAC
from DIRAC.Core.Utilities import InstallTools
from DIRAC import gConfig, gLogger, S_OK, S_ERROR

def startService( service ):
  """
  Start the given service
  """

  # The commands to execute are hard-coded to avoid shell injection
  if service == 'vcycle':
    command = '/etc/rc.d/init.d/vcycled start'
  elif service == 'squid':
    command = 'service squid start'# '/etc/rc.d/init.d/squid start'
  else:
    return S_ERROR( 'Service %s does not exist' % ( service ) )

  try:
    result = subprocess.call( command, shell = True )
  except Exception, e:
    return S_ERROR( e )

  if result == 0:
    return S_OK( 'Service %s started correctly' % ( service ) )
  else:
    return S_ERROR( 'Service %s couldn\'t be started' % ( service ) )

def stopService( service ):
  """
  Stop the given service
  """

  # The commands to execute are hard-coded to avoid shell injection
  if service == 'vcycle':
    command = '/etc/rc.d/init.d/vcycled stop'
  elif service == 'squid':
    command = 'service squid stop'
  else:
    return S_ERROR( 'Service %s does not exist' % ( service ) )

  try:
    result = subprocess.call( command, shell = True )
  except Exception, e:
    return S_ERROR( e )

  if result == 0:
    return S_OK( 'Service %s stopped correctly' % ( service ) )
  else:
    return S_ERROR( 'Service %s couldn\'t be stopped' % ( service ) )

def restartService( service ):
  """
  Restart the given service
  """

  # The commands to execute are hard-coded to avoid shell injection
  if service == 'vcycle':
    command = '/etc/rc.d/init.d/vcycled restart'
  elif service == 'squid':
    command = 'service squid restart'
  else:
    return S_ERROR( 'Service %s does not exist' % ( service ) )

  try:
    result = subprocess.call( command, shell = True )
  except Exception, e:
    return S_ERROR( e )

  if result == 0:
    return S_OK( 'Service %s restarted correctly' % ( service ) )
  else:
    return S_ERROR( 'Service %s couldn\'t be restarted' % ( service ) )

def statusService( service ):
  """
  Return the status of the given service
  """

  # The commands to execute are hard-coded to avoid shell injection
  if service == 'vcycle':
    command = '/etc/rc.d/init.d/vcycled status'
  elif service == 'squid':
    command = 'service squid status'
  else:
    return S_ERROR( 'Service %s does not exist' % ( service ) )

  try:
    result = subprocess.call( command, shell = True )
  except Exception, e:
    return S_ERROR( e )

  if result == 0:
    return S_OK( 'Service %s is running' % ( service ) )
  elif result == 3:
    return S_OK( 'Service %s is not running' % ( service ) )
  else:
    return S_ERROR( 'Status of service %s is unknown' % ( service ) )

def installComponent( componentType, system, component, extensions, componentModule = '', checkModule = True ):
  """
  Install runit directory for the specified component
  """
  # Check if the component is already installed
  runitCompDir = os.path.join( InstallTools.runitDir, system, component )
  if os.path.exists( runitCompDir ):
    msg = "%s %s_%s already installed" % ( componentType, system, component )
    gLogger.notice( msg )
    return S_OK( runitCompDir )

  # Check that the software for the component is installed
  # Any "Load" or "Module" option in the configuration defining what modules the given "component"
  # needs to load will be taken care of by checkComponentModule.
  if checkModule:
    cModule = componentModule
    if not cModule:
      cModule = component
    result = InstallTools.checkComponentModule( componentType, system, cModule )
    if not result['OK']:
      if not InstallTools.checkComponentSoftware( componentType, system, cModule, extensions )['OK'] and componentType != 'executor':
        error = 'Software for %s %s/%s is not installed' % ( componentType, system, component )
        if InstallTools.exitOnError:
          gLogger.error( error )
          DIRAC.exit( -1 )
        return S_ERROR( error )

  gLogger.notice( 'Installing %s %s/%s' % ( componentType, system, component ) )

  # Retrieve bash variables to be set
  result = gConfig.getOption( 'DIRAC/Setups/%s/%s' % ( InstallTools.CSGlobals.getSetup(), system ) )
  if not result[ 'OK' ]:
    return result
  instance = result[ 'Value' ]

  result = InstallTools.getComponentCfg( componentType, system, component, instance, extensions )
  if not result[ 'OK' ]:
    return result
  compCfg = result[ 'Value' ]

  result = InstallTools._getSectionName( componentType )
  if not result[ 'OK' ]:
    return result
  section = result[ 'Value' ]

  bashVars = ''
  if compCfg.isSection( 'Systems/%s/%s/%s/%s/Environment' % ( system, instance, section, component ) ):
    dictionary = compCfg.getAsDict()
    bashSection = dictionary[ 'Systems' ][ system ][ instance ][ section ][ component ][ 'BashVariables' ]
    for var in bashSection:
      bashVars = '%s\nexport %s=%s' % ( bashVars, var, bashSection[ var ] )

  # Now do the actual installation
  try:
    componentCfg = os.path.join( InstallTools.linkedRootPath, 'etc', '%s_%s.cfg' % ( system, component ) )
    if not os.path.exists( componentCfg ):
      fd = open( componentCfg, 'w' )
      fd.close()

    InstallTools._createRunitLog( runitCompDir )

    execute = 'exec python'
    if componentType == 'consumer':
      execute = '# Consumers do not have a dirac-consumer script, they are run manually\n%s $DIRAC/LHCbDIRAC/%sSystem/Consumer/%s.py' % ( execute, system, component )
    else:
      execute = '%s $DIRAC/DIRAC/Core/scripts/dirac-%s.py %s/%s %s < /dev/null' % ( execute, componentType, system, component, componentCfg )

    runFile = os.path.join( runitCompDir, 'run' )
    fd = open( runFile, 'w' )
    fd.write( 
"""#!/bin/bash
rcfile=%(bashrc)s
[ -e $rcfile ] && source $rcfile
#
exec 2>&1
#
[ "%(componentType)s" = "agent" ] && renice 20 -p $$
#%(bashVariables)s
#
%(execute)s
""" % {'bashrc': os.path.join( InstallTools.instancePath, 'bashrc' ),
       'bashVariables': bashVars,
       'componentType': componentType,
       'execute': execute } )
    fd.close()

    os.chmod( runFile, InstallTools.gDefaultPerms )

    if componentType.lower() == 'agent':
      stopFile = os.path.join( runitCompDir, 'control', 't' )
      fd = open( stopFile, 'w' )
      fd.write( 
"""#!/bin/bash
echo %(controlDir)s/%(system)s/%(component)s/stop_agent
touch %(controlDir)s/%(system)s/%(component)s/stop_agent
""" % {'controlDir': runitDir,
       'system' : system,
       'component': component } )
      fd.close()

      os.chmod( stopFile, InstallTools.gDefaultPerms )

  except Exception:
    error = 'Failed to prepare setup for %s %s/%s' % ( componentType, system, component )
    gLogger.exception( error )
    if InstallTools.exitOnError:
      DIRAC.exit( -1 )
    return S_ERROR( error )

  result = InstallTools.execCommand( 5, [runFile] )

  gLogger.notice( result['Value'][1] )

  return S_OK( runitCompDir )

InstallTools.loadDiracCfg()
InstallTools.COMPONENT_TYPES = [ 'service', 'agent', 'executor', 'consumer' ]
InstallTools.installComponent = installComponent
