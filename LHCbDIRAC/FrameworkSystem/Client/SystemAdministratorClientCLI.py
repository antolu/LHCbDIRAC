""" File Catalog Client Command Line Interface. Extension for LHCb, for consumers.
"""

__RCSID__ = "$Id$"

from DIRAC import gLogger
from DIRAC.FrameworkSystem.Client.ComponentMonitoringClient import ComponentMonitoringClient
from DIRAC.FrameworkSystem.Client.SystemAdministratorClientCLI import SystemAdministratorClientCLI as DIRACSystemAdministratorClientCLI
from DIRAC.FrameworkSystem.Client.SystemAdministratorClient import SystemAdministratorClient

class SystemAdministratorClientCLI( DIRACSystemAdministratorClientCLI ):
  """
  """
  def __init__( self, host = None ):
    DIRACSystemAdministratorClientCLI.__init__( self, host )
    self.runitComponents = [ "service", "agent", "executor", "consumer" ]
    self.externalServices = [ "vcycle", "squid" ]

  def do_install( self, args ):
    """
        Install various DIRAC components

        usage:

          install mysql
          install db <database>
          install service <system> <service> [-m <ModuleName>] [-p <Option>=<Value>] [-p <Option>=<Value>] ...
          install agent <system> <agent> [-m <ModuleName>] [-p <Option>=<Value>] [-p <Option>=<Value>] ...
          install executor <system> <executor> [-m <ModuleName>] [-p <Option>=<Value>] [-p <Option>=<Value>] ...
          install consumer <consumer> [-m <ModuleName>] [-p <Option>=<Value>] [-p <Option>=<Value>] ...
    """

    DIRACSystemAdministratorClientCLI.do_install( self, args )

  def do_start( self, args ):
    """ Start services or agents or database server

        usage:

          start <system|*> <service|agent|*>
          start mysql
          start <service name>
    """
    argss = args.split()
    if len( argss ) < 1:
      gLogger.notice( self.do_start.__doc__ )
      return
    option = argss[0]
    del argss[0]

    if option in self.externalServices:
      self.manageService( option, 'start' )
    else:
      DIRACSystemAdministratorClientCLI.do_start( self, args )

  def do_restart( self, args ):
    """ Restart services or agents or database server

        usage:

          restart <system|*> <service|agent|*>
          restart mysql
          restart <service name>
    """
    argss = args.split()
    if len( argss ) < 1:
      gLogger.notice( self.do_restart.__doc__ )
      return
    option = argss[0]
    del argss[0]

    if option in self.externalServices:
      self.manageService( option, 'restart' )
    else:
      DIRACSystemAdministratorClientCLI.do_restart( self, args )

  def do_stop( self, args ):
    """ Stop services or agents or database server

        usage:

          stop <system|*> <service|agent|*>
          stop mysql
          stop <service name>
    """
    argss = args.split()
    if len( argss ) < 1:
      gLogger.notice( self.do_stop.__doc__ )
      return
    option = argss[0]
    del argss[0]

    if option in self.externalServices:
      self.manageService( option, 'stop' )
    else:
      DIRACSystemAdministratorClientCLI.do_stop( self, args )

  def do_status( self, args ):
    """ Check status of services running on the machine

        usage:

          status <service name>
    """

    argss = args.split()
    if len( argss ) < 1:
      gLogger.notice( self.do_stop.__doc__ )
      return
    option = argss[0]
    del argss[0]

    if option in self.externalServices:
      self.manageService( option, 'status' )
    else:
      self.__errMsg( '%s is not a valid service' % option )

  def manageService( self, service, action ):
    """ Manage services running on this machine

      usage:

        service <action> <serviceName>
    """
    client = ComponentMonitoringClient()
    result = client.getInstallations( { 'UninstallationTime': None }, { 'System': 'External', 'Module': service, 'Type': 'External' }, { 'HostName': self.host }, False )
    if not result[ 'OK' ]:
      self.__errMsg( result[ 'Message' ] )
      return
    elif  len( result[ 'Value' ] ) < 1:
      self.__errMsg( '%s is not installed' % ( service ) )
      return

    client = SystemAdministratorClient( self.host, self.port )
    if action == 'start':
      result = client.startService( service )
    elif action == 'stop':
      result = client.stopService( service )
    elif action == 'restart':
      result = client.restartService( service )
    elif action == 'status':
      result = client.statusService( service )

    if not result[ 'OK' ]:
      self.__errMsg( result[ 'Message' ] )
      return

    gLogger.notice( result[ 'Value' ] )
