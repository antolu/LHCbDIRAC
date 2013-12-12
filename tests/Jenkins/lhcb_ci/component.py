""" lhcb_ci.component

  LHCb Continuous Integration system libraries.

  @author: mario.ubeda.garcia@cern.ch
  
"""

# libraries
import time


# lhcb_ci imports
import lhcb_ci.commons
import lhcb_ci.db
import lhcb_ci.extensions


# DIRAC imports
from DIRAC                                         import gConfig
from DIRAC.ConfigurationSystem.Client              import PathFinder
from DIRAC.Core.Base.DB                            import DB
from DIRAC.Core.DISET.private.Service              import Service
from DIRAC.Core.DISET.private.ServiceConfiguration import ServiceConfiguration
from DIRAC.Core.DISET.RPCClient                    import RPCClient
from DIRAC.Core.DISET.ServiceReactor               import ServiceReactor
from DIRAC.Core.Utilities                          import InstallTools


class Component( object ):

  
  def __new__( cls, system, component, name ):
    
    if cls is Component:
      if component == 'DB':
        cls = DBComponent
      elif component == 'Service':
        cls = ServiceComponent
      elif component == 'Agent':
        cls = AgentComponent
      elif component == 'Client':
        cls = ClientComponent    
      else:
        raise NotImplementedError( 'No component implementation for %s' % component )
    return super( Component, cls ).__new__( cls )

  
  def __init__( self, system, component, name ):
    
    self.system    = system
    self.component = component
    self.name      = name
    
    self.extensions     = lhcb_ci.extensions.getCSExtensions()
    self.params         = {}
    self.activeThreads  = []
    self.currentThreads = []

  def __repr__( self ):
    
    return '%s: %s/%s/%s' % ( self.__class__.__name__, self.system, self.component, self.name )

  
  def _systemName( self ):
    return self.system.replace( 'System', '' )


  def _log( self, method ):
    
    #TODO: what about counting the number of times this method is called...
    
    lhcb_ci.logger.debug( '%s %s: %s/%s' % ( method, self.component, self._systemName(), self.name ) )
    
  #.............................................................................
  
  
  def rawObj( self ):
    """ rawObj
    
    EXTEND ME PLEASE.
    """  
    self._log( 'rawObj' )

  
  def configure( self ):
    """ configure
    
    EXTEND ME PLEASE.
    """
    self._log( 'configure' )
  
  
  def install( self ):
    """ install
    
    EXTEND ME PLEASE.
    """  
    self._log( 'install' )


  def uninstall( self ):
    """ uninstall
    
    EXTEND ME PLEASE.
    """  
    self._log( 'uninstall' )

  
  def run( self ):
    """ run
    
    EXTEND ME PLEASE.
    """  
    self._log( 'run' )
    self.currentThreads, self.activeThreads = lhcb_ci.commons.trackThreads()


  def stop( self ):
    """ stop
    
    EXTEND ME PLEASE.
    """  
    self._log( 'stop' )


#...............................................................................


class DBComponent( Component ):


  def rawObj( self ):
    """ rawObj
    
    This method returns a RAW DB Object.
    
    """
    super( DBComponent, self ).rawObj()

    return DB( self.name, '%s/%s' % ( self._systemName(), self.name ), 10 )

  
  def configure( self ):
    """ configure
  
    This method configures the database in the CS.
      
    """
    super( DBComponent, self ).configure()
  
    return InstallTools.addDatabaseOptionsToCS( gConfig, self._systemName(), self.name )
    
    
  def install( self ):
    """ install
    
    This method installs database using DIRAC standard tools ( first needs to make
    sure it has the MySQL passwords ).
    
    """
    super( DBComponent, self ).install()
    
    # Makes sure InstallTools is aware of the MySQLPasswords
    InstallTools.getMySQLPasswords()
    
    return InstallTools.installDatabase( self.name )

  # Run method in DBComponent does not make much sense. For completeness, we 
  # link it to install.  
  run = install
  
  def uninstall( self ):
    """ uninstall
    
    This method physically removes the database from the MySQL server.
    
    """
    super( DBComponent, self ).uninstall()
  
    # I know... it is unsafe, but the current version does not work with
    # parametrics... 
    query = "drop database %s" % self.name
    return lhcb_ci.db.execute( query )

  # Stop method in DBComponent does not make much sense. For completeness, we 
  # link it to uninstall.
  stop = uninstall


  #.............................................................................
  # DBComponent particular methods


  def getTables( self ):

    self._log( 'getTables' )

    query  = "show tables"
    tables = lhcb_ci.db.execute( query, self.name )
  
    if not tables[ 'OK' ] or not tables[ 'Value' ]:
      return tables
    
    return { 'OK' : True, 'Value' : [ table[0] for table in tables[ 'Value' ] ] }     
    
  
#...............................................................................


class ServiceComponent( Component ):

  
  def __init__( self, *args ):
    
    super( ServiceComponent, self ).__init__( *args )
    self.server      = None
    self.serviceName = None
    self.service     = None
    

  def rawObj( self ):
    """ rawObj
    
    This method returns a RAW DB Object.
    
    """
    super( ServiceComponent, self ).rawObj()

    return Service( { 'loadName'   : self.composeServiceName(), 
                      'modName'    : self.composeServiceName(), 
                      'standalone' : False } )
  
  
  def configure( self ):
    
    super( ServiceComponent, self ).configure()
    
    return InstallTools.addDefaultOptionsToCS( gConfig, 'service', self._systemName(), 
                                               self.name, self.extensions )
  
  
  def install( self ):
    
    super( ServiceComponent, self ).install()
  
    return InstallTools.setupComponent( 'service', self._systemName(), self.name, self.extensions )
  
  
  def uninstall( self ):
    
    super( ServiceComponent, self ).uninstall()
    
    return InstallTools.uninstallComponent( self._systemName(), self.name )
  
  def run( self ):
    
    super( ServiceComponent, self ).run()
    
    sReactor = ServiceReactor()
  
    res = sReactor.initialize( [ self.composeServiceName() ] )
    if not res[ 'OK' ]:
      return res
  
    server = lhcb_ci.service.ServiceThread( sReactor = sReactor )  
    server.start()

    # Let's give two seconds to the thread to wake up a bit..  
    time.sleep( 2 )
  
    serviceName = sReactor._ServiceReactor__services.keys()[ 0 ]
    service     = sReactor._ServiceReactor__services[ serviceName ]

    #FIXME: explain in detail what is going on here
    self.params = { 'server'      : server, 
                    'serviceName' : serviceName,
                    'service'     : service }
    
    return res

  def stop( self ):
    
    super( ServiceComponent, self ).stop()
    
    if not 'server' in self.params:
      return { 'OK' : False, 'Message' : 'No server to be stopped' }
    
    server = self.params[ 'server' ]
    
    # Stop while True
    server.sReactor._ServiceReactor__alive = False
    server.sReactor.closeListeningConnections()
  
    # And delete Service object from dictionary
    #FIXME: maybe we do not need to do this
    #  del sReactor._ServiceReactor__services[ serviceName ]
    
    server.join( 60 )
  
    msg = { 'OK' : True, 'Value' : None } 
    if server.isAlive():
      msg = { 'OK' : False, 'Message' : '%s Server thread is alive' % self.composeServiceName() }
   
    del server.sReactor
  
    threadsAfterPurge = lhcb_ci.commons.killThreads( self.currentThreads )

    if not threadsAfterPurge == self.activeThreads:
      msg = { 'OK' : False, 'Message' : 'Different number of threads !' }
  
    return msg     
  
  
  #.............................................................................
  # ServiceComponent particular methods 
  
  def composeServiceName( self ):
    
    self._log( 'composeServiceName' )
    
    return '%s/%s' % ( self._systemName(), self.name )
  
  def getServicePort( self ):
    """ getServicePort
  
    Given a system and a service, returns its configured port.
    """

    self._log( 'getServicePort' )

    servConf = ServiceConfiguration( [ self.composeServiceName() ] )
    return servConf.getPort()

  def getServiceAuthorization( self ):
    """ getServiceAuthorization
  
    Given a system and a service, returns its configured Authorization rules.
    """

    self._log( 'getServiceAuthorization' )

    servicePath = PathFinder.getServiceSection( self.composeServiceName() )

    return gConfig.getOptionsDict( '%s/Authorization' % servicePath )
 
  def ping( self ):
    
    self._log( 'ping' )
    
    url       = self.params[ 'service' ]._url
    rpcClient = RPCClient( url )  
    
    return rpcClient.ping()

#...............................................................................


class AgentComponent( Component ):
  pass

#...............................................................................

class ClientComponent( Component ):
  pass

#...............................................................................
#EOF
