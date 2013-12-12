""" lhcb_ci.component

  LHCb Continuous Integration system libraries.

  @author: mario.ubeda.garcia@cern.ch
  
"""


# lhcb_ci imports
import lhcb_ci.db
import lhcb_ci.extensions
import lhcb_ci.service


# DIRAC imports
from DIRAC                import gConfig
from DIRAC.Core.Base.DB   import DB
from DIRAC.Core.Utilities import InstallTools


class Component( object ):

  
  def __new__( cls, system, component, name ):
    
    if cls is Component:
      if component == 'DB':
        cls = DBComponent
      elif component == 'Service':
        cls = ServiceComponent
      elif component == 'Agent':
        cls = AgentComponent  
      else:
        raise NotImplementedError( 'No component implementation for %s' % component )
    return super( Component, cls ).__new__( cls )

  
  def __init__( self, system, component, name ):
    
    self.system    = system
    self.component = component
    self.name      = name

  
  def _systemName( self ):
    return self.system.replace( 'System', '' )

  #.............................................................................
  
  
  def rawObj( self ):
    """ rawObj
    
    EXTEND ME PLEASE.
    """  
    lhcb_ci.logger.debug( 'rawObj %s: %s/%s' % ( self.component, self._systemName(), self.name ) )

  
  def configure( self ):
    """ configure
    
    EXTEND ME PLEASE.
    """
    lhcb_ci.logger.debug( 'configure %s: %s/%s' % ( self.component, self._systemName(), self.name ) )
  
  
  def install( self ):
    """ install
    
    EXTEND ME PLEASE.
    """  
    lhcb_ci.logger.debug( 'install %s: %s/%s' % ( self.component, self._systemName(), self.name ) )


  def uninstall( self ):
    """ uninstall
    
    EXTEND ME PLEASE.
    """  
    lhcb_ci.logger.debug( 'uninstall %s: %s/%s' % ( self.component, self._systemName(), self.name ) )

  
  def run( self ):
    pass
  
  def stop( self ):
    pass


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
  
  
  def uninstall( self ):
    """ uninstall
    
    This method physically removes the database from the MySQL server.
    
    """
    super( DBComponent, self ).uninstall()
  
    # I know... it is unsafe, but the current version does not work with
    # parametrics... 
    query = "drop database %s" % self.name
    return lhcb_ci.db.execute( query )


  #.............................................................................
  # DBComponent particular methods


  def getTables( self ):

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

  
  def configure( self ):
    
    super( ServiceComponent, self ).configure()
    
    return InstallTools.addDefaultOptionsToCS( gConfig, 'service', self._systemName(), 
                                               self.name, lhcb_ci.extensions.getCSExtensions() )
  
  def run( self ):
    
    systemName = self.system.replace( 'System', '' )
    sReactor = lhcb_ci.service.initializeServiceReactor( systemName , self.name )
    if not sReactor[ 'OK' ]:
      return sReactor
    sReactor = sReactor[ 'Value' ]
    
    # Extract the initialized ServiceReactor        
    self.server, self.serviceName, self.service = lhcb_ci.service.serve( sReactor )

    
  def stop( self ):
    
    return lhcb_ci.service.unserve( self.server )  
       

#...............................................................................


class AgentComponent( Component ):
  pass


#...............................................................................
#EOF
