""" lhcb_ci.component

  LHCb Continuous Integration system libraries.

  @author: mario.ubeda.garcia@cern.ch
  
"""


# lhcb_ci imports
import lhcb_ci.db
import lhcb_ci.service


# DIRAC imports
from DIRAC                import gConfig
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
    pass
  
  def run( self ):
    pass
  
  def stop( self ):
    pass


#...............................................................................


class DBComponent( Component ):

  
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

    # Makes sure InstallTools is aware of the MySQLPasswords
    InstallTools.getMySQLPasswords()
    
    return InstallTools.installDatabase( self.name )
  
  
  def run( self ):
    
    return self.install( self.name )

  
  def stop( self ):
    
    return lhcb_ci.db.dropDB( self.name )

  
#...............................................................................


class ServiceComponent( Component ):

  
  def __init__( self, *args ):
    
    super( ServiceComponent, self ).__init__( *args )
    self.server      = None
    self.serviceName = None
    self.service     = None

  
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
