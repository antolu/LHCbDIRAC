""" lhcb_ci.test.test_configure

  LHCb Continuous Integration system libraries.

  @author: mario.ubeda.garcia@cern.ch
  
"""


import os

import lhcb_ci
import lhcb_ci.basecase 
import lhcb_ci.db
import lhcb_ci.service


class Configure_Test( lhcb_ci.basecase.Service_TestCase ):
  """ Configure_Test
  
  This class contains dirty & sticky tests. The configuration steps have been
  transformed into simple unittests, which are run here. Dirty & sticky because
  the tests will alter the CS structure, adding the necessary configuration 
  parameters to be able to run the rest of the tests. 
  
  Disclaimer: do not change the name of the tests, as some of them need to
  run in order.
  
  """
  
  def test_configured_mysql_passwords( self ):
    """ test_configured_mysql_passwords
    
    Makes sure the passwords are properly set on the dirac.cfg and accessed via
    the InstallTools module.
    """
    
    self.logTestName( 'test_configured_mysql_passwords' )
        
    self.assertEquals( lhcb_ci.db.InstallTools.mysqlRootPwd,  self.rootPass )
    self.assertEquals( lhcb_ci.db.InstallTools.mysqlPassword, self.userPass )
    
    res = lhcb_ci.db.InstallTools.getMySQLPasswords()
    self.assertEquals( res[ 'OK' ], True )
    
    self.assertEquals( lhcb_ci.db.InstallTools.mysqlRootPwd,  self.rootPass )
    self.assertEquals( lhcb_ci.db.InstallTools.mysqlPassword, self.userPass )  
  
  
  def test_configure_db( self ):
    """ test_configure_db
    
    Tests that we can configure databases on an "empty CS".
    """
    
    self.logTestName( 'test_configure_db' )
  
    for systemName, systemDBs in self.databases.iteritems():   
      
      systemName = systemName.replace( 'System', '' )
      
      for dbName in systemDBs:
        
        res = lhcb_ci.db.configureDB( systemName, dbName )
        self.assertDIRACEquals( res[ 'OK' ], True, res ) 


  def test_configure_service( self ):
    """ test_configure_service
    
    Test that we can configure services on an "empty CS".
    """

    self.logTestName( 'test_configure_service' )

    _EXCEPTIONS = [ 'ProductionRequest', 'RunDBInterface', 'Future' ]
    # ProductionRequest : Can not find Services/ProductionRequest in template
    # RunDBInterface    : Can not find Services/RunDBInterface in template
    # Future            : Can not find Services/Future in template

    for systemName, services in self.swServices.iteritems():
      
      # Master Configuration is already on place.
      if systemName == 'Configuration':
        self.log.debug( 'Skipping Master Configuration' )
        continue 
      
      for serviceName in services:
      
        if serviceName in _EXCEPTIONS:
          self.log.exception( 'EXCEPTION: skipped %s' % serviceName )
          continue
      
        res = lhcb_ci.service.configureService( systemName, serviceName )
        self.assertDIRACEquals( res[ 'OK' ], True, res )         
        

  def test_configured_service_ports( self ):
    """ test_configured_service_ports
    
    Tests that the services configuration does not overlap, namely ports and
    hosts.
    """
    
    self.logTestName( 'test_configured_service_ports' )
    
    _EXCEPTIONS = [ 'LcgFileCatalogProxy', 'RunDBInterface', 'Future' ]
    
    ports = {}
    
    for system, services in self.swServices.iteritems():
      
      for service in services:
      
        if service in _EXCEPTIONS:
          self.log.exception( 'EXCEPTION: skipped %s' % service )
          continue
      
        serviceName = '%s/%s' % ( system, service )
      
        port = lhcb_ci.service.getServicePort( system, service )
        _msg = '%s:%s already taken by %s' % ( serviceName, port, ports.get( port,'' ) )
        
        # If false, it raises. 
        self.assertTrue( port not in ports, _msg )
        
        ports[ port ] = serviceName

    # Sort port numbers
    sortedPorts = ports.keys()
    sortedPorts.sort()

    # Write ports report
    with open( os.path.join( lhcb_ci.workspace, 'lhcb_ci-services.txt' ), 'w' ) as servFile:
      for port in sortedPorts:
        servFile.write( '%s : %s' % ( port, ports[ port ] ) )  


  ##############################################################################
  #
  # Check services port
  # Check services authentication
  #
  ##############################################################################


  #.............................................................................
  # Nosetests attrs

  
  # test_configured_mysql_passwords
  test_configured_mysql_passwords.configure = 1
  test_configured_mysql_passwords.db        = 1
  
  # test_configure_db
  test_configure_db.configure = 1
  test_configure_db.db        = 1
  
  # test_configure_service
  test_configure_service.configure = 1
  test_configure_service.service   = 1
  
  test_configured_service_ports.configure = 1
  test_configured_service_ports.service   = 1


#...............................................................................
#EOF