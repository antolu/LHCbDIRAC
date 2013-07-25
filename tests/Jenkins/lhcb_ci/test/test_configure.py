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
    
#    _EXCEPTIONS = [ 'LcgFileCatalogProxy', 'RunDBInterface', 'Future', 'MigrationMonitoring', 'ProductionRequest' ]
    
    ports = {}
    
    for system, services in self.swServices.iteritems():
      
      for service in services:
      
        if self.isException( service ):
#        if service in _EXCEPTIONS:
#          self.log.exception( 'EXCEPTION: skipped %s' % service )
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
    with open( os.path.join( lhcb_ci.workspace, 'lhcb_ci-service-ports.txt' ), 'w' ) as servFile:
      for port in sortedPorts:
        servFile.write( '%s : %s\n' % ( port, ports[ port ] ) )  


  def test_configured_service_authorization( self ):
    """ test_configured_service_authorization
    
    Tests that the services default configuration sets a minimum security level.   
    """

    self.logTestName( 'test_configured_service_authorization' )
    
#    _EXCEPTIONS = [ 'BookkeepingManager', 'Publisher', 'ProductionRequest', 'LcgFileCatalogProxy',
#                    'DataUsage', 'StorageUsage', 'DataIntegrity', 'RunDBInterface', 'RAWIntegrity',
#                    'Gateway', 'JobStateSync', 'Future', 'OptimizationMind' ]
    
    securityProperties = set( lhcb_ci.service.getSecurityProperties() )
    
    authRules = {}
    
    for system, services in self.swServices.iteritems():
      
      for service in services:
        
        #if service in _EXCEPTIONS:
        if self.isException( service ):
          #self.log.exception( 'EXCEPTION: skipped %s' % service )
          continue
        
        serviceName = '%s/%s' % ( system, service )
        
        self.log.debug( '%s authorization rules' % serviceName )
        
        authRules[ serviceName ] = {}
        
        res = lhcb_ci.service.getServiceAuthorization( system, service )
        self.assertDIRACEquals( res[ 'OK' ], True, res )
        
        authorization = res[ 'Value' ]
        
        self.assertTrue( authorization, 'Empty authorization rules not allowed %s' % serviceName )
        for method, secProp in authorization.iteritems():
          
          if not isinstance( secProp, str ):
            self.log.debug( 'Found non str authorization rule for %s.%s' % ( serviceName, method ) )
            continue
          
          # lower case, just in case
          method  = method.lower()
          secProp = set( secProp.lower().replace( ' ','' ).split( ',' ) )
          
          if method == 'default':
            self.assertFalse( 'all' in secProp, 'Default : All authorization rule is FORBIDDEN %s' % serviceName )
            self.assertFalse( 'any' in secProp, 'Default : Any authorization rule is FORBIDDEN %s' % serviceName )
          
          if not ( secProp & set( [ 'all', 'any', 'authenticated' ] ) ):
            self.assertTrue( secProp <= securityProperties, '%s is an invalid SecProp %s' % ( secProp, serviceName ) )
          elif secProp & set( [ 'all', 'any' ] ):
            self.log.warning( '%s.%s has all/any no SecurityProperty' % ( serviceName, method ) )

          authRules[ serviceName ][ method ] = secProp
            
           
    # Write authorization report
    with open( os.path.join( lhcb_ci.workspace, 'lhcb_ci-service-authorization.txt' ), 'w' ) as servFile:
      for servName, authRule in authRules.iteritems():
        servFile.write( '%s\n' % servName )
        for method, secProp in authRule.iteritems():
          servFile.write( '  %s : %s\n' % ( method.ljust( 40 ), secProp ) )


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
  
  test_configured_service_authorization.configure = 1
  test_configured_service_authorization.service   = 1


#...............................................................................
#EOF