""" lhcb_ci.test.test_service

  LHCb Continuous Integration system libraries.

  @author: mario.ubeda.garcia@cern.ch
  
"""


import collections
import threading

import lhcb_ci.basecase
import lhcb_ci.db
import lhcb_ci.service


class ConfigureTest( lhcb_ci.basecase.Service_TestCase ):
  """ ConfigureTest
  
  This class contains dirty & sticky tests. The configuration steps have been
  transformed into simple unittests, which are run here. Dirty & sticky because
  the tests will alter the CS structure, adding the necessary configuration 
  parameters to be able to run the rest of the tests. 
  
  Disclaimer: do not change the name of the tests, as some of them need to
  run in order.
  
  """

  def test_configure_service( self ):
    """ test_configure_service
    
    Test that we can configure services on an "empty CS".
    """

    self.logTestName()

    for systemName, services in self.swServices.iteritems():
      
      # Master Configuration is already on place.
      if systemName == 'Configuration':
        self.log.debug( 'Skipping Master Configuration' )
        continue 
      
      for serviceName in services:
      
        if self.isException( serviceName ):
          continue
      
        res = lhcb_ci.service.configureService( systemName, serviceName )
        self.assertDIRACEquals( res[ 'OK' ], True, res )         


  #.............................................................................    
  # Nosetests attrs

  # test_configure_service
  test_configure_service.configure = 1
  test_configure_service.service   = 1


class InstallationTest( lhcb_ci.basecase.Service_TestCase ):
  """ InstallationTest
  
  Tests performing operations related with the Services installation.
  
  """


  def test_configured_service_ports( self ):
    """ test_configured_service_ports
    
    Tests that the services configuration does not overlap, namely ports.
    
    """
    
    self.logTestName()
    
    ports = {}
    
    for system, services in self.swServices.iteritems():
      
      for service in services:
      
        serviceName = '%s/%s' % ( system, service )
      
        if self.isException( service ):
          try:
            ports[ 'xxxx' ].append( serviceName )
          except KeyError:
            ports[ 'xxxx' ] = [ serviceName ]  
          continue  
      
        port = lhcb_ci.service.getServicePort( system, service )
        _msg = '%s:%s already taken by %s' % ( serviceName, port, ports.get( port,'' ) )
        
        # If false, it raises. 
        self.assertTrue( port not in ports, _msg )
        
        ports[ port ] = serviceName

    # Sort port numbers
    sortedPorts = ports.keys()
    sortedPorts.sort()

    # Write ports report
    with open( self.reportPath(), 'w' ) as servFile:
      for port in sortedPorts:
        servFile.write( '%s : %s\n' % ( port, ports[ port ] ) )  


  def test_configured_service_authorization( self ):
    """ test_configured_service_authorization
    
    Tests that the services default configuration sets a minimum security level.
    This means, any / all by Default is forbidden, all the security properties
    must be valid ones and discourages from the usage of any / all in general.
    
    """

    self.logTestName()
    
    securityProperties = set( lhcb_ci.service.getSecurityProperties() )
    
    authRules = collections.defaultdict( dict )
    
    for system, services in self.swServices.iteritems():
      
      for service in services:
        
        serviceName = '%s/%s' % ( system, service )
        
        if self.isException( service ):
          authRules[ serviceName ] = { 'xxxx' : 'skipped' }
          continue
        
        
        self.log.debug( '%s authorization rules' % serviceName )
        
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

          authRules[ serviceName ][ method ] = ', '.join([ sp for sp in secProp ])
            
           
    # Write authorization report
    with open( self.reportPath(), 'w' ) as servFile:
      for servName, authRule in authRules.iteritems():
        servFile.write( '%s\n' % servName )
        for method, secProp in authRule.iteritems():
          servFile.write( '  %s : %s\n' % ( method.ljust( 40 ), secProp ) )
          

  def test_services_install_drop( self ):
    """ test_services_install_drop
    
    Tests that we can install / drop directly services using the DIRAC tools. It
    does not check whether the services run with errors or not. It iterates over
    all services found in self.swServices, which are all python files *Hanlder.py
    
    It avoids the Configuration Server as it is running.
    
    """    
    
    self.logTestName()
            
    for system, services in self.swServices.iteritems():
      
      if system == 'Configuration':
        self.log.debug( 'Skipping Master Configuration' )
        continue 
      
      for service in services:
        self.log.debug( "%s %s" % ( system, service ) )
       
        res = lhcb_ci.service.setupService( system, service )      
        self.assertDIRACEquals( res[ 'OK' ], True, res )
        self.assertEquals( res[ 'Value' ][ 'RunitStatus' ], 'Run' )
        
        res = lhcb_ci.service.uninstallService( system, service )      
        self.assertDIRACEquals( res[ 'OK' ], True, res )
        

  def test_run_services( self ):
    """ test_run_services
    
    This test iterates over all the services found in the code ( self.swServices )
    and runs then through the ServiceReactor. This is kind of ugly, due to an
    unknown number of daemonized and non daemonized threads started by each
    service ( it happens that every developer likes different solutions ). In this
    respect, all threads created by the ServiceReactor and childs, are stopped
    to avoid problems. Once the service is running on a paralell thread, it is 
    pinged to check it is working.
    
    """
    
    self.logTestName()
    
    for system, services in self.swServices.iteritems():
      
      if system == 'Configuration':
        self.log.debug( 'Skipping Master Configuration' )
        continue 

      for service in services:

        if self.isException( service ):
          continue

        self.log.debug( "%s %s" % ( system, service ) )
        
        # This also includes the _limbo threads..
        threadsToBeAvoided = threading.enumerate() 
        activeThreads      = threading.active_count()      
        
        # Tries to find on the same system a database to be installed. If it fails,
        # installs all databases on the system.      
        dbNames = self.databases.get( '%sSystem' % system, [] )
        if '%sDB' % service in dbNames:
          self.log.debug( 'Found database for %s' % service )
          dbNames = [ '%sDB' % service ]        
        
        for dbName in dbNames:          
          db = lhcb_ci.db.installDB( dbName )
          self.assertDIRACEquals( db[ 'OK' ], True, db )
          
        res = lhcb_ci.service.initializeServiceReactor( system, service )
        self.assertDIRACEquals( res[ 'OK' ], True, res )
        # Extract the initialized ServiceReactor
        sReactor = res[ 'Value' ]
        
        res = lhcb_ci.service.serveAndPing( sReactor )
        self.log.debug( str( res ) )
        self.assertDIRACEquals( res[ 'OK' ], True, res )
        
        self.assertEquals( res[ 'Value' ][ 'name' ], '%s/%s' % ( system, service ) )
        # If everything is OK, the ping should be done within the first 10 seconds
        self.assertEquals( res[ 'Value' ][ 'service uptime' ] < 10, True )
                
        del sReactor

        for dbName in dbNames:          
          db = lhcb_ci.db.dropDB( dbName )
          self.assertDIRACEquals( db[ 'OK' ], True, db )
        
        # Clean leftovers         
        lhcb_ci.service.killThreads( threadsToBeAvoided )
        
        currentActiveThreads = threading.active_count()
        # We make sure that there are no leftovers on the threading
        self.assertEquals( activeThreads, currentActiveThreads )
    

  #.............................................................................    
  # Nosetests attrs


  # test_configured_service_ports
  test_configured_service_ports.configure = 1
  test_configured_service_ports.service   = 1
  
  # test_configured_service_authorization
  test_configured_service_authorization.configure = 1
  test_configured_service_authorization.service   = 1

  # test_services_install_drop
  test_services_install_drop.install = 1
  test_services_install_drop.service = 1
  
  # test_run_services
  test_run_services.install = 1
  test_run_services.service = 1
  

#...............................................................................
#EOF