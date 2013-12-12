""" lhcb_ci.test.test_service

  LHCb Continuous Integration system libraries.

  @author: mario.ubeda.garcia@cern.ch
  
"""


import collections

import lhcb_ci.basecase
import lhcb_ci.commons
import lhcb_ci.component
#import lhcb_ci.db
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

  def test_configure_services( self ):
    """ test_configure_services
    
    Test that we can configure services on an "empty CS".
    """

    self.logTestName()

    for systemName, services in self.swServices.iteritems():
      
      #systemName = systemName.replace( 'System', '' )
      
      # Master Configuration is already on place.
      if systemName == 'ConfigurationSystem':
        self.log.debug( 'Skipping Master Configuration' )
        continue 
      
      for serviceName in services:
      
        if self.isException( serviceName ):
          continue
      
        service = lhcb_ci.component.Component( systemName, 'Service', serviceName )
        res     = service.configure()
        #res = lhcb_ci.service.configureService( systemName, serviceName )
        self.assertDIRACEquals( res[ 'OK' ], True, res )         


  #.............................................................................    
  # Nosetests attrs

  # test_configure_service
  test_configure_services.configure = 1
  test_configure_services.service   = 1


class InstallationTest( lhcb_ci.basecase.Service_TestCase ):
  """ InstallationTest
  
  Tests performing operations related with the Services installation.
  
  """


  def test_service_ports( self ):
    """ test_service_ports
    
    Tests that the services configuration does not overlap, namely ports.
    
    """
    
    self.logTestName()
    
    ports = {}
    
    for system, services in self.swServices.iteritems():
      
      for serviceName in services:
      
        service = lhcb_ci.component.Component( system, 'Service', serviceName )
        
        #fullServiceName = service.composeServiceName()
      
        if self.isException( serviceName ):
          if not 'xxxx' in ports:
            ports[ 'xxxx' ] = [ serviceName ]
          else:
            ports[ 'xxxx' ].append( serviceName )              
          continue  
        
        port = service.getServicePort()
        
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


  def test_service_authorization( self ):
    """ test_service_authorization
    
    Tests that the services default configuration sets a minimum security level.
    This means, any / all by Default is forbidden, all the security properties
    must be valid ones and discourages from the usage of any / all in general.
    
    """

    self.logTestName()
    
    securityProperties = set( lhcb_ci.service.getSecurityProperties() )
    
    authRules = collections.defaultdict( dict )
    
    for system, services in self.swServices.iteritems():
      
      #system = system.replace( 'System', '' )
      
      for serviceName in services:
        
        service = lhcb_ci.component.Component( system, 'Service', serviceName )
        
        fullServiceName = service.composeServiceName()
        
        if self.isException( serviceName ):
          authRules[ fullServiceName ] = { 'xxxx' : 'skipped' }
          continue
        
        
        self.log.debug( '%s authorization rules' % fullServiceName )
        
        res = service.getServiceAuthorization()
        self.assertDIRACEquals( res[ 'OK' ], True, res )
        authorization = res[ 'Value' ]
        
        self.assertTrue( authorization, 'Empty authorization rules not allowed %s' % fullServiceName )
        for method, secProp in authorization.iteritems():
          
          if not isinstance( secProp, str ):
            self.log.debug( 'Found non str authorization rule for %s.%s' % ( fullServiceName, method ) )
            continue
          
          # lower case, just in case
          method  = method.lower()
          secProp = set( secProp.lower().replace( ' ','' ).split( ',' ) )
          
          if method == 'default':
            self.assertFalse( 'all' in secProp, 'Default : All authorization rule is FORBIDDEN %s' % fullServiceName )
            self.assertFalse( 'any' in secProp, 'Default : Any authorization rule is FORBIDDEN %s' % fullServiceName )
          
          if not ( secProp & set( [ 'all', 'any', 'authenticated' ] ) ):
            self.assertTrue( secProp <= securityProperties, '%s is an invalid SecProp %s' % ( secProp, fullServiceName ) )
          elif secProp & set( [ 'all', 'any' ] ):
            self.log.warning( '%s.%s has all/any no SecurityProperty' % ( fullServiceName, method ) )

          authRules[ fullServiceName ][ method ] = ', '.join([ sp for sp in secProp ])
            
           
    # Write authorization report
    with open( self.reportPath(), 'w' ) as servFile:
      for servName, authRule in authRules.iteritems():
        servFile.write( '%s\n' % servName )
        for method, secProp in authRule.iteritems():
          servFile.write( '  %s : %s\n' % ( method.ljust( 40 ), secProp ) )


  def test_services_common_import( self ):
    """ test_services_common_import
    
    Tests that we can import the DIRAC Service objects pointing to an specific Service.
    It iterates over all services discovered on the code *Handler.py objects and instantiates
    a DIRAC.Core.DISET.private.Service object to interact with them. 
    
    """

    self.logTestName()
   
    for diracSystem, services in self.swServices.iteritems():
      
      diracSystem = diracSystem.replace( 'System', '' )
      
      for service in services:

        serviceName = '%s/%s' % ( diracSystem, service )
          
        if self.isException( service ):
          continue

        # Keep track of threads to wash them
        currentThreads, activeThreads = lhcb_ci.commons.trackThreads()
        
        # Tries to get a Service DIRAC object
        self.log.debug( 'Service %s' % serviceName )
        service = lhcb_ci.service.getService( serviceName )
        
        # Cleanup
        del service       
        # Clean leftovers         
        threadsAfterPurge = lhcb_ci.commons.killThreads( currentThreads )
        # We make sure that there are no leftovers on the threading
        self.assertEquals( activeThreads, threadsAfterPurge )        
            
            
  # FIXME: this test method has a thread leak   
  def test_services_voimport( self ):
    """ test_services_voimport
    
    Tries to import the Handler modules and create a class Object. Iterating over all
    services found in the code, tries to import their modules and instantiate
    one class object.
    
    """
    
    self.logTestName()
   
    for diracSystem, services in self.swServices.iteritems():
      
      diracSystem = diracSystem.replace( 'System', '' )
      
      for service in services:

        serviceName   = '%s/%s' % ( diracSystem, service )
        serviceHander = '%sHandler' % service  
         
        if self.isException( serviceHander ):
          continue
          
        # Import DIRAC module and get object
        servicePath = 'DIRAC.%sSystem.Service.%s' % ( diracSystem, serviceHander )
        self.log.debug( 'VO Importing %s' % servicePath )
        
        # Keep track of threads to wash them
        currentThreads, activeThreads = lhcb_ci.commons.trackThreads()
        
        serviceMod = lhcb_ci.extensions.import_( servicePath )
        self.assertEquals( hasattr( serviceMod, serviceHander ), True )
        
        serviceClass = getattr( serviceMod, serviceHander )
        
        lhcb_ci.service.initializeServiceClass( serviceClass, serviceName )
        
        serviceInstance = serviceClass( {}, None )
        del serviceInstance    

        # Clean leftovers         
        threadsAfterPurge = lhcb_ci.commons.killThreads( currentThreads )
        # We make sure that there are no leftovers on the threading
        self.assertEquals( activeThreads, threadsAfterPurge )


  def test_services_install_drop( self ):
    """ test_services_install_drop
    
    Tests that we can install / drop directly services using the DIRAC tools. It
    does not check whether the services run with errors or not. It iterates over
    all services found in self.swServices, which are all python files *Hanlder.py
    
    It avoids the Configuration Server as it is running.
    
    """    
    
    self.logTestName()
            
    for systemName, services in self.swServices.iteritems():
      
      #system = system.replace( 'System', '' )
      
      if systemName == 'ConfigurationSystem':
        self.log.debug( 'Skipping Master Configuration' )
        continue 
      
      for serviceName in services:
        self.log.debug( "%s %s" % ( systemName, serviceName ) )

        if self.isException( serviceName ):
          continue

        # FIXME: hack to speedup tests
        if not 'Transformation' in serviceName:
          continue

        currentThreads, activeThreads = lhcb_ci.commons.trackThreads()        

        service = lhcb_ci.component.Component( systemName, 'Service', serviceName )
        res     = service.install()
       
        #res = lhcb_ci.service.setupService( system, service )      
        self.assertDIRACEquals( res[ 'OK' ], True, res )
        self.assertEquals( res[ 'Value' ][ 'RunitStatus' ], 'Run' )
        
        res     = service.uninstall()
        #res = lhcb_ci.service.uninstallService( systemName.replace( 'System', '' ), service )      
        self.assertDIRACEquals( res[ 'OK' ], True, res )

        # Clean leftovers         
        threadsAfterPurge = lhcb_ci.commons.killThreads( currentThreads )
        # We make sure that there are no leftovers on the threading
        self.assertEquals( activeThreads, threadsAfterPurge )  
    

  #.............................................................................    
  # Nosetests attrs

  # test_configured_service_ports
  test_service_ports.configure = 1
  test_service_ports.service   = 1
  
  # test_configured_service_authorization
  test_service_authorization.configure = 1
  test_service_authorization.service   = 1

  test_services_common_import.install = 0
  test_services_common_import.service = 0

  #FIXME: thread leak
  test_services_voimport.install = 0
  test_services_voimport.service = 0

  # test_services_install_drop
  test_services_install_drop.install = 1
  test_services_install_drop.service = 1
  
    

class SmokeTest( lhcb_ci.basecase.Service_TestCase ):
  """ SmokeTest
  
  Tests performing basic common operations on the services.
  
  """
  

  def test_run_services( self ):
    """ test_run_services
    
    This test iterates over all the services found in the code ( self.swServices )
    and runs then through the ServiceReactor. This is kind of ugly, due to an
    unknown number of daemonized and non daemonized threads started by each
    service ( it happens that every developer likes different solutions ). In this
    respect, all threads created by the ServiceReactor and childs, are stopped
    to avoid problems. Once the service is running on a parallel thread, it is 
    pinged to check it is working.
    
    """
    
    self.logTestName()
    
    for system, services in self.swServices.iteritems():
      
      system = system.replace( 'System', '' )
      
      if system == 'Configuration':
        self.log.debug( 'Skipping Master Configuration' )
        continue 

      for service in services:

        if self.isException( service ):
          continue

        self.log.debug( "%s %s" % ( system, service ) )
        
        # Keep track of threads to wash them
        currentThreads, activeThreads = lhcb_ci.commons.trackThreads()
        
        # Tries to find on the same system a database to be installed. If it fails,
        # installs all databases on the system.      
        dbNames = self.databases.get( '%sSystem' % system, [] )
        if '%sDB' % service in dbNames:
          self.log.debug( 'Found database for %s' % service )
          dbNames = [ '%sDB' % service ]        
        
        for dbName in dbNames:
          db  = lhcb_ci.component.Component( '%sSystem' % system, 'DB', dbName )
          res = db.install()
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
          res = db.uninstall()
          self.assertDIRACEquals( res[ 'OK' ], True, res )
        
        # Clean leftovers         
        threadsAfterPurge = lhcb_ci.commons.killThreads( currentThreads )
        # We make sure that there are no leftovers on the threading
        self.assertEquals( activeThreads, threadsAfterPurge )


  #.............................................................................    
  # Nosetests attrs


  # test_run_services
  test_run_services.smoke   = 0
  test_run_services.service = 0
  

#...............................................................................
#EOF