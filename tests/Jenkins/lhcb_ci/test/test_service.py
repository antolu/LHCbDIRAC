""" lhcb_ci.test.test_service

  LHCb Continuous Integration system libraries.

  @author: mario.ubeda.garcia@cern.ch
  
"""

import threading

import lhcb_ci.basecase
import lhcb_ci.db
import lhcb_ci.service

class Installation_Test( lhcb_ci.basecase.Service_TestCase ):
  """ Installation_Test
  
  Tests performing operations related with the Services installation.
  
  """

  def test_services_install_drop( self ):
    """ test_services_install_drop
    
    Tests that we can install / drop directly services using the DIRAC tools. It
    does not check whether the services run with errors or not. It iterates over
    all services found in self.swServices, which are all python files *Hanlder.py
    
    It avoids the Configuration Server as it is running.
    
    """    
    
    self.logTestName( 'test_services_install_drop' )
            
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
    
    self.logTestName( 'test_run_services' )
    
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


  # test_services_install_drop
  test_services_install_drop.install = 1
  test_services_install_drop.service = 1
  
  # test_run_services
  test_run_services.install = 1
  test_run_services.service = 1
  

#...............................................................................
#EOF