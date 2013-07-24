""" lhcb_ci.test.test_configure

  LHCb Continuous Integration system libraries.

  @author: mario.ubeda.garcia@cern.ch
  
"""


import lhcb_ci.basecase 
import lhcb_ci.db


class Configure_Test( lhcb_ci.basecase.Service_TestCase ):
  """ Configure_Test
  
  This class contains dirty & sticky tests. The configuration steps have been
  transformed into simple unittests, which are run here. Dirty & sticky because
  the tests will alter the CS structure, adding the necessary configuration 
  parameters to be able to run the rest of the tests.
  
  """
  
  
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

    _EXCEPTIONS = [ 'ProductionRequest', 'RunDBInterface' ]
    # ProductionRequest : Can not find Services/ProductionRequest in template
    # RunDBInterface    : Can not find Services/RunDBInterface in template

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
        

  #.............................................................................
  # Nosetests attrs
  
  
  # test_configure_db
  test_configure_db.configure = 1
  test_configure_db.db        = 1
  
  # test_configure_service
  test_configure_service.configure = 1
  test_configure_service.service   = 1


#...............................................................................
#EOF