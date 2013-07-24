""" lhcb_ci.test.test_configure

  LHCb Continuous Integration system libraries.

  @author: mario.ubeda.garcia@cern.ch
  
"""


import lhcb_ci.basecase 
import lhcb_ci.db

class Configure_Test( lhcb_ci.basecase.DB_TestCase ):
  
  
  def test_configure_db( self ):
    """ test_configure_db
    
    Tests that we can configure databases on an "empty CS". This is kind of extremely
    dirty, because we will carry on with the configured CS for the rest of the tests.
    """
    
    self.logTestName( 'test_configure_db' )
  
    _EXCEPTIONS = [ 'RequestDB' ]
  
    for systemName, systemDBs in self.databases.iteritems():   
      
      systemName = systemName.replace( 'System', '' )
      
      for dbName in systemDBs:
  
        if dbName in _EXCEPTIONS:
          self.log.exception( 'EXCEPTION: skipping %s' % dbName )
          continue
        
        res = lhcb_ci.db.configureDB( systemName, dbName )
        self.assertDIRACEquals( res[ 'OK' ], True, res ) 


  #.............................................................................
  # Nosetests attrs
  
  
  # test_configure_db
  test_configure_db.configure = 1
  test_configure_db.db        = 1  


#...............................................................................
#EOF