""" lhcb_ci.test.test_service

  LHCb Continuous Integration system libraries.

  @author: mario.ubeda.garcia@cern.ch
  
"""

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
    does not check whether the services run with errors or not.
    """    
    
    self.log.debug( 'test_services_install_drop' )
            
    for system, services in self.swServices.iteritems():
      
      if system == 'Configuration':
        self.log.debug( 'Skipping Master Configuration' )
        continue 
      
      for service in services:
        self.log.debug( "%s %s" % ( system, service ) )

#        dbName = '%sDB' % service
#        db = lhcb_ci.db.installDB( dbName )
#        if not db[ 'OK' ]:
#          self.log.debug( 'No DB for service %s' % service )
        
        res = lhcb_ci.service.setupService( system, service )      
        self.assertDIRACEquals( res[ 'OK' ], True, res )
        self.log.debug( res )
        
        res = lhcb_ci.service.uninstallService( system, service )      
        self.assertDIRACEquals( res[ 'OK' ], True, res )
        self.log.debug( res )
      
#        if db[ 'OK' ]:
#          self.log.debug( 'Dropping DB %s for service' % dbName )
#          res = lhcb_ci.db.dropDB( dbName )
#          self.assertDIRACEquals( res[ 'OK' ], True, res )

  def run_services( self ):
    
    self.log.debug( 'test_run_services' )
    
    for system, services in self.swServices.iteritems():
      
      if system == 'Configuration':
        self.log.debug( 'Skipping Master Configuration' )
        continue 

      for service in services:
        self.log.debug( "%s %s" % ( system, service ) )

      


    

#...............................................................................
#EOF
