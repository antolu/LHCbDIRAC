""" lhcb_ci.test.test_service

  LHCb Continuous Integration system libraries.

  @author: mario.ubeda.garcia@cern.ch
  
"""

import lhcb_ci.basecase
import lhcb_ci.service

class Installation_Test( lhcb_ci.basecase.Service_TestCase ):
  """ Installation_Test
  
  Tests performing operations related with the Services installation.
  """

  def test_install_services_drop( self ):
    
    self.log.debug( self.swServices )
        
    for system, services in self.swServices.iteritems():
      
      if system == 'Configuration':
        self.log.debug( 'Skipping Master Configuration' )
        continue 
      
      for service in services:
        self.log.debug( "%s %s" % ( system, service ) )

        res = lhcb_ci.service.setupService( system, service )      
        self.assertDIRACEquals( res[ 'OK' ], True, res )
        
        res = lhcb_ci.service.uninstallService( system, service )      
        self.assertDIRACEquals( res[ 'OK' ], True, res )
      

#...............................................................................
#EOF
