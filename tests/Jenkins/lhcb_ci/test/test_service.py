""" lhcb_ci.test.test_service

  LHCb Continuous Integration system libraries.

  @author: mario.ubeda.garcia@cern.ch
  
"""

import lhcb_ci.basecase

class Installation_Test( lhcb_ci.basecase.Service_TestCase ):
  """ Installation_Test
  
  Tests performing operations related with the Services installation.
  """

  def test_install_services( self ):
    
    self.log.debug( self.codeServices )
    self.log.debug( self.installedServices )
    
    for system, services in self.codeServices.iteritems():
      
      for service in services:
        self.log.debug( "%s %s" % ( system, service ) )
                        

#...............................................................................
#EOF
