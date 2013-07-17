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
      
      if system not in self.installedServices:
        self.log.exception( 'EXCEPTION: System %s not installed' % system )
        continue
      
      for service in services:
        self.log.debug( "%s %s" % ( system, service ) )
      
        if not service in self.installedServices[ system ]:  
          self.log.exception( 'EXCEPTION: Service %s/%s not installed' % ( system, service ) )                       

#...............................................................................
#EOF
