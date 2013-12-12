""" lhcb_ci.test.clients.DataManagementSystem.test_ftsclient

  LHCb Continuous Integration system libraries.

  @author: mario.ubeda.garcia@cern.ch
  
"""

# Run Federico's code

# Think about SSHElement...

import lhcb_ci.basecase


class FTSClientTest( lhcb_ci.basecase.ClientTestCase ):

  
  SUT = 'DataManagementSystem.Client.FTSClient'


  @lhcb_ci.basecase.timeDecorator
  def test_demo( self ):    
      
    pass
  
  test_demo.smoke  = 0
  test_demo.client = 0


#...............................................................................
#EOF
