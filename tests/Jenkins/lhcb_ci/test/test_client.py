""" lhcb_ci.test.test_client

  LHCb Continuous Integration system libraries.

  @author: mario.ubeda.garcia@cern.ch
  
"""

# Run Federico's code

# Think about SSHElement...

import lhcb_ci.basecase


class TransformationClientTest( lhcb_ci.basecase.Client_TestCase ):
  
  SUT = 'TransformationSystem.Client.TransformationClient'
  
  @lhcb_ci.basecase.time_test
  def test_demo( self ):
    
    raise NameError( 'DONE' )  
  
  test_demo.smoke  = 1
  test_demo.client = 1
  
#...............................................................................
#EOF
