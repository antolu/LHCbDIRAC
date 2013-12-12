""" lhcb_ci.test.test_client

  LHCb Continuous Integration system libraries.

  @author: mario.ubeda.garcia@cern.ch
  
"""

# Run Federico's code

# Think about SSHElement...

import lhcb_ci.basecase


class TransformationClientTest( lhcb_ci.basecase.Client_TestCase ):
  
  SUT = 'TransformationSystem.Client.TransformationClient'
  
  #@lhcb_ci.basecase.time_test
  def test_addAndRemove( self ):
    
    client = self.sutCls() 
    lhcb_ci.logger.debug( client )
    
    res = client.addTransformation( 'transName', 'description', 'longDescription', 
                                    'MCSimulation', 'Standard', 'Manual', '' )
    self.assertDIRACEquals( res[ 'OK' ], True, res )
    
#    self.assert_( res['OK'] )
#    transID = res['Value']
#
#    # try to add again (this should fail)
#    res = self.transClient.addTransformation( 'transName', 'description', 'longDescription', 'MCSimulation', 'Standard',
#                                              'Manual', '' )
#    self.assertFalse( res['OK'] )
#
#    # clean
#    res = self.transClient.cleanTransformation( transID )
#    self.assert_( res['OK'] )
#    res = self.transClient.getTransformationParameters( transID, 'Status' )
#    self.assert_( res['OK'] )
#    self.assertEqual( res['Value'], 'TransformationCleaned' )
#
#    # really delete
#    res = self.transClient.deleteTransformation( transID )
#    self.assert_( res['OK'] )
#
#    # delete non existing one (fails)
#    res = self.transClient.deleteTransformation( transID )
#    self.assertFalse( res['OK'] )
    
      
  
  test_addAndRemove.smoke  = 1
  test_addAndRemove.client = 1
  
#...............................................................................
#EOF
