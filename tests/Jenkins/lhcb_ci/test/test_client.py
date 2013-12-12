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
    
    transID = res[ 'Value' ]

    # try to add again (this should fail)
    res = client.addTransformation( 'transName', 'description', 'longDescription', 
                                    'MCSimulation', 'Standard', 'Manual', '' )
    self.assertFalse( res['OK'] )

    # clean
    res = client.cleanTransformation( transID )
    self.assertDIRACEquals( res[ 'OK' ], True, res )
    res = client.getTransformationParameters( transID, 'Status' )
    self.assertDIRACEquals( res[ 'OK' ], True, res )
    self.assertEqual( res['Value'], 'TransformationCleaned' )

    # really delete
    res = client.deleteTransformation( transID )
    self.assertDIRACEquals( res[ 'OK' ], True, res )

    # delete non existing one (fails)
    res = client.deleteTransformation( transID )
    self.assertDIRACEquals( res[ 'OK' ], True, res )
    
      
  
  test_addAndRemove.smoke  = 0
  test_addAndRemove.client = 0


#...............................................................................
#EOF
