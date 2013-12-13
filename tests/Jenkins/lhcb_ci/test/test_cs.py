""" lhcb_ci.test.test_cs

  LHCb Continuous Integration system libraries.

  @author: mario.ubeda.garcia@cern.ch

  The DIRAC CS is a little big monster that needs to be configured. We will do
  it on this test class.
  
"""

import lhcb_ci.basecase

class ConfigureTest( lhcb_ci.basecase.BaseTestCase ):
  
  def test_shifterProxy( self ):
    
    from DIRAC.ConfigurationSystem.Client.CSAPI import CSAPI
    csapi = CSAPI()
    
    csapi.createSection( '/Operations/Shifter/DataManager' )
    csapi.setOption( '/Operations/Shifter/DataManager/User' , 'lhcbciuser' )
    csapi.setOption( '/Operations/Shifter/DataManager/Group', 'user' )
    
    csapi.commit()
    
  
  test_shifterProxy.configure = 1
  test_shifterProxy.cs        = 1
    

#...............................................................................
#EOF
