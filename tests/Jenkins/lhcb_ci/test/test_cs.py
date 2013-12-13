""" lhcb_ci.test.test_cs

  LHCb Continuous Integration system libraries.

  @author: mario.ubeda.garcia@cern.ch

  The DIRAC CS is a little big monster that needs to be configured. We will do
  it on this test class.
  
"""

import lhcb_ci.basecase
import lhcb_ci.commons

from DIRAC.ConfigurationSystem.Client.CSAPI import CSAPI

class ConfigureTest( lhcb_ci.basecase.BaseTestCase ):
  
  def test_shifterProxy( self ):   
    
    self.currentThreads_, self.activeThreads_ = lhcb_ci.commons.trackThreads()
    
    csapi = CSAPI()
    
    res = csapi.createSection( '/Operations/Defaults/Shifter/DataManager' )
    self.assertDIRACEquals( res[ 'OK' ], True, res )
    res = csapi.setOption( '/Operations/Defaults/Shifter/DataManager/User' , 'lhcbciuser' )
    self.assertDIRACEquals( res[ 'OK' ], True, res )
    res = csapi.setOption( '/Operations/Defaults/Shifter/DataManager/Group', 'user' )
    self.assertDIRACEquals( res[ 'OK' ], True, res )
    
    res = csapi.commit()
    self.assertDIRACEquals( res[ 'OK' ], True, res )
    
    del csapi
    lhcb_ci.commons.killThreads( self.currentThreads_ )
    
  def test_resources( self ):
  
    self.currentThreads_, self.activeThreads_ = lhcb_ci.commons.trackThreads()
  
    csapi = CSAPI()
    
    res = csapi.createSection( '/Resources/Sites' )
    self.assertDIRACEquals( res[ 'OK' ], True, res )
    
    res = csapi.commit()
    self.assertDIRACEquals( res[ 'OK' ], True, res )

    del csapi
    lhcb_ci.commons.killThreads( self.currentThreads_ )
  
  
  test_shifterProxy.configure = 1
  test_shifterProxy.cs        = 1
  
  test_resources.configure = 1
  test_resources.cs        = 1
    

#...............................................................................
#EOF
