""" lhcb_ci.test.test_cs

  LHCb Continuous Integration system libraries.

  @author: mario.ubeda.garcia@cern.ch

  The DIRAC CS is a little big monster that needs to be configured. We will do
  it on this test class.
  
"""

import lhcb_ci.basecase

from DIRAC.ConfigurationSystem.Client.CSAPI import CSAPI

class ConfigureTest( lhcb_ci.basecase.BaseTestCase ):
  
  def test_shifterProxy( self ):   
    
    csapi = CSAPI()
    
    csapi.createSection( '/Operations/Defaults/Shifter/DataManager' )
    csapi.setOption( '/Operations/Defaults/Shifter/DataManager/User' , 'lhcbciuser' )
    csapi.setOption( '/Operations/Defaults/Shifter/DataManager/Group', 'user' )
    
    csapi.commit()
    
    
  def test_resources( self ):
  
    csapi = CSAPI()
    
    csapi.createSection( '/Resources/Sites' )
    #csapi.setOption( '/Operations/Shifter/DataManager/User' , 'lhcbciuser' )
    #csapi.setOption( '/Operations/Shifter/DataManager/Group', 'user' )
    
    csapi.commit()
  
  
  test_shifterProxy.configure = 1
  test_shifterProxy.cs        = 1
  
  test_resources.configure = 1
  test_resources.cs        = 1
    

#...............................................................................
#EOF
