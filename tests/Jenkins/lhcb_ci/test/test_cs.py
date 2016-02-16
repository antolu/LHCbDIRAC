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
  """ ConfigureTest
  
  This class contains dirty & sticky tests. The configuration steps have been
  transformed into simple unittests, which are run here. Dirty & sticky because
  the tests will alter the CS structure, adding the necessary configuration 
  parameters to be able to run the rest of the tests. 
  
  """

  
  def test_shifterProxy( self ):
    """ test_shifterProxy
    
    Let's try to set a new ShifterProxy in the CS.
    
    """   
    
    #CSAPI object leaves few threads in background... need to make sure all are
    #gone once the method is done.
    currentThreads, _activeThreads = lhcb_ci.commons.trackThreads()
    
    csapi = CSAPI()
    
    res = csapi.createSection( '/Operations/Defaults/Shifter/DataManager' )
    self.assertDIRACEquals( res[ 'OK' ], True, res )
    res = csapi.setOption( '/Operations/Defaults/Shifter/DataManager/User' , 'lhcbciuser' )
    self.assertDIRACEquals( res[ 'OK' ], True, res )
    res = csapi.setOption( '/Operations/Defaults/Shifter/DataManager/Group', 'user' )
    self.assertDIRACEquals( res[ 'OK' ], True, res )
    
    res = csapi.commit()
    self.assertDIRACEquals( res[ 'OK' ], True, res )
    
    #CSAPI object leaves few threads in background... need to make sure all are
    #gone once the method is done.
    del csapi
    lhcb_ci.commons.killThreads( currentThreads )
    
  def test_resources( self ):
    """ test_resources
    
    Creates /Resources/Sites section, completely empty ! Lot of work to be done
    here in v7r0 !.
    
    """  
  
    #CSAPI object leaves few threads in background... need to make sure all are
    #gone once the method is done.
    currentThreads, _activeThreads = lhcb_ci.commons.trackThreads()
    
    csapi = CSAPI()
    
    res = csapi.createSection( '/Resources/Sites' )
    self.assertDIRACEquals( res[ 'OK' ], True, res )
    
    res = csapi.commit()
    self.assertDIRACEquals( res[ 'OK' ], True, res )

    #CSAPI object leaves few threads in background... need to make sure all are
    #gone once the method is done.
    del csapi
    lhcb_ci.commons.killThreads( currentThreads )
  
  
  test_shifterProxy.configure = 1
  test_shifterProxy.cs        = 1
  
  test_resources.configure = 1
  test_resources.cs        = 1
    

#...............................................................................
#EOF
