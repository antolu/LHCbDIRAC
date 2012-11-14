# $HeadURL: $
''' Test_Interfaces_API_DiracProduction

'''

import mock
import unittest

import LHCbDIRAC.Interfaces.API.DiracProduction as moduleTested 

__RCSID__ = '$Id: $'

################################################################################

class DiracProduction_TestCase( unittest.TestCase ):
  
  def setUp( self ):
    '''
    Setup
    '''
    
    mock_DiracLHCb      = mock.Mock( return_value = { 'OK' : True, 'Value' : 1 } )
    self.mock_DiracLHCb = mock_DiracLHCb 
    
    moduleTested.DiracLHCb = lambda :self.mock_DiracLHCb
        
    self.moduleTested = moduleTested
    self.testClass    = self.moduleTested.DiracProduction
    
  def tearDown( self ):
    '''
    TearDown
    '''
    del self.testClass
    del self.moduleTested
      
################################################################################
# Tests

class DiracProduction_Success( DiracProduction_TestCase ):
  
  def test_instantiate( self ):
    ''' tests that we can instantiate one object of the tested class
    '''  
    
    prod = self.testClass( 1 )
    self.assertEqual( 'DiracProduction', prod.__class__.__name__ )

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF    