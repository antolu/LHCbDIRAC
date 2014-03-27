""" Test Interfaces API DiracProduction
"""

import mock, unittest

import os

import LHCbDIRAC.Interfaces.API.DiracProduction as moduleTested 
from LHCbDIRAC.Interfaces.API.LHCbJob import LHCbJob

__RCSID__ = '$Id: $'

################################################################################

class APITestCase( unittest.TestCase ):
  
  def setUp( self ):
    """
    Setup
    """
    
    mock_DiracLHCb      = mock.Mock( return_value = { 'OK' : True, 'Value' : 1 } )
    self.mock_DiracLHCb = mock_DiracLHCb 
    
    moduleTested.DiracLHCb = lambda :self.mock_DiracLHCb
        
    self.moduleTested = moduleTested
    self.testClass    = self.moduleTested.DiracProduction
    
    self.lj = LHCbJob()
    
  def tearDown( self ):
    """
    TearDown
    """
    del self.testClass
    del self.moduleTested
      
################################################################################
# Tests

class DiracProductionSuccess( APITestCase ):
  
  def test_instantiate( self ):
    """ tests that we can instantiate one object of the tested class
    """
    
    prod = self.testClass( 1 )
    self.assertEqual( 'DiracProduction', prod.__class__.__name__ )

################################################################################

class LHCbJobSuccess( APITestCase ):

  def test_LJ( self ):
    """ 
    """
    open( 'optionsFiles', 'a' ).close()
    res = self.lj.setApplication( 'appName', 'v1r0', 'optionsFiles', systemConfig = 'x86_64-slc6-gcc-44-opt' )
    self.assert_( res['OK'] )
    res = self.lj.setApplication( 'appName', 'v1r0', 'optionsFiles', systemConfig = 'x86_64-slc5-gcc-41-opt' )
    self.assert_( res['OK'] )
    res = self.lj.setApplication( 'appName', 'v1r0', 'optionsFiles', systemConfig = 'x86_64-slc5-gcc-43-opt' )
    self.assert_( res['OK'] )
    os.remove( 'optionsFiles' )

################################################################################

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( APITestCase )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( DiracProductionSuccess ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF    
