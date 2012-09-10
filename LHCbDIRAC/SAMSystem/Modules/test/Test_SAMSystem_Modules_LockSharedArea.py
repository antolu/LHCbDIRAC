# $HeadURL: $
''' Test_SAMSystem_Modules_LockSharedArea
'''

import mock
import unittest

import LHCbDIRAC.SAMSystem.Modules.LockSharedArea as moduleTested 

__RCSID__ = '$Id: $'

################################################################################

class LockSharedArea_TestCase( unittest.TestCase ):
  
  def setUp( self ):
    '''
    Setup
    '''
         
    self.moduleTested = moduleTested
    self.testClass    = self.moduleTested.LockSharedArea

  def tearDown( self ):
    '''
    Tear down
    '''
    
    del self.moduleTested
    del self.testClass

################################################################################

class LockSharedArea_Success( LockSharedArea_TestCase ):
  
  def test_instantiate( self ):
    ''' tests that we can instantiate one object of the tested class
    '''  
    
    module = self.testClass()
    self.assertEqual( 'LockSharedArea', module.__class__.__name__ )

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF