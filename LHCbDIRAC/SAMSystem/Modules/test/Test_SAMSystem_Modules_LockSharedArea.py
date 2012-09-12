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
       
    mock_ops = mock.Mock()
    mock_ops2 = mock.Mock()
    mock_ops2.getValue.return_value = 10
    mock_ops.return_value = mock_ops2
       
    moduleTested.Operations = mock_ops   
                
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
    
  def test_init( self ):
    
    module = self.testClass()  
    self.assertEquals( 'sam-lock.log', module.logFile )
    self.assertEquals( 'CE-lhcb-lock', module.testName )
    self.assertEquals( 'DIRAC-SAM-Test-Lock', module.lockFile )
    self.assertEquals( False, module.forceLockRemoval )
    self.assertEquals( False, module.safeMode )
  
    

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF