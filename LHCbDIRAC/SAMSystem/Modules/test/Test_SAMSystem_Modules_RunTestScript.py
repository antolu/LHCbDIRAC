# $HeadURL: $
''' Test_SAMSystem_Modules_RunTestScript
'''

import mock
import unittest

import LHCbDIRAC.SAMSystem.Modules.RunTestScript as moduleTested 

__RCSID__ = '$Id$'

################################################################################

class RunTestScript_TestCase( unittest.TestCase ):
  
  def setUp( self ):
    '''
    Setup
    ''' 
    
    mock_os = mock.Mock()
    mock_os.path.exists.return_value = False    
         
    mock_shell = mock.Mock()
    mock_shell.return_value = { 'OK' : False, 'Message' : 'Bo!' }     
         
    moduleTested.os        = mock_os     
    moduleTested.shellCall = mock_shell
         
    self.moduleTested = moduleTested
    self.testClass    = self.moduleTested.RunTestScript

  def tearDown( self ):
    '''
    Tear down
    '''
    
    del self.moduleTested
    del self.testClass

################################################################################

class RunTestScript_Success( RunTestScript_TestCase ):
  
  def test_instantiate( self ):
    ''' tests that we can instantiate one object of the tested class
    '''  
    
    module = self.testClass()
    self.assertEqual( 'RunTestScript', module.__class__.__name__ )
    
  def test_checkScript( self ):
    ''' tests the method _checkScript
    '''   
    
    module = self.testClass()
    
    res = module._RunTestScript__checkScript()
    self.assertEquals( False, res[ 'OK' ] )
    self.assertEquals( '', res[ 'Message' ] )
    self.assertEquals( 'Script not found', res[ 'Description' ] )
    self.assertEquals( 'notice', res[ 'SamResult' ] )
    
    self.moduleTested.os.path.exists.return_value = True
    res = module._RunTestScript__checkScript()
    self.assertEquals( False, res )
    
    self.moduleTested.os.path.exists.return_value = False
    
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF