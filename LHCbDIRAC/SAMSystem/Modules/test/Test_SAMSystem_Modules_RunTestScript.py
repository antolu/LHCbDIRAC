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
         
#    mock_shell = mock.Mock()
#    mock_shell.return_value = { 'OK' : False, 'Message' : 'Bo!' }     
         
    moduleTested.os        = mock_os     
#    moduleTested.shellCall = mock_shell
         
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
    
    module.runCommand = mock.Mock()
    module.runCommand.return_value = { 'OK' : False, 'Message' : 'Bo!' }
    
    self.moduleTested.os.path.exists.return_value = True
    
    res = module._RunTestScript__checkScript()
    self.assertEquals( False, res[ 'OK' ] )
    self.assertEquals( '', res[ 'Message' ] )
    self.assertEquals( 'Script not found', res[ 'Description' ] )
    self.assertEquals( 'info', res[ 'SamResult' ] )
    
    module.runCommand.return_value = { 'OK' : True, 'Value' : 1 }
    res = module._RunTestScript__checkScript()
    self.assertEquals( True, res[ 'OK' ] )
    self.assertEquals( '', res[ 'Value' ] )
    
    self.moduleTested.os.path.exists.return_value = False
    
  def test_execute( self ):
    ''' tests the method _execute
    '''    
    
    module = self.testClass()
    
    res = module._execute()
    self.assertEquals( True, res[ 'OK' ] )
    self.assertEquals( 'Script not found', res[ 'Value' ] )

    module.runCommand = mock.Mock()
    module.runCommand.return_value = { 'OK' : True, 'Value' : 1 }
    self.moduleTested.os.path.exists.return_value = True    
    
    res = module._execute()
    self.assertEquals( True, res[ 'OK' ] )
    self.assertEquals( 'CE-lhcb-test-script Test Successful', res[ 'Value' ] )
    
  def test_resolveInputVariables( self ):
    ''' tests the method resolveInputVariables
    '''  

    module = self.testClass()
    
    res = module.resolveInputVariables()
    self.assertEquals( True, res[ 'OK' ] )
    self.assertEquals( '', module.scriptName )
    self.assertEquals( True, module.enable )
    
    module.step_commons[ 'scriptName' ] = 'scriptName'
    res = module.resolveInputVariables()
    self.assertEquals( True, res[ 'OK' ] )
    self.assertEquals( 'scriptName', module.scriptName )    
    self.assertEquals( True, module.enable )    
        
    module.scriptName                   = ''    
    module.step_commons[ 'scriptName' ] = 123
    res = module.resolveInputVariables()
    self.assertEquals( True, res[ 'OK' ] )
    self.assertEquals( 123, module.scriptName )
    self.assertEquals( False, module.enable )
    
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF