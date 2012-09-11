# $HeadURL: $
''' Test_SAMSystem_Modules_ModuleBaseSAM
'''

import mock
import unittest

import LHCbDIRAC.SAMSystem.Modules.ModuleBaseSAM as moduleTested 

__RCSID__ = '$Id: $'

################################################################################

class ModuleBaseSAM_TestCase( unittest.TestCase ):
  
  def setUp( self ):
    '''
    Setup
    '''
         
    mock_os = mock.Mock()
    mock_os.environ = { 'JOBID' : '123' }    
    
    mock_shell = mock.Mock()
    mock_shell.return_value = { 'OK' : True, 'Value' : [ 0, 'stdout', 'stderr' ] }
    
    moduleTested.os        = mock_os
    moduleTested.shellCall = mock_shell
         
    self.moduleTested = moduleTested
    self.testClass    = self.moduleTested.ModuleBaseSAM

  def tearDown( self ):
    '''
    Tear down
    '''
    
    del self.moduleTested
    del self.testClass

################################################################################

class ModuleBaseSAM_Success( ModuleBaseSAM_TestCase ):
  
  def test_instantiate( self ):
    ''' tests that we can instantiate one object of the tested class
    '''  
    
    module = self.testClass()
    self.assertEqual( 'ModuleBaseSAM', module.__class__.__name__ )
  
  def test_init( self ):
    ''' tests the __init__ method
    '''
    
    module = self.testClass()
    module.jobID = '123'
        
  def test_resolveInputVariables( self ):
    ''' tests the method resolveInputVariables
    '''  
    
    # By default the *_commons members are None, we expect them to be dictionaries
    module = self.testClass()
    self.assertRaises( TypeError, module.resolveInputVariables )
    
    module.step_commons     = {}
    module.workflow_commons = {}
    module.resolveInputVariables()
    
    self.assertEqual( True, module.enable )
    self.assertEqual( None, module.jobReport )
    
    module.enable = False
    module.resolveInputVariables()

    self.assertEqual( False, module.enable )
    self.assertEqual( None, module.jobReport )    
    
    module.step_commons[ 'enable' ] = 123
    module.resolveInputVariables()

    self.assertEqual( False, module.enable )
    self.assertEqual( None, module.jobReport )  

    module.step_commons[ 'enable' ] = True
    module.resolveInputVariables()

    self.assertEqual( True, module.enable )
    self.assertEqual( None, module.jobReport )  

    module.step_commons[ 'enable' ] = False
    module.resolveInputVariables()

    self.assertEqual( False, module.enable )
    self.assertEqual( None, module.jobReport )
    
    module.jobReport = 1
    module.resolveInputVariables()
    self.assertEqual( False, module.enable )
    self.assertEqual( 1, module.jobReport )
            
    module.workflow_commons[ 'JobReport' ] = 123
    module.resolveInputVariables()
      
    self.assertEqual( False, module.enable )
    self.assertEqual( 123, module.jobReport )

  def test_setSAMLogFile( self ):
    ''' tests the method samLogFile
    '''
    
    module = self.testClass()
    res = module.setSAMLogFile()
    self.assertEqual( False, res[ 'OK' ] )
    
    module.logFile = 'logFile'
    res = module.setSAMLogFile()
    self.assertEqual( False, res[ 'OK' ] )
    
    module.testName = 'testName'
    self.assertRaises( TypeError, module.setSAMLogFile )
    
    module.workflow_commons = {}
    res = module.setSAMLogFile()
    self.assertEqual( True, res[ 'OK' ] )
    self.assertEquals( 'logFile', module.workflow_commons[ 'SAMLogs' ][ 'testName' ] )
    
  def test_setApplicationStatus( self ):
    ''' tests the method setApplicationStatus
    '''  
    
    module = self.testClass()
    module.jobID = None
    res = module.setApplicationStatus( False )
    self.assertEqual( True, res[ 'OK' ] )
    self.assertEqual( 'JobID not defined', res[ 'Value' ] )
    
    module.jobID = 123
    res = module.setApplicationStatus( False )
    self.assertEqual( True, res[ 'OK' ] )
    self.assertEqual( 'No reporting tool given', res[ 'Value' ] )
    
    jobReport = mock.Mock()
    jobReport.setApplicationStatus.return_value = { 'OK' : True, 'Value' : 'X' }
    module.jobReport = jobReport
    
    res = module.setApplicationStatus( False )
    self.assertEqual( True, res[ 'OK' ] )
    self.assertEqual( 'X', res[ 'Value' ] )
    
    jobReport.setApplicationStatus.return_value = { 'OK' : False, 'Message' : 'Y' }
    module.jobReport = jobReport
    
    res = module.setApplicationStatus( False )
    self.assertEqual( False, res[ 'OK' ] )
    self.assertEqual( 'Y', res[ 'Message' ] )

  def test_setJobParameter( self ):
    ''' tests the method setJobParameter
    '''

    module       = self.testClass()
    module.jobID = None
    
    res = module.setJobParameter( 'name', 'value' )
    self.assertEqual( True, res[ 'OK' ] )
    self.assertEqual( 'JobID not defined', res[ 'Value' ] )
    
    module.jobID = 123
    self.assertRaises( TypeError, module.setJobParameter )
    
    module.workflow_commons = {}
    
    res = module.setJobParameter( 'name', 'value' )
    self.assertEqual( True, res[ 'OK' ] )
    self.assertEqual( 'No reporting tool given', res[ 'Value' ] )

    jobReport = mock.Mock()
    jobReport.setJobParameter.return_value = { 'OK' : True, 'Value' : 'X' } 
    module.workflow_commons[ 'JobReport' ] = jobReport

    res = module.setJobParameter( 'name', 'value' )
    self.assertEqual( True, res[ 'OK' ] )
    self.assertEqual( 'X', res[ 'Value' ] )    

    jobReport.setJobParameter.return_value = { 'OK' : False, 'Message' : 'Y' } 
    module.workflow_commons[ 'JobReport' ] = jobReport

    res = module.setJobParameter( 'name', 'value' )
    self.assertEqual( False, res[ 'OK' ] )
    self.assertEqual( 'Y', res[ 'Message' ] )    

  def test__execute( self ):
    ''' tests the method _execute
    '''
    
    module = self.testClass()
    res    = module._execute()
    
    self.assertEqual( True, res[ 'OK' ] )
    
  def test_getMessageString( self ):
    ''' tests the method _getMessageString
    '''  
    
    module = self.testClass()
    
    res = module._getMessageString( '' ) 
    self.assertEqual( '\n\n\n', res )
    res = module._getMessageString( 'abc' )
    self.assertEqual( '---\nabc\n---\n', res )
    
    res = module._getMessageString( '', True ) 
    self.assertEqual( '\n\n\n\n', res )
    res = module._getMessageString( 'abc', True )
    self.assertEqual( '\n===\nabc\n===\n', res )

    res = module._getMessageString( '1\n2' ) 
    self.assertEqual( '-\n1\n2\n-\n', res )
    res = module._getMessageString( 'abc\ndefg' )
    self.assertEqual( '----\nabc\ndefg\n----\n', res )
    
    res = module._getMessageString( '1\n2', True ) 
    self.assertEqual( '\n=\n1\n2\n=\n', res )
    res = module._getMessageString( 'abc\ndefg', True )
    self.assertEqual( '\n====\nabc\ndefg\n====\n', res )
    
  def test_runCommand( self ):
    ''' tests the method runCommand
    '''
    
    module = self.testClass()
    module.logFile = '/dev/null'
    
    res = module.runCommand( 'message', 'command' )
    self.assertEquals( True, res[ 'OK' ] )
    self.assertEquals( 'stdout', res[ 'Value' ] )
        
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF