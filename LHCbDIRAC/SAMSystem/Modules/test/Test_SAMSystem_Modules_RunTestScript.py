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
         
    self.moduleTested = moduleTested
    self.testClass    = self.moduleTested.ModuleBaseSAM

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
    
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF