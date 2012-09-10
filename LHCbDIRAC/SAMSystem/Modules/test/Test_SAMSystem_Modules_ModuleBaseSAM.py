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

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF