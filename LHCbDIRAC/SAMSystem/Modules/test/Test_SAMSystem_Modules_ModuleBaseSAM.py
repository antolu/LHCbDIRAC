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
    
  def test_resolveInputVariables( self ):
    ''' tests the medhot resolveInputVariables
    '''  
    
    module = self.testClass()
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
        
    module.workflow_commons[ 'JobReport' ] = 1
      
    self.assertEqual( False, module.enable )
    self.assertEqual( 1, module.jobReport )
    

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF