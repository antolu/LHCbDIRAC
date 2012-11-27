''' Test_TS_Client_TaskManager

'''

import mock
import unittest

import LHCbDIRAC.TransformationSystem.Client.TaskManager as moduleTested 

__RCSID__ = '$Id: $'
     
################################################################################

class TaskManager_TestCase( unittest.TestCase ):
  
  def setUp( self ):
    '''
    Setup
    '''        
    self.moduleTested = moduleTested
    self.testClass    = self.moduleTested.LHCbWorkflowTasks
    
  def tearDown( self ):
    '''
    TearDown
    '''
    del self.testClass
    del self.moduleTested     
     
################################################################################
# Tests

class TaskManager_Success( TaskManager_TestCase ):

  def test_instantiate( self ):
    ''' tests that we can instantiate one object of the tested class
    '''  
    instance = self.testClass()
    self.assertEqual( 'LHCbWorkflowTasks', instance.__class__.__name__ ) 

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF    