# $HeadURL: $
''' Test_TS_Client_TaskManager

'''

import mock
import unittest

import LHCbDIRAC.TransformationSystem.Client.TaskManager as moduleTested 

__RCSID__ = '$Id: $'
     
################################################################################
# Tests

class TaskManagerTest( unittest.TestCase ):
  
  def setUp( self ):
    ''' Setup
    '''
    self.testClass = mock.Mock()
  
  def tearDown( self ):
    ''' TearDown
    '''
    del self.testClass
  
  def test_instantiate( self ):
    ''' tests that we can instantiate one object of the tested class
    '''  
    
    instance = self.testClass()
    self.assertEqual( 'TaskManager', instance.__class__.__name__ )

################################################################################
# Fixtures

class DefaultTestCase( TaskManagerTest ):
  
  def setUp( self ):
    '''
    Setup
    '''
#    mock_DiracLHCb      = mock.Mock( return_value = { 'OK' : True, 'Value' : 1 } )
#    self.mock_DiracLHCb = mock_DiracLHCb 
#    
#    moduleTested.DiracLHCb = lambda :self.mock_DiracLHCb
        
    self.moduleTested = moduleTested
    self.testClass    = self.moduleTested.LHCbWorkflowTasks
    
  def tearDown( self ):
    '''
    TearDown
    '''
    del self.testClass
    del self.moduleTested

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF    