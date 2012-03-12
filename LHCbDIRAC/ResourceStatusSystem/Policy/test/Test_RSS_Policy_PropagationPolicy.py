# $HeadURL: $
''' Test_RSS_Policy_PropagationPolicy

  Simplest test case for the RSS policies. Can be taken as example for other
  tests.

'''

import unittest

__RCSID__ = '$Id: $'

class PolicyBase( object ):
  
  def __init__( self ):
    self.commandRes = None
    
  def evaluate( self ):
    return self.commandRes

class PropagationPolicy_TestCase( unittest.TestCase ):
  
  def setUp( self ):
    
    # We need the proper software, and then we overwrite it.
    import LHCbDIRAC.ResourceStatusSystem.Policy.Propagation_Policy as moduleTested
    moduleTested.PolicyBase = PolicyBase   
    moduleTested.Propagation_Policy.__bases__ = ( PolicyBase, ) 

    self.policy = moduleTested.Propagation_Policy

  def tearDown( self ):
    
    del self.policy

class PropagationPolicy_Success( PropagationPolicy_TestCase ):
  
  def test_instantiate( self ):
    ''' tests that we can instantiate one object of the tested class
    '''  
    p = self.policy()
    self.assertEqual( 'Propagation_Policy', p.__class__.__name__ )
  
  def test_evaluate_none( self ):
    ''' tests that we can evaluate the policy when none is returned
    '''
    p   = self.policy()
    res = p.evaluate()
    self.assertEqual( res[ 'Status' ], 'Error' )
        
  def test_evaluate_ok( self ):
    ''' tests that we can evaluate the policy when S_OK is returned
    '''
    
    p   = self.policy()
    stats = { 'Active' : 0, 'Bad' : 0, 'Probing' : 0, 'Banned' : 0 }
    p.commandRes = { 'OK' : True, 'Value' : stats }
    res = p.evaluate()
    self.assertEqual( res[ 'Status' ], 'Unknown' )
    stats = { 'Active' : 1, 'Bad' : 0, 'Probing' : 0, 'Banned' : 0 }
    p.commandRes = { 'OK' : True, 'Value' : stats }
    res = p.evaluate()
    self.assertEqual( res[ 'Status' ], 'Active' )
    stats = { 'Active' : 1, 'Bad' : 1, 'Probing' : 0, 'Banned' : 0 }
    p.commandRes = { 'OK' : True, 'Value' : stats }
    res = p.evaluate()
    self.assertEqual( res[ 'Status' ], 'Bad' )
    stats = { 'Active' : 0, 'Bad' : 1, 'Probing' : 1, 'Banned' : 0 }
    p.commandRes = { 'OK' : True, 'Value' : stats }
    res = p.evaluate()
    self.assertEqual( res[ 'Status' ], 'Bad' )
    stats = { 'Active' : 0, 'Bad' : 0, 'Probing' : 1, 'Banned' : 1 }
    p.commandRes = { 'OK' : True, 'Value' : stats }
    res = p.evaluate()
    self.assertEqual( res[ 'Status' ], 'Bad' )
    stats = { 'Active' : 0, 'Bad' : 0, 'Probing' : 0, 'Banned' : 1 }
    p.commandRes = { 'OK' : True, 'Value' : stats }
    res = p.evaluate()
    self.assertEqual( res[ 'Status' ], 'Banned' )
    
            
  def test_evaluate_nok( self ):
    ''' tests that we can evaluate the policy when S_ERROR is returned
    '''
    p   = self.policy()
    p.commandRes = { 'OK' : False, 'Message' : 'Error Message' }
    res = p.evaluate()
    self.assertEqual( res[ 'Status' ], 'Error' )      
        
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF  