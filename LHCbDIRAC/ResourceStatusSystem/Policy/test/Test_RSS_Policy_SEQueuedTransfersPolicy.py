# $HeadURL: $
''' Test_RSS_Policy_SEQueuedTransfersPolicy

  Simplest test case for the RSS policies. Can be taken as example for other
  tests.

'''

import unittest

__RCSID__ = '$Id: $'

class PolicyBase( object ):
  
  def __init__( self ):
    self.args       = None
    self.commandRes = None
    
  def evaluate( self ):
    return self.commandRes

class SEQueuedTransfersPolicy_TestCase( unittest.TestCase ):
  
  def setUp( self ):
    
    # We need the proper software, and then we overwrite it.
    import LHCbDIRAC.ResourceStatusSystem.Policy.SEQueuedTransfers_Policy as moduleTested
    moduleTested.PolicyBase = PolicyBase   
    moduleTested.SEQueuedTransfers_Policy.__bases__ = ( PolicyBase, ) 

    self.policy = moduleTested.SEQueuedTransfers_Policy

  def tearDown( self ):
    
    del self.policy

class SEQueuedTransfersPolicy_Success( SEQueuedTransfersPolicy_TestCase ):
  
  def test_instantiate( self ):
    ''' tests that we can instantiate one object of the tested class
    '''  
    p = self.policy()
    self.assertEqual( 'SEQueuedTransfers_Policy', p.__class__.__name__ )
  
  def test_evaluate_none( self ):
    ''' tests that we can evaluate the policy when none is returned
    '''
    p   = self.policy()
    res = p.evaluate()
    self.assertEqual( res[ 'Status' ], 'Error' )
        
  def test_evaluate_ok( self ):
    ''' tests that we can evaluate the policy when S_OK is returned
    '''
    
    p = self.policy()
    
    p.commandRes = { 'OK' : True, 'Value' : { 'Queued transfers' : 0 } }
    res = p.evaluate()
    self.assertEqual( res[ 'Status' ], 'Active' )
 
    p.commandRes = { 'OK' : True, 'Value' : { 'Queued transfers' : 80 } }
    res = p.evaluate()
    self.assertEqual( res[ 'Status' ], 'Bad' )
    
    p.commandRes = { 'OK' : True, 'Value' : { 'Queued transfers' : 110 } }
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