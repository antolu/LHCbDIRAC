''' Test_RSS_Policy_SAMResultsPolicy

  Simplest test case for the RSS policies. Can be taken as example for other
  tests.

'''

import unittest

__RCSID__ = '$Id$'

class PolicyBase( object ):
  
  def __init__( self ):
    self.args       = None
    self.commandRes = None
    
  def evaluate( self ):
    return self.commandRes

class SAMResultsPolicy_TestCase( unittest.TestCase ):
  
  def setUp( self ):
    
    # We need the proper software, and then we overwrite it.
    import LHCbDIRAC.ResourceStatusSystem.Policy.SAMResultsPolicy as moduleTested
    moduleTested.PolicyBase = PolicyBase   
    moduleTested.SAMResultsPolicy.__bases__ = ( PolicyBase, ) 

    self.policy = moduleTested.SAMResultsPolicy

  def tearDown( self ):
    
    del self.policy

class SAMResultsPolicy_Success( SAMResultsPolicy_TestCase ):
  
  def test_instantiate( self ):
    ''' tests that we can instantiate one object of the tested class
    '''  
    p = self.policy()
    self.assertEqual( 'SAMResultsPolicy', p.__class__.__name__ )
  
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
    
    status       = { 1 : 'error' }
    p.commandRes = { 'OK' : True, 'Value' : status }
    res = p.evaluate()
    self.assertEqual( res[ 'Status' ], 'Bad' )

    status       = { 1 : 'down' }    
    p.commandRes = { 'OK' : True, 'Value' : status }
    res = p.evaluate()
    self.assertEqual( res[ 'Status' ], 'Bad' )
    
    status       = { 1 : 'warn' }
    p.commandRes = { 'OK' : True, 'Value' : status }
    res = p.evaluate()
    self.assertEqual( res[ 'Status' ], 'Probing' )
    
    status       = { 1 : 'maint' }
    p.commandRes = { 'OK' : True, 'Value' : status }
    res = p.evaluate()
    self.assertEqual( res[ 'Status' ], 'Bad' )
    
    status       = { 1 : 'na' }
    p.commandRes = { 'OK' : True, 'Value' : status }
    res = p.evaluate()
    self.assertEqual( res[ 'Status' ], 'Unknown' )
    
    status       = { 1 : 'nana' }
    p.commandRes = { 'OK' : True, 'Value' : status }
    res = p.evaluate()
    self.assertEqual( res[ 'Status' ], 'Active' )
    
  def test_evaluate_nok( self ):
    ''' tests that we can evaluate the policy when S_ERROR is returned
    '''
    p   = self.policy()
    p.commandRes = { 'OK' : False, 'Message' : 'Error Message' }
    res = p.evaluate()
    self.assertEqual( res[ 'Status' ], 'Error' )      
        
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF  