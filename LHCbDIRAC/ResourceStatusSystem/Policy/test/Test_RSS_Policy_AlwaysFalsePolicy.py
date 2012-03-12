# $HeadURL: $
''' Test_RSS_Policy_AlwaysFalsePolicy

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

class AlwaysFalsePolicy_TestCase( unittest.TestCase ):
  
  def setUp( self ):
    
    # We need the proper software, and then we overwrite it.
    import LHCbDIRAC.ResourceStatusSystem.Policy.AlwaysFalse_Policy as moduleTested
    moduleTested.PolicyBase = PolicyBase   
    moduleTested.AlwaysFalse_Policy.__bases__ = ( PolicyBase, ) 

    self.policy = moduleTested.AlwaysFalse_Policy

  def tearDown( self ):
    
    del self.policy

class AlwaysFalsePolicy_Success( AlwaysFalsePolicy_TestCase ):
  
  def test_instantiate( self ):
    ''' tests that we can instantiate one object of the tested class
    '''  
    p = self.policy()
    self.assertEqual( 'AlwaysFalse_Policy', p.__class__.__name__ )
  
  def test_evaluate_none( self ):
    ''' tests that we can evaluate the policy
    '''
    p   = self.policy()
    res = p.evaluate()
    self.assertEqual( res[ 'Status' ], 'Active' )
        
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF  