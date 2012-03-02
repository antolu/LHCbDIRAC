# $HeadURL: $
''' TestAlwaysFalsePolicy

  Simplest test case for the RSS policies. Can be taken as example for other
  tests.

'''

import unittest

class PolicyBase:
  
  def evaluate( self ):
    pass

__RCSID__ = '$Id: $'

class AlwaysFalseTestCase( unittest.TestCase ):
  
  def setUp( self ):
    
    # We need the proper software, and then we overwrite it.
    import LHCbDIRAC.ResourceStatusSystem.Policy.AlwaysFalse_Policy as moduleTested
    moduleTested.PolicyBase = PolicyBase   
    moduleTested.AlwaysFalse_Policy.__bases__ = ( PolicyBase, ) 

    self.policy = moduleTested.AlwaysFalse_Policy

  def tearDown( self ):
    
    del self.policy

class AlwaysFalse_Success( AlwaysFalseTestCase ):
  
  def test_instantiate( self ):
    ''' tests that we can instantiate one object of the tested class
    '''  
    p = self.policy()
    #self.assertEqual( AlwaysFalse_Policy, p.__class__ )
    self.assertEqual( 1, 1 )
  
  def test_evaluate( self ):
    ''' tests that we can evaluate the policy
    '''
    p   = self.policy()
    res = p.evaluate()
    self.assertEqual( res[ 'Status' ], 'Active' )
        
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF  