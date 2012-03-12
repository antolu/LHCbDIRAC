# $HeadURL: $
''' Test_RSS_Policy_DownHillPropagationPolicy

  Simplest test case for the RSS policies. Can be taken as example for other
  tests.

'''

import unittest

__RCSID__ = '$Id: $'

class PolicyBase( object ):
  
  def evaluate( self, result = None ):
    return result

class DownHillPropagationPolicy_TestCase( unittest.TestCase ):
  
  def setUp( self ):
    
    # We need the proper software, and then we overwrite it.
    import LHCbDIRAC.ResourceStatusSystem.Policy.DownHillPropagation_Policy as moduleTested
    moduleTested.PolicyBase = PolicyBase   
    moduleTested.DownHillPropagation_Policy.__bases__ = ( PolicyBase, ) 

    self.policy = moduleTested.DownHillPropagation_Policy

  def tearDown( self ):
    
    del self.policy

class DownHillPropagationPolicy_Success( DownHillPropagationPolicy_TestCase ):
  
  def test_instantiate( self ):
    ''' tests that we can instantiate one object of the tested class
    '''  
    p = self.policy()
    self.assertEqual( 'DownHillPropagation_Policy', p.__class__.__name__ )
  
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
    res = p.evaluate( result = { 'OK' : True, 'Value' : None } )
    self.assertEqual( res[ 'Status' ], 'Unkown' )
    res = p.evaluate( result = { 'OK' : True, 'Value' : 'Active' } )
    self.assertEqual( res[ 'Status' ], 'Active' )
        
  def test_evaluate_nok( self ):
    ''' tests that we can evaluate the policy when S_ERROR is returned
    '''
    p   = self.policy()
    res = p.evaluate( result = { 'OK' : False, 'Message' : 'Error Message' } )
    self.assertEqual( res[ 'Status' ], 'Error' )      
        
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF  