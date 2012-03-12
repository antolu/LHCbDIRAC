# $HeadURL: $
''' Test_RSS_Policy_SEOccupancyPolicy

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

class SEOccupancyPolicy_TestCase( unittest.TestCase ):
  
  def setUp( self ):
    
    # We need the proper software, and then we overwrite it.
    import LHCbDIRAC.ResourceStatusSystem.Policy.SEOccupancys_Policy as moduleTested
    moduleTested.PolicyBase = PolicyBase   
    moduleTested.SEOccupancy_Policy.__bases__ = ( PolicyBase, ) 

    self.policy = moduleTested.SEOccupancy_Policy

  def tearDown( self ):
    
    del self.policy

class SEOccupancyPolicy_Success( SEOccupancyPolicy_TestCase ):
  
  def test_instantiate( self ):
    ''' tests that we can instantiate one object of the tested class
    '''  
    p = self.policy()
    self.assertEqual( 'SEOccupancy_Policy', p.__class__.__name__ )
  
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
    
    p.commandRes = { 'OK' : True, 'Value' : 0 }
    res = p.evaluate()
    self.assertEqual( res[ 'Status' ], 'Banned' )
 
    p.commandRes = { 'OK' : True, 'Value' : 5 }
    res = p.evaluate()
    self.assertEqual( res[ 'Status' ], 'Bad' )
    
    p.commandRes = { 'OK' : True, 'Value' : 15 }
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