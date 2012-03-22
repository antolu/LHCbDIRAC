# $HeadURL: $
''' Test_RSS_Policy_SLSPolicy

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

class Configurations:
  
  @staticmethod
  def getPolicyParameters():
    return { 'Transfer_QUALITY_LOW'  : 5,
                'Transfer_QUALITY_HIGH' : 100 }

class TransferQualityPolicy_TestCase( unittest.TestCase ):
  
  def setUp( self ):
    
    # We need the proper software, and then we overwrite it.
    import LHCbDIRAC.ResourceStatusSystem.Policy.TransferQualityPolicy as moduleTested
    moduleTested.PolicyBase = PolicyBase   
    moduleTested.TransferQualityPolicy.__bases__ = ( PolicyBase, ) 
    moduleTested.Configurations = Configurations()

    self.policy = moduleTested.TransferQualityPolicy

  def tearDown( self ):
    
    del self.policy

class TransferQualityPolicy_Success( TransferQualityPolicy_TestCase ):
  
  def test_instantiate( self ):
    ''' tests that we can instantiate one object of the tested class
    '''  
    p = self.policy()
    self.assertEqual( 'TransferQualityPolicy', p.__class__.__name__ )
  
  def test_evaluate_none( self ):
    ''' tests that we can evaluate the policy when none is returned
    '''
    p   = self.policy()
    p.args = [ '1', '2' ]
    res = p.evaluate()
    self.assertEqual( res[ 'Status' ], 'Error' )
        
  def test_evaluate_ok( self ):
    ''' tests that we can evaluate the policy when S_OK is returned
    '''
    
    p = self.policy()
    
    p.commandRes = { 'OK' : True, 'Value' : None }
    res = p.evaluate()
    self.assertEqual( res[ 'Status' ], 'Unknown' )
 
    p.args       = [ '1', '2' ]
    p.commandRes = { 'OK' : True, 'Value' : 0 }
    res = p.evaluate()
    self.assertEqual( res[ 'Status' ], 'Bad' )
    
    p.args       = [ '1', '2' ]
    p.commandRes = { 'OK' : True, 'Value' : 10 }
    res = p.evaluate()
    self.assertEqual( res[ 'Status' ], 'Probing' )

    p.args       = [ '1', '2' ]
    p.commandRes = { 'OK' : True, 'Value' : 1000 }
    res = p.evaluate()
    self.assertEqual( res[ 'Status' ], 'Active' )
        
    p.args       = [ '1', 'FAILOVER' ]
    p.commandRes = { 'OK' : True, 'Value' : 0 }
    res = p.evaluate()
    self.assertEqual( res[ 'Status' ], 'Probing' )
    
    p.args       = [ '1', 'FAILOVER' ]
    p.commandRes = { 'OK' : True, 'Value' : 10 }
    res = p.evaluate()
    self.assertEqual( res[ 'Status' ], 'Active' )
    
    p.args       = [ '1', 'FAILOVER' ]
    p.commandRes = { 'OK' : True, 'Value' : 1000 }
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