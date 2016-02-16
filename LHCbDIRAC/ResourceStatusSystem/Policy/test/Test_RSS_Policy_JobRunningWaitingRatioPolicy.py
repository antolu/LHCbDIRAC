#''' Test_RSS_Policy_JobRunningWaitingRatioPolicy
#'''
#
#import mock
#import unittest
#
#import LHCbDIRAC.ResourceStatusSystem.Policy.JobRunningWaitingRatioPolicy as moduleTested
#
#################################################################################
#
#class JobRunningWaitingRatioPolicy_TestCase( unittest.TestCase ):
#  
#  def setUp( self ):
#    '''
#    Setup
#    '''
#                  
#    self.moduleTested = moduleTested
#    self.testClass    = self.moduleTested.JobRunningWaitingRatioPolicy
#
#  def tearDown( self ):
#    '''
#    Tear down
#    '''
#   
#    del self.moduleTested
#    del self.testClass
#  
#
#################################################################################
#
#class JobRunningWaitingRatioPolicy_Success( JobRunningWaitingRatioPolicy_TestCase ):
#  
#  def test_instantiate( self ):
#    ''' tests that we can instantiate one object of the tested class
#    '''  
#   
#    module = self.testClass()
#    self.assertEqual( 'JobRunningWaitingRatioPolicy', module.__class__.__name__ )
#
#  def test_evaluate( self ):
#    ''' tests the method _evaluate
#    '''  
#        
#    module = self.testClass()
#    
#    res = module._evaluate( { 'OK' : False, 'Message' : 'Bo!' } )
#    self.assertEquals( True, res[ 'OK' ] )
#    self.assertEquals( 'Error', res[ 'Value' ][ 'Status' ] )
#    self.assertEquals( 'Bo!', res[ 'Value' ][ 'Reason' ] )
#
#    res = module._evaluate( { 'OK' : True, 'Value' : None } )
#    self.assertEquals( True, res[ 'OK' ] )
#    self.assertEquals( 'Unknown', res[ 'Value' ][ 'Status' ] )
#    self.assertEquals( 'No values to take a decision', res[ 'Value' ][ 'Reason' ] )
#
#    res = module._evaluate( { 'OK' : True, 'Value' : [] } )
#    self.assertEquals( True, res[ 'OK' ] )
#    self.assertEquals( 'Unknown', res[ 'Value' ][ 'Status' ] )
#    self.assertEquals( 'No values to take a decision', res[ 'Value' ][ 'Reason' ] )
#
#    res = module._evaluate( { 'OK' : True, 'Value' : [{}] } )
#    self.assertEquals( True, res[ 'OK' ] )
#    self.assertEquals( 'Unknown', res[ 'Value' ][ 'Status' ] )
#    self.assertEquals( 'No values to take a decision', res[ 'Value' ][ 'Reason' ] )
#    
#    res  = module._evaluate( { 'OK' : True, 'Value' : [{ 'Running' : 0, 'Waiting' : 0, 'Staging' : 0 }] } )
#    self.assertEquals( True, res[ 'OK' ] )
#    self.assertEquals( 'Unknown', res[ 'Value' ][ 'Status' ] )
#    self.assertEquals( 'No jobs take a decision', res[ 'Value' ][ 'Reason' ] )    
#    
#    res  = module._evaluate( { 'OK' : True, 'Value' : [{ 'Running' : 1, 'Waiting' : 1, 'Staging' : 1 }] } )
#    self.assertEquals( True, res[ 'OK' ] )
#    self.assertEquals( 'Banned', res[ 'Value' ][ 'Status' ] )
#    self.assertEquals( 'Job Running / Waiting ratio of 0.33', res[ 'Value' ][ 'Reason' ] )    
#
#    res  = module._evaluate( { 'OK' : True, 'Value' : [{ 'Running' : 1, 'Waiting' : 1, 'Staging' : 0 }] } )
#    self.assertEquals( True, res[ 'OK' ] )
#    self.assertEquals( 'Degraded', res[ 'Value' ][ 'Status' ] )
#    self.assertEquals( 'Job Running / Waiting ratio of 0.50', res[ 'Value' ][ 'Reason' ] )
#
#    res  = module._evaluate( { 'OK' : True, 'Value' : [{ 'Running' : 2, 'Waiting' : 1, 'Staging' : 0 }] } )
#    self.assertEquals( True, res[ 'OK' ] )
#    self.assertEquals( 'Active', res[ 'Value' ][ 'Status' ] )
#    self.assertEquals( 'Job Running / Waiting ratio of 0.67', res[ 'Value' ][ 'Reason' ] )
#
#################################################################################
##EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF