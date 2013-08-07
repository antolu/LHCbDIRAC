#''' Test_RSS_Policy_PilotEfficiencyPolicy
#'''
#
#import mock
#import unittest
#
#import LHCbDIRAC.ResourceStatusSystem.Policy.PilotEfficiencyPolicy as moduleTested
#
#################################################################################
#
#class PilotEfficiencyPolicy_TestCase( unittest.TestCase ):
#  
#  def setUp( self ):
#    '''
#    Setup
#    '''
#                  
#    self.moduleTested = moduleTested
#    self.testClass    = self.moduleTested.PilotEfficiencyPolicy
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
#class PilotEfficiencyPolicy_Success( PilotEfficiencyPolicy_TestCase ):
#  
#  def test_instantiate( self ):
#    ''' tests that we can instantiate one object of the tested class
#    '''  
#   
#    module = self.testClass()
#    self.assertEqual( 'PilotEfficiencyPolicy', module.__class__.__name__ )
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
#    res  = module._evaluate( { 'OK' : True, 'Value' : [{ 'Aborted' : 0, 'Deleted' : 0,
#                                                         'Done' : 0, 'Failed' : 0 }] } )
#    self.assertEquals( True, res[ 'OK' ] )
#    self.assertEquals( 'Unknown', res[ 'Value' ][ 'Status' ] )
#    self.assertEquals( 'No pilots to take a decision', res[ 'Value' ][ 'Reason' ] )    
#    
#    res  = module._evaluate( { 'OK' : True, 'Value' : [{ 'Aborted' : 1, 'Deleted' : 0,
#                                                         'Done' : 1, 'Failed' : 0 }] } )
#    self.assertEquals( True, res[ 'OK' ] )
#    self.assertEquals( 'Banned', res[ 'Value' ][ 'Status' ] )
#    self.assertEquals( 'Pilots Efficiency of 0.50', res[ 'Value' ][ 'Reason' ] )    
#
#    res  = module._evaluate( { 'OK' : True, 'Value' : [{ 'Aborted' : 0, 'Deleted' : 0,
#                                                         'Done' : 3, 'Failed' : 1 }] } )
#    self.assertEquals( True, res[ 'OK' ] )
#    self.assertEquals( 'Degraded', res[ 'Value' ][ 'Status' ] )
#    self.assertEquals( 'Pilots Efficiency of 0.75', res[ 'Value' ][ 'Reason' ] )
#
#    res  = module._evaluate( { 'OK' : True, 'Value' : [{ 'Aborted' : 0, 'Deleted' : 0,
#                                                         'Done' : 19, 'Failed' : 1 }] } )
#    self.assertEquals( True, res[ 'OK' ] )
#    self.assertEquals( 'Active', res[ 'Value' ][ 'Status' ] )
#    self.assertEquals( 'Pilots Efficiency of 0.95', res[ 'Value' ][ 'Reason' ] )
#
#################################################################################    
##EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF