#''' Test_RSS_Policy_PilotWebSummaryEfficiencyPolicy
#'''
#
#import mock
#import unittest
#
#import LHCbDIRAC.ResourceStatusSystem.Policy.PilotWebSummaryEfficiencyPolicy as moduleTested
#
#################################################################################
#
#class PilotWebSummaryEfficiencyPolicy_TestCase( unittest.TestCase ):
#  
#  def setUp( self ):
#    '''
#    Setup
#    '''
#                  
#    self.moduleTested = moduleTested
#    self.testClass    = self.moduleTested.PilotWebSummaryEfficiencyPolicy
#
#  def tearDown( self ):
#    '''
#    Tear down
#    '''
#   
#    del self.moduleTested
#    del self.testClass
#
#################################################################################
#
#class PilotWebSummaryEfficiencyPolicy_Success( PilotWebSummaryEfficiencyPolicy_TestCase ):
#  
#  def test_instantiate( self ):
#    ''' tests that we can instantiate one object of the tested class
#    '''  
#   
#    module = self.testClass()
#    self.assertEqual( 'PilotWebSummaryEfficiencyPolicy', module.__class__.__name__ )
#
#  def test_evaluate( self ):
#    ''' tests the method _evaluate
#    '''
#    
#    module = self.testClass()
#    
#    res = module._evaluate( { 'OK' : False, 'Message' : 'Bo!' } )
#    self.assertTrue(res['OK'])
#    self.assertEqual( 'Error', res[ 'Value' ][ 'Status' ] )
#    self.assertEqual( 'Bo!', res[ 'Value' ][ 'Reason' ] )
#
#    res = module._evaluate( { 'OK' : True, 'Value' : None } )
#    self.assertTrue(res['OK'])
#    self.assertEqual( 'Unknown', res[ 'Value' ][ 'Status' ] )
#    self.assertEqual( 'No values to take a decision', res[ 'Value' ][ 'Reason' ] )
#
#    res = module._evaluate( { 'OK' : True, 'Value' : [] } )
#    self.assertTrue(res['OK'])
#    self.assertEqual( 'Unknown', res[ 'Value' ][ 'Status' ] )
#    self.assertEqual( 'No values to take a decision', res[ 'Value' ][ 'Reason' ] )
#
#    res = module._evaluate( { 'OK' : True, 'Value' : [{ 'A' : 1 }] } )
#    self.assertTrue(res['OK'])
#    self.assertEqual( 'Error', res[ 'Value' ][ 'Status' ] )
#    self.assertEqual( '"Status" key missing', res[ 'Value' ][ 'Reason' ] )    
#
#    res = module._evaluate( { 'OK' : True, 'Value' : [{ 'Status' : 1 }] } )
#    self.assertTrue(res['OK'])
#    self.assertEqual( 'Error', res[ 'Value' ][ 'Status' ] )
#    self.assertEqual( '"PilotJobEff" key missing', res[ 'Value' ][ 'Reason' ] )              
#
#    res = module._evaluate( { 'OK' : True, 'Value' : [{ 'Status' : 1, 'PilotJobEff' : 1 }] } )
#    self.assertTrue(res['OK'])
#    self.assertEqual( 'Error', res[ 'Value' ][ 'Status' ] )
#    self.assertEqual( 'Unknown status "1"', res[ 'Value' ][ 'Reason' ] )
#
#    res = module._evaluate( { 'OK' : True, 'Value' : [{ 'Status' : 'Good', 'PilotJobEff' : 1 }] } )
#    self.assertTrue(res['OK'])
#    self.assertEqual( 'Active', res[ 'Value' ][ 'Status' ] )
#    self.assertEqual( 'Pilots Efficiency: 1 with status Good', res[ 'Value' ][ 'Reason' ] )
#
#    res = module._evaluate( { 'OK' : True, 'Value' : [{ 'Status' : 'Fair', 'PilotJobEff' : 1 }] } )
#    self.assertTrue(res['OK'])
#    self.assertEqual( 'Active', res[ 'Value' ][ 'Status' ] )
#    self.assertEqual( 'Pilots Efficiency: 1 with status Fair', res[ 'Value' ][ 'Reason' ] )
#
#    res = module._evaluate( { 'OK' : True, 'Value' : [{ 'Status' : 'Poor', 'PilotJobEff' : 1 }] } )
#    self.assertTrue(res['OK'])
#    self.assertEqual( 'Degraded', res[ 'Value' ][ 'Status' ] )
#    self.assertEqual( 'Pilots Efficiency: 1 with status Poor', res[ 'Value' ][ 'Reason' ] )
#
#    res = module._evaluate( { 'OK' : True, 'Value' : [{ 'Status' : 'Idle', 'PilotJobEff' : 1 }] } )
#    self.assertTrue(res['OK'])
#    self.assertEqual( 'Unknown', res[ 'Value' ][ 'Status' ] )
#    self.assertEqual( 'Pilots Efficiency: 1 with status Idle', res[ 'Value' ][ 'Reason' ] )
#
#    res = module._evaluate( { 'OK' : True, 'Value' : [{ 'Status' : 'Bad', 'PilotJobEff' : 1 }] } )
#    self.assertTrue(res['OK'])
#    self.assertEqual( 'Banned', res[ 'Value' ][ 'Status' ] )
#    self.assertEqual( 'Pilots Efficiency: 1 with status Bad', res[ 'Value' ][ 'Reason' ] )
#    
#################################################################################
##EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF