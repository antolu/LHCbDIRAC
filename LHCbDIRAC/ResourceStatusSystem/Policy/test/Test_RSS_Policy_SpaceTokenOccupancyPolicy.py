#''' Test_RSS_Policy_SpaceTokenOccupancyPolicy
#'''
#
#import mock
#import unittest
#
#import LHCbDIRAC.ResourceStatusSystem.Policy.SpaceTokenOccupancyPolicy as moduleTested
#
#################################################################################
#
#class SpaceTokenOccupancyPolicy_TestCase( unittest.TestCase ):
#  
#  def setUp( self ):
#    '''
#    Setup
#    '''
#                  
#    self.moduleTested = moduleTested
#    self.testClass    = self.moduleTested.SpaceTokenOccupancyPolicy
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
#class SpaceTokenOccupancyPolicy_Success( SpaceTokenOccupancyPolicy_TestCase ):
#
#  def test_instantiate( self ):
#    ''' tests that we can instantiate one object of the tested class
#    '''  
#   
#    module = self.testClass()
#    self.assertEqual( 'SpaceTokenOccupancyPolicy', module.__class__.__name__ )
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
#    res = module._evaluate( { 'OK' : True, 'Value' : [{ 'A' : 1 }] } )
#    self.assertEquals( True, res[ 'OK' ] )
#    self.assertEquals( 'Error', res[ 'Value' ][ 'Status' ] )
#    self.assertEquals( 'Key total missing', res[ 'Value' ][ 'Reason' ] )   
#
#    res = module._evaluate( { 'OK' : True, 'Value' : [{ 'Total' : 1 }] } )
#    self.assertEquals( True, res[ 'OK' ] )
#    self.assertEquals( 'Error', res[ 'Value' ][ 'Status' ] )
#    self.assertEquals( 'Key free missing', res[ 'Value' ][ 'Reason' ] ) 
#
#    res = module._evaluate( { 'OK' : True, 'Value' : [{ 'Total' : 100, 'Free' : 0.0 }] } )
#    self.assertEquals( True, res[ 'OK' ] )
#    self.assertEquals( 'Error', res[ 'Value' ][ 'Status' ] )
#    self.assertEquals( 'Key guaranteed missing', res[ 'Value' ][ 'Reason' ] ) 
#
#    res = module._evaluate( { 'OK' : True, 'Value' : [{ 'Total' : 100, 'Free' : 0.0,
#                                                        'Guaranteed' : 1 }] } )
#    self.assertEquals( True, res[ 'OK' ] )
#    self.assertEquals( 'Banned', res[ 'Value' ][ 'Status' ] )
#    self.assertEquals( 'Space availability: 0 % (SE Full!)', res[ 'Value' ][ 'Reason' ] ) 
#
#    res = module._evaluate( { 'OK' : True, 'Value' : [{ 'Total' : 100, 'Free' : 9.0,
#                                                        'Guaranteed' : 1 }] } )
#    self.assertEquals( True, res[ 'OK' ] )
#    self.assertEquals( 'Degraded', res[ 'Value' ][ 'Status' ] )
#    self.assertEquals( 'Space availability: 9 % (SE has not much space left)', 
#                       res[ 'Value' ][ 'Reason' ] ) 
#
#    res = module._evaluate( { 'OK' : True, 'Value' : [{ 'Total' : 100, 'Free' : 100,
#                                                        'Guaranteed' : 1 }] } )
#    self.assertEquals( True, res[ 'OK' ] )
#    self.assertEquals( 'Active', res[ 'Value' ][ 'Status' ] )
#    self.assertEquals( 'Space availability: 100 % (SE has enough space left)', 
#                       res[ 'Value' ][ 'Reason' ] ) 
#
#
#################################################################################
##EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF