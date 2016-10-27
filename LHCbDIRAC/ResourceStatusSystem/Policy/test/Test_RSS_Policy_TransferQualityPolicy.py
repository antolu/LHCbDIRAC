''' Test_RSS_Policy_TransferQualityPolicy
'''

import unittest

import LHCbDIRAC.ResourceStatusSystem.Policy.TransferQualityPolicy as moduleTested

################################################################################

class TransferQualityPolicy_TestCase( unittest.TestCase ):

  def setUp( self ):
    '''
    Setup
    '''

    self.moduleTested = moduleTested
    self.testClass    = self.moduleTested.TransferQualityPolicy

  def tearDown( self ):
    '''
    Tear down
    '''

    del self.moduleTested
    del self.testClass

################################################################################

class TransferQualityPolicy_Success( TransferQualityPolicy_TestCase ):

  def test_instantiate( self ):
    ''' tests that we can instantiate one object of the tested class
    '''

    module = self.testClass()
    self.assertEqual( 'TransferQualityPolicy', module.__class__.__name__ )

  def test_evaluate( self ):
    ''' tests the method _evaluate
    '''

    module = self.testClass()

    res = module._evaluate( { 'OK' : False, 'Message' : 'Bo!' } )
    self.assertEquals( True, res[ 'OK' ] )
    self.assertEquals( 'Error', res[ 'Value' ][ 'Status' ] )
    self.assertEquals( 'Bo!', res[ 'Value' ][ 'Reason' ] )

    res = module._evaluate( { 'OK' : True, 'Value' : None } )
    self.assertEquals( True, res[ 'OK' ] )
    self.assertEquals( 'Unknown', res[ 'Value' ][ 'Status' ] )
    self.assertEquals( 'No values to take a decision', res[ 'Value' ][ 'Reason' ] )

    res = module._evaluate( { 'OK' : True, 'Value' : [] } )
    self.assertEquals( True, res[ 'OK' ] )
    self.assertEquals( 'Unknown', res[ 'Value' ][ 'Status' ] )
    self.assertEquals( 'No values to take a decision', res[ 'Value' ][ 'Reason' ] )

    res = module._evaluate( { 'OK' : True, 'Value' : { 'A' : 1 } } )
    self.assertEquals( True, res[ 'OK' ] )
    self.assertEquals( 'Error', res[ 'Value' ][ 'Status' ] )
    self.assertEquals( 'Missing "Name" key', res[ 'Value' ][ 'Reason' ] )

    res = module._evaluate( { 'OK' : True, 'Value' : { 'Name' : '1' } } )
    self.assertEquals( True, res[ 'OK' ] )
    self.assertEquals( 'Error', res[ 'Value' ][ 'Status' ] )
    self.assertEquals( 'Missing "Mean" key', res[ 'Value' ][ 'Reason' ] )

    res = module._evaluate( { 'OK' : True, 'Value' : { 'Name' : '1', 'Mean' : None } } )
    self.assertEquals( True, res[ 'OK' ] )
    self.assertEquals( 'Unknown', res[ 'Value' ][ 'Status' ] )
    self.assertEquals( 'No values to take a decision', res[ 'Value' ][ 'Reason' ] )

    res = module._evaluate( { 'OK' : True, 'Value' : { 'Name' : '1', 'Mean' : 0 } } )
    self.assertEquals( True, res[ 'OK' ] )
    self.assertEquals( 'Degraded', res[ 'Value' ][ 'Status' ] )
    self.assertEquals( 'TransferQuality: 0 -> Low', res[ 'Value' ][ 'Reason' ] )

    res = module._evaluate( { 'OK' : True, 'Value' : { 'Name' : '1', 'Mean' : 70 } } )
    self.assertEquals( True, res[ 'OK' ] )
    self.assertEquals( 'Degraded', res[ 'Value' ][ 'Status' ] )
    self.assertEquals( 'TransferQuality: 70 -> Mean', res[ 'Value' ][ 'Reason' ] )

    res = module._evaluate( { 'OK' : True, 'Value' : { 'Name' : '1', 'Mean' : 95 } } )
    self.assertEquals( True, res[ 'OK' ] )
    self.assertEquals( 'Active', res[ 'Value' ][ 'Status' ] )
    self.assertEquals( 'TransferQuality: 95 -> High', res[ 'Value' ][ 'Reason' ] )

    res = module._evaluate( { 'OK' : True, 'Value' : { 'Name' : '1failover', 'Mean' : 0 } } )
    self.assertEquals( True, res[ 'OK' ] )
    self.assertEquals( 'Degraded', res[ 'Value' ][ 'Status' ] )
    self.assertEquals( 'TransferQuality: 0 -> Low', res[ 'Value' ][ 'Reason' ] )

    res = module._evaluate( { 'OK' : True, 'Value' : { 'Name' : '1failover', 'Mean' : 70 } } )
    self.assertEquals( True, res[ 'OK' ] )
    self.assertEquals( 'Active', res[ 'Value' ][ 'Status' ] )
    self.assertEquals( 'TransferQuality: 70 -> Mean', res[ 'Value' ][ 'Reason' ] )

    res = module._evaluate( { 'OK' : True, 'Value' : { 'Name' : '1failover', 'Mean' : 95 } } )
    self.assertEquals( True, res[ 'OK' ] )
    self.assertEquals( 'Active', res[ 'Value' ][ 'Status' ] )
    self.assertEquals( 'TransferQuality: 95 -> High', res[ 'Value' ][ 'Reason' ] )

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
