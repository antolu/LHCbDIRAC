''' Test_RSS_Policy_GGUSTicketsPolicy
'''

import unittest

import LHCbDIRAC.ResourceStatusSystem.Policy.GGUSTicketsPolicy as moduleTested

################################################################################

class GGUSTicketsPolicy_TestCase( unittest.TestCase ):

  def setUp( self ):
    '''
    Setup
    '''

    self.moduleTested = moduleTested
    self.testClass    = self.moduleTested.GGUSTicketsPolicy

  def tearDown( self ):
    '''
    Tear down
    '''

    del self.moduleTested
    del self.testClass


################################################################################

class GGUSTicketsPolicy_Success( GGUSTicketsPolicy_TestCase ):

  def test_instantiate( self ):
    ''' tests that we can instantiate one object of the tested class
    '''

    module = self.testClass()
    self.assertEqual( 'GGUSTicketsPolicy', module.__class__.__name__ )

  def test_evaluate( self ):
    ''' tests the method _evaluate
    '''

    module = self.testClass()

    res = module._evaluate( { 'OK' : False, 'Message' : 'Bo!' } )
    self.assertTrue(res['OK'])
    self.assertEquals( 'Error', res[ 'Value' ][ 'Status' ] )
    self.assertEquals( 'Bo!', res[ 'Value' ][ 'Reason' ] )

    res = module._evaluate( { 'OK' : True, 'Value' : None } )
    self.assertTrue(res['OK'])
    self.assertEquals( 'Unknown', res[ 'Value' ][ 'Status' ] )
    self.assertEquals( 'No values to take a decision', res[ 'Value' ][ 'Reason' ] )

    res = module._evaluate( { 'OK' : True, 'Value' : [] } )
    self.assertTrue(res['OK'])
    self.assertEquals( 'Unknown', res[ 'Value' ][ 'Status' ] )
    self.assertEquals( 'No values to take a decision', res[ 'Value' ][ 'Reason' ] )

    res = module._evaluate( { 'OK' : True, 'Value' : [{ 'A' : 1 }] } )
    self.assertTrue(res['OK'])
    self.assertEquals( 'Error', res[ 'Value' ][ 'Status' ] )
    self.assertEquals( 'Expected OpenTickets key for GGUSTickets', res[ 'Value' ][ 'Reason' ] )

    res = module._evaluate( { 'OK' : True, 'Value' : [{ 'OpenTickets' : 0 }] } )
    self.assertTrue(res['OK'])
    self.assertEquals( 'Active', res[ 'Value' ][ 'Status' ] )
    self.assertEquals( 'NO GGUSTickets unsolved', res[ 'Value' ][ 'Reason' ] )

    self.assertRaises( KeyError, module._evaluate, { 'OK' : True, 'Value' : [{ 'OpenTickets' : 1 }] } )

    res = module._evaluate( { 'OK' : True, 'Value' : [{ 'OpenTickets' : 1, 'Tickets' : '1a' }] } )
    self.assertTrue(res['OK'])
    self.assertEquals( 'Degraded', res[ 'Value' ][ 'Status' ] )
    self.assertEquals( '1 GGUSTickets unsolved: 1a', res[ 'Value' ][ 'Reason' ] )


if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( GGUSTicketsPolicy_TestCase )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( GGUSTicketsPolicy_Success ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
