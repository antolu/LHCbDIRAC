''' Test_RSS_Policy_JobEfficiencyPolicy
'''

import unittest

import LHCbDIRAC.ResourceStatusSystem.Policy.JobWebSummaryEfficiencyPolicy as moduleTested

################################################################################

class JobWebSummaryEfficiencyPolicy_TestCase( unittest.TestCase ):

  def setUp( self ):
    '''
    Setup
    '''
    self.moduleTested = moduleTested
    self.testClass    = self.moduleTested.JobWebSummaryEfficiencyPolicy

  def tearDown( self ):
    '''
    Tear down
    '''

    del self.moduleTested
    del self.testClass


################################################################################

class JobWebSummaryEfficiencyPolicy_Success( JobWebSummaryEfficiencyPolicy_TestCase ):

  def test_instantiate( self ):
    ''' tests that we can instantiate one object of the tested class
    '''

    module = self.testClass()
    self.assertEqual( 'JobWebSummaryEfficiencyPolicy', module.__class__.__name__ )

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
    self.assertEquals( '"Status" key missing', res[ 'Value' ][ 'Reason' ] )

    res = module._evaluate( { 'OK' : True, 'Value' : [{ 'Status' : 1 }] } )
    self.assertTrue(res['OK'])
    self.assertEquals( 'Error', res[ 'Value' ][ 'Status' ] )
    self.assertEquals( '"Efficiency" key missing', res[ 'Value' ][ 'Reason' ] )

    res = module._evaluate( { 'OK' : True, 'Value' : [{ 'Status' : 1, 'Efficiency' : 1 }] } )
    self.assertTrue(res['OK'])
    self.assertEquals( 'Error', res[ 'Value' ][ 'Status' ] )
    self.assertEquals( 'Unknown status "1"', res[ 'Value' ][ 'Reason' ] )

    res = module._evaluate( { 'OK' : True, 'Value' : [{ 'Status' : 'Good', 'Efficiency' : 1 }] } )
    self.assertTrue(res['OK'])
    self.assertEquals( 'Active', res[ 'Value' ][ 'Status' ] )
    self.assertEquals( 'Jobs Efficiency: 1 with status Good', res[ 'Value' ][ 'Reason' ] )

    res = module._evaluate( { 'OK' : True, 'Value' : [{ 'Status' : 'Fair', 'Efficiency' : 1 }] } )
    self.assertTrue(res['OK'])
    self.assertEquals( 'Active', res[ 'Value' ][ 'Status' ] )
    self.assertEquals( 'Jobs Efficiency: 1 with status Fair', res[ 'Value' ][ 'Reason' ] )

    res = module._evaluate( { 'OK' : True, 'Value' : [{ 'Status' : 'Poor', 'Efficiency' : 1 }] } )
    self.assertTrue(res['OK'])
    self.assertEquals( 'Degraded', res[ 'Value' ][ 'Status' ] )
    self.assertEquals( 'Jobs Efficiency: 1 with status Poor', res[ 'Value' ][ 'Reason' ] )

    res = module._evaluate( { 'OK' : True, 'Value' : [{ 'Status' : 'Idle', 'Efficiency' : 1 }] } )
    self.assertTrue(res['OK'])
    self.assertEquals( 'Unknown', res[ 'Value' ][ 'Status' ] )
    self.assertEquals( 'Jobs Efficiency: 1 with status Idle', res[ 'Value' ][ 'Reason' ] )

    res = module._evaluate( { 'OK' : True, 'Value' : [{ 'Status' : 'Bad', 'Efficiency' : 1 }] } )
    self.assertTrue(res['OK'])
    self.assertEquals( 'Banned', res[ 'Value' ][ 'Status' ] )
    self.assertEquals( 'Jobs Efficiency: 1 with status Bad', res[ 'Value' ][ 'Reason' ] )


if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( JobWebSummaryEfficiencyPolicy_TestCase )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( JobWebSummaryEfficiencyPolicy_Success ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )


################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
