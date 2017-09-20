''' Test_RSS_Policy_Configurations
'''

import unittest

import LHCbDIRAC.ResourceStatusSystem.Policy.Configurations as moduleTested

from types import NoneType

__RCSID__ = "$Id$"

################################################################################

class Configurations_TestCase( unittest.TestCase ):

  def setUp( self ):
    '''
    Setup
    '''

    self.moduleTested = moduleTested

  def tearDown( self ):
    '''
    Tear down
    '''

    del self.moduleTested

################################################################################

class Configurations_Success( Configurations_TestCase ):

  def test_PoliciesMetaLHCb( self ):

    policies = self.moduleTested.POLICIESMETA_LHCB

    policyKeys = set( [ 'description', 'module', 'command', 'args' ] )

    for policyName, policy in policies.items():
      print policyName
      self.assertEqual( policyKeys, set( policy.keys() ) )
      self.assertTrue( isinstance( policy[ 'description' ], str ) )
      self.assertTrue( isinstance( policy[ 'module' ], str ) )
      self.assertTrue( isinstance( policy[ 'command' ], (tuple, None) ) )
      self.assertTrue( isinstance( policy[ 'args' ], (dict, None) ) )

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( Configurations_TestCase )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( Configurations_Success ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
