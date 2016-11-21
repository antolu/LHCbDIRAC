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
      self.assertEqual( True, isinstance( policy[ 'description' ], str ) )
      self.assertEqual( True, isinstance( policy[ 'module' ], str ) )
      self.assertEqual( True, type( policy[ 'command' ] ) in [ tuple, NoneType ] )
      self.assertEqual( True, type( policy[ 'args' ] ) in [ dict, NoneType ] )

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
