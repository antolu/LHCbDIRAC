''' Test_RSS_Client_ResourceManagementClient
'''

#pylint: disable=protected-access, missing-docstring, invalid-name, line-too-long

import unittest
import mock

from DIRAC import gLogger
import LHCbDIRAC.ResourceStatusSystem.Client.ResourceManagementClient as moduleTested

__RCSID__ = "$Id$"

################################################################################

class ResourceManagementClient_TestCase( unittest.TestCase ):

  def setUp( self ):
    '''
    Setup
    '''
    gLogger.setLevel("DEBUG")
    self.moduleTested = moduleTested
    self.testClass = self.moduleTested.ResourceManagementClient

  def tearDown( self ):
    '''
    Tear down
    '''

    del self.moduleTested
    del self.testClass

################################################################################

class ResourceManagementClient_Success( ResourceManagementClient_TestCase ):

  def test_instantiate( self ):
    ''' tests that we can instantiate one object of the tested class
    '''

    module = self.testClass()
    self.assertEqual( 'ResourceManagementClient', module.__class__.__name__ )

  def test_init( self ):
    ''' test the __init__ method
    '''

    module = self.testClass()

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
