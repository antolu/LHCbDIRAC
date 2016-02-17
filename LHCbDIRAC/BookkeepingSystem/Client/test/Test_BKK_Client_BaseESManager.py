''' Test_BKK_Client_BaseESManager

'''

import unittest
import LHCbDIRAC.BookkeepingSystem.Client.BaseESManager as moduleTested

__RCSID__ = "$Id$"

################################################################################

class BaseESManager_TestCase( unittest.TestCase ):

  def setUp( self ):
    '''
    Setup
    '''

    self.testClass = moduleTested.BaseESManager

  def tearDown( self ):
    '''
    TearDown
    '''
    
    del self.testClass

################################################################################

class BaseESManager_Success( BaseESManager_TestCase ):

  def test_instantiate( self ):
    ''' tests that we can instantiate one object of the tested class
    '''
    client = self.testClass()
    self.assertEqual( 'BaseESManager', client.__class__.__name__ )

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF