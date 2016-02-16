''' Test_CS_Agent_DummyTest

'''

import unittest

__RCSID__ = '$Id: $'

class DummyTest_TestCase( unittest.TestCase ): pass

class Dummy_Success( DummyTest_TestCase ):
  
  def test_dummy( self ):
    self.assertEqual( True, True )