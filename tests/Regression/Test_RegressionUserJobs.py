from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

import unittest

from LHCbTestDirac.Utilities.IntegrationTest import IntegrationTest

from LHCbDIRAC.Interfaces.API.LHCbJob import LHCbJob
from LHCbDIRAC.Interfaces.API.DiracLHCb import DiracLHCb
from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient


class RegressionTestCase( IntegrationTest ):
  ''' Base class for the Regression test cases
  '''
  def setUp( self ):
    super( IntegrationTest, self ).setUp()

    self.diracLHCb = DiracLHCb()
    self.bkkClient = BookkeepingClient()

    self.j_u_hello = LHCbJob( 'helloWorld.xml' )
    self.j_u_helloPlus = LHCbJob( 'helloWorld-FewMoreStuff.xml' )
#    self.j_u_collision12 = LHCbJob( 'collision12.xml' )
#    self.j_u_rootMerger = LHCbJob( 'rootMerger.xml' )

class HelloWorldSuccess( RegressionTestCase ):
  def test_execute( self ):
    res = self.j_u_hello.runLocal( self.diracLHCb, self.bkkClient )
    self.assertTrue( res['OK'] )

class HelloWorldPlusSuccess( RegressionTestCase ):
  def test_execute( self ):
    res = self.j_u_helloPlus.runLocal( self.diracLHCb, self.bkkClient )
    self.assertTrue( res['OK'] )

# class Collision12Success( RegressionTestCase ):
#  def test_execute( self ):
#    res = self.j_u_collision12.runLocal( self.diracLHCb, self.bkkClient )
#    self.assertTrue( res['OK'] )

# class RootMergerSuccess( RegressionTestCase ):
#  def test_execute( self ):
#    res = self.j_u_rootMerger.runLocal( self.diracLHCb, self.bkkClient )
#    self.assertTrue( res['OK'] )


#############################################################################
# Test Suite run
#############################################################################

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( RegressionTestCase )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( HelloWorldSuccess ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( HelloWorldPlusSuccess ) )
#  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( Collision12Success ) )
#  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( RootMergerSuccess ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
