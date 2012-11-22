import unittest, os, shutil
from DIRAC import gLogger
from DIRAC.Core.Base.Script import parseCommandLine

from LHCbDIRAC.Interfaces.API.LHCbJob import LHCbJob
from LHCbDIRAC.Interfaces.API.DiracLHCb import DiracLHCb
from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient


class RegressionTestCase( unittest.TestCase ):
  ''' Base class for the Regression test cases
  '''
  def setUp( self ):

    parseCommandLine()

    gLogger.setLevel( 'DEBUG' )
    self.diracLHCb = DiracLHCb()
    self.bkkClient = BookkeepingClient()

    self.j_u_hello = LHCbJob( 'helloWorld.xml' )

  def tearDown( self ):
    for fileIn in os.listdir( '.' ):
      if 'Local' in fileIn:
        shutil.rmtree( fileIn )
      for fileToRemove in ['std.out', 'std.err']:
        try:
          os.remove( fileToRemove )
        except OSError:
          continue


class HelloWorldSuccess( RegressionTestCase ):
  def test_execute( self ):
    res = self.j_mc_20160.runLocal( self.diracLHCb, self.bkkClient )
    self.assertTrue( res['OK'] )


#############################################################################
# Test Suite run
#############################################################################

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( RegressionTestCase )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( HelloWorldSuccess ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
