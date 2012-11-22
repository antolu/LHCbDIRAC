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

    self.j_mc_20160 = LHCbJob( '20160.xml' )
    self.j_reco_20194 = LHCbJob( '20194.xml' )
    self.j_stripp_20349 = LHCbJob( '20349.xml' )
    self.j_merge_20752 = LHCbJob( '20752.xml' )


  def tearDown( self ):
    for fileIn in os.listdir( '.' ):
      if 'Local' in fileIn:
        shutil.rmtree( fileIn )
      for fileToRemove in ['std.out', 'std.err']:
        try:
          os.remove( fileToRemove )
        except OSError:
          continue


class MCSuccess( RegressionTestCase ):
  def test_execute( self ):
    res = self.j_mc_20160.runLocal( self.diracLHCb, self.bkkClient )
    self.assertTrue( res['OK'] )

class RecoSuccess( RegressionTestCase ):
  def test_execute( self ):
    res = self.j_reco_20194.runLocal( self.diracLHCb, self.bkkClient )
    self.assertTrue( res['OK'] )

class StrippSuccess( RegressionTestCase ):
  def test_execute( self ):
    res = self.j_stripp_20349.runLocal( self.diracLHCb, self.bkkClient )
    self.assertTrue( res['OK'] )

class MergeSuccess( RegressionTestCase ):
  def test_execute( self ):
    res = self.j_merge_20752.runLocal( self.diracLHCb, self.bkkClient )
    self.assertTrue( res['OK'] )


#############################################################################
# Test Suite run
#############################################################################

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( RegressionTestCase )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( MCSuccess ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( RecoSuccess ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( StrippSuccess ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( MergeSuccess ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
