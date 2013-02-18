from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from TestLHCbDIRAC.Regression.utils import cleanTestDir

import unittest, os
from DIRAC import gLogger

from LHCbDIRAC.Interfaces.API.LHCbJob import LHCbJob
from LHCbDIRAC.Interfaces.API.DiracLHCb import DiracLHCb
from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient


class RegressionTestCase( unittest.TestCase ):
  ''' Base class for the Regression test cases
  '''
  def setUp( self ):
    cleanTestDir()

    gLogger.setLevel( 'DEBUG' )
    self.diracLHCb = DiracLHCb()
    self.bkkClient = BookkeepingClient()

    self.j_mc_20160 = LHCbJob( '20160.xml' )
    self.j_reco_20194 = LHCbJob( '20194.xml' )
    self.j_stripp_20349 = LHCbJob( '20349.xml' )
    self.j_merge_20752 = LHCbJob( '20752.xml' )
    self.j_merge_21211 = LHCbJob( '21211.xml' )
    self.j_mergeMDF_20657 = LHCbJob( '20657.xml' )

  def tearDown( self ):
    cleanTestDir()

class MCSuccess( RegressionTestCase ):
  def test_execute( self ):
    res = self.j_mc_20160.runLocal( self.diracLHCb, self.bkkClient )
    self.assertTrue( res['OK'] )

    self.assertEqual( pConfBoole, pConfBooleExpected )
    self.assertEqual( pConfMoore, pConfMooreExpected )
    self.assertEqual( pConfBrunel, pConfBrunelExpected )
    self.assertEqual( pConfDaVinci, pConfDaVinciExpected )

class RecoSuccess( RegressionTestCase ):
  def test_execute( self ):
    res = self.j_reco_20194.runLocal( self.diracLHCb, self.bkkClient )
    self.assertTrue( res['OK'] )

    # Now checking for some outputs
    # prodConf files
    for fileIn in os.listdir( '.' ):
      if 'Local_' in fileIn:
        fd = os.open( './' + fileIn + '/prodConf_Brunel_00012345_00006789_1.py' )
        pConfBrunel = fd.read()
        fd = os.open( './' + fileIn + '/prodConf_DaVinci_00012345_00006789_2.py' )
        pConfDaVinci = fd.read()

    pConfBrunelExpected = ( open( 'pConfBrunelRecoExpected.txt' ) ).read()
    pConfDaVinciExpected = ( open( 'pConfDaVinciRecoExpected.txt' ) ).read()

    self.assertEqual( pConfBrunel, pConfBrunelExpected )
    self.assertEqual( pConfDaVinci, pConfDaVinciExpected )

class StrippSuccess( RegressionTestCase ):
  def test_execute( self ):
    res = self.j_stripp_20349.runLocal( self.diracLHCb, self.bkkClient )
    self.assertTrue( res['OK'] )

    # Now checking for some outputs
    # prodConf files
    for fileIn in os.listdir( '.' ):
      if 'Local_' in fileIn:
        fd = os.open( './' + fileIn + '/prodConf_DaVinci_00012345_00006789_1.py' )
        pConfDaVinci = fd.read()

    pConfDaVinciExpected = ( open( 'pConfDaVinciStrippExpected.txt' ) ).read()

    self.assertEqual( pConfDaVinci, pConfDaVinciExpected )

class MergeSuccess( RegressionTestCase ):
  def test_execute( self ):
    res = self.j_merge_20752.runLocal( self.diracLHCb, self.bkkClient )
    self.assertTrue( res['OK'] )

    # Now checking for some outputs
    # prodConf files
    for fileIn in os.listdir( '.' ):
      if 'Local_' in fileIn:
        fd = os.open( './' + fileIn + '/prodConf_LHCb_00012345_00006789_1.py' )
        pConfLHCb = fd.read()

    pConfLHCbExpected = ( open( 'pConfLHCbExpected.txt' ) ).read()

    self.assertEqual( pConfLHCb, pConfLHCbExpected )

class MergeMultStreamsSuccess( RegressionTestCase ):
  def test_execute( self ):
    res = self.j_merge_21211.runLocal( self.diracLHCb, self.bkkClient )
    self.assertTrue( res['OK'] )

    # Now checking for some outputs
    # prodConf files
    for fileIn in os.listdir( '.' ):
      if 'Local_' in fileIn:
        fd = os.open( './' + fileIn + '/prodConf_DaVinci_00012345_00006789_1.py' )
        pConfDaVinci = fd.read()

    pConfDaVinciExpected = ( open( 'pConfDaVinciMergeExpected.txt' ) ).read()

    self.assertEqual( pConfDaVinci, pConfDaVinciExpected )

class MergeMDFSuccess( RegressionTestCase ):
  def test_execute( self ):
    res = self.j_mergeMDF_20657.runLocal( self.diracLHCb, self.bkkClient )
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
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( MergeMultStreamsSuccess ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( MergeMDFSuccess ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
