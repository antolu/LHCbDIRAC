#!/usr/bin/env python
from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

import unittest, os, shutil

from DIRAC.tests.Utilities.utils import find_all

from LHCbDIRAC.tests.Utilities.utils import getOutput
from LHCbDIRAC.tests.Utilities.IntegrationTest import IntegrationTest

from LHCbDIRAC.Interfaces.API.LHCbJob import LHCbJob
from LHCbDIRAC.Interfaces.API.DiracLHCb import DiracLHCb
from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient


class RegressionTestCase( IntegrationTest ):
  """ Base class for the Regression test cases
  """
  def setUp( self ):
    super( IntegrationTest, self ).setUp()

    self.diracLHCb = DiracLHCb()
    self.bkkClient = BookkeepingClient()

    location40651 = find_all( '40651.xml', '.', 'Regression' )[0]
    self.j_mc_40651 = LHCbJob( location40651 )
    self.j_mc_40651.setConfigArgs( 'pilot.cfg' )

    location40652 = find_all( '40652.xml', '.', 'Regression' )[0]
    self.j_mc_40652 = LHCbJob( location40652 )
    self.j_mc_40652.setConfigArgs( 'pilot.cfg' )

#     location20194 = find_all( '20194.xml', '.', 'Regression' )[0]
#     self.j_reco_20194 = LHCbJob( location20194 )
#     self.j_reco_20194.setConfigArgs( 'pilot.cfg' )

    # Reco from Collision15em
    location46146 = find_all( '46146.xml', '.', 'Regression' )[0]
    self.j_reco_46146 = LHCbJob( location46146 )
    self.j_reco_46146.setConfigArgs( 'pilot.cfg' )

#     location31017 = find_all( '31017.xml', '.', 'Regression' )[0]
#     self.j_stripp_31017 = LHCbJob( location31017 )
#     self.j_stripp_31017.setConfigArgs( 'pilot.cfg' )

    # Turbo Stripping Collision15em
    location46403 = find_all( '46403.xml', '.', 'Regression' )[0]
    self.j_stripp_46403 = LHCbJob( location46403 )
    self.j_stripp_46403.setConfigArgs( 'pilot.cfg' )

    location40464 = find_all( '40464.xml', '.', 'Regression' )[0]
    self.j_merge_40464 = LHCbJob( location40464 )
    self.j_merge_40464.setConfigArgs( 'pilot.cfg' )

    location21211 = find_all( '21211.xml', '.', 'Regression' )[0]
    self.j_merge_21211 = LHCbJob( location21211 )
    self.j_merge_21211.setConfigArgs( 'pilot.cfg' )

    location20657 = find_all( '20657.xml', '.', 'Regression' )[0]
    self.j_mergeMDF_20657 = LHCbJob( location20657 )
    self.j_mergeMDF_20657.setConfigArgs( 'pilot.cfg' )

    location31057 = find_all( '31057.xml', '.', 'Regression' )[0]
    self.j_swimming_31057 = LHCbJob( location31057 )
    self.j_swimming_31057.setConfigArgs( 'pilot.cfg' )

#   def tearDown( self ):
#     pass

class MCSuccess( RegressionTestCase ):
  def test_Regression_Production( self ):
    res = self.j_mc_40651.runLocal( self.diracLHCb, self.bkkClient )
    self.assertTrue( res['OK'] )

#     for found, expected in getOutput( 'MC' ):
#       self.assertEqual( found, expected )

class MCReconstructionSuccess( RegressionTestCase ):
  def test_Regression_Production( self ):
    res = self.j_mc_40652.runLocal( self.diracLHCb, self.bkkClient )
    self.assertTrue( res['OK'] )

#     for found, expected in getOutput( 'MC' ):
#       self.assertEqual( found, expected )

class RecoSuccess( RegressionTestCase ):
  def test_Regression_Production( self ):
#     res = self.j_reco_20194.runLocal( self.diracLHCb, self.bkkClient )
    res = self.j_reco_46146.runLocal( self.diracLHCb, self.bkkClient )
    self.assertTrue( res['OK'] )

#     for found, expected in getOutput( 'Reco' ):
#       self.assertEqual( found, expected )

class StrippSuccess( RegressionTestCase ):
  def test_Regression_Production( self ):
    res = self.j_stripp_46403.runLocal( self.diracLHCb, self.bkkClient )
    self.assertTrue( res['OK'] )

#     for found, expected in getOutput( 'Stripp' ):
#       self.assertEqual( found, expected )

class MergeSuccess( RegressionTestCase ):
  def test_Regression_Production( self ):
    res = self.j_merge_40464.runLocal( self.diracLHCb, self.bkkClient )
    self.assertTrue( res['OK'] )

#     for found, expected in getOutput( 'Merge' ):
#       self.assertEqual( found, expected )

# FIXME: to update
class MergeMultStreamsSuccess( RegressionTestCase ):
  def test_Regression_Production( self ):
    res = self.j_merge_21211.runLocal( self.diracLHCb, self.bkkClient )
    self.assertTrue( res['OK'] )

#     for found, expected in getOutput( 'MergeM' ):
#       self.assertEqual( found, expected )

class MergeMDFSuccess( RegressionTestCase ):
  def test_Regression_Production( self ):
    res = self.j_mergeMDF_20657.runLocal( self.diracLHCb, self.bkkClient )
    self.assertTrue( res['OK'] )

# FIXME: to update
class SwimmingSuccess( RegressionTestCase ):
  def test_Regression_Production( self ):
    res = self.j_swimming_31057.runLocal( self.diracLHCb, self.bkkClient )
    self.assertTrue( res['OK'] )


#############################################################################
# Test Suite run
#############################################################################

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( RegressionTestCase )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( MCSuccess ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( MCReconstructionSuccess ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( RecoSuccess ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( StrippSuccess ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( MergeSuccess ) )
#  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( MergeMultStreamsSuccess ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( MergeMDFSuccess ) )
#   suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( SwimmingSuccess ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
