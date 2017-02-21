#!/usr/bin/env python

""" Regression production jobs are "real" XMLs of production jobs that ran in production
"""

#pylint: disable=missing-docstring,invalid-name,wrong-import-position

import unittest

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from DIRAC.tests.Utilities.utils import find_all

from tests.Utilities.IntegrationTest import IntegrationTest

from LHCbDIRAC.Interfaces.API.LHCbJob import LHCbJob
from LHCbDIRAC.Interfaces.API.DiracLHCb import DiracLHCb
from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient


class RegressionTestCase( IntegrationTest ):
  """ Base class for the Regression test cases
  """
  def setUp( self ):
    super( RegressionTestCase, self ).setUp()

    self.diracLHCb = DiracLHCb()
    self.bkkClient = BookkeepingClient()

    print "\n****************************************** Start running test"

#     location31017 = find_all( '31017.xml', '.', 'Regression' )[0]
#     self.j_stripp_31017 = LHCbJob( location31017 )
#     self.j_stripp_31017.setConfigArgs( 'pilot.cfg' )

#   def tearDown( self ):
#     pass

class MCSuccess( RegressionTestCase ):
  def test_Regression_Production( self ):

    location40651 = find_all( '40651.xml', '..', 'Regression' )[0]
    j_mc_40651 = LHCbJob( location40651 )
    j_mc_40651.setConfigArgs( 'pilot.cfg' )

    res = j_mc_40651.runLocal( self.diracLHCb, self.bkkClient )
    self.assertTrue( res['OK'] )

#     for found, expected in getOutput( 'MC' ):
#       self.assertEqual( found, expected )

class MCReconstructionSuccess( RegressionTestCase ):
  def test_Regression_Production( self ):
    location40652 = find_all( '40652.xml', '..', 'Regression' )[0]
    j_mc_40652 = LHCbJob( location40652 )
    j_mc_40652.setConfigArgs( 'pilot.cfg' )

    res = j_mc_40652.runLocal( self.diracLHCb, self.bkkClient )
    self.assertTrue( res['OK'] )

#     for found, expected in getOutput( 'MC' ):
#       self.assertEqual( found, expected )

class RecoSuccess( RegressionTestCase ):
  def test_Regression_Production( self ):
    # Reco from Collision15em
    location46146 = find_all( '46146.xml', '..', 'Regression' )[0]
    j_reco_46146 = LHCbJob( location46146 )
    j_reco_46146.setConfigArgs( 'pilot.cfg' )

    res = j_reco_46146.runLocal( self.diracLHCb, self.bkkClient )
    self.assertTrue( res['OK'] )

#     for found, expected in getOutput( 'Reco' ):
#       self.assertEqual( found, expected )

class StrippSuccess( RegressionTestCase ):
  def test_Regression_Production( self ):
    # Turbo Stripping Collision15em
    location46403 = find_all( '46403.xml', '..', 'Regression' )[0]
    j_stripp_46403 = LHCbJob( location46403 )
    j_stripp_46403.setConfigArgs( 'pilot.cfg' )

    res = j_stripp_46403.runLocal( self.diracLHCb, self.bkkClient )
    self.assertTrue( res['OK'] )

#     for found, expected in getOutput( 'Stripp' ):
#       self.assertEqual( found, expected )

class MCMergeSuccess( RegressionTestCase ):
  def test_Regression_Production( self ):
    location51753 = find_all( '51753.xml', '..', 'Regression' )[0]
    j_MCmerge_51753 = LHCbJob( location51753 )
    j_MCmerge_51753.setConfigArgs( 'pilot.cfg' )

    res = j_MCmerge_51753.runLocal( self.diracLHCb, self.bkkClient )
    self.assertTrue( res['OK'] )

#     for found, expected in getOutput( 'Merge' ):
#       self.assertEqual( found, expected )

# FIXME: to update
class MergeMultStreamsSuccess( RegressionTestCase ):
  def test_Regression_Production( self ):
    location21211 = find_all( '21211.xml', '..', 'Regression' )[0]
    j_merge_21211 = LHCbJob( location21211 )
    j_merge_21211.setConfigArgs( 'pilot.cfg' )

    res = j_merge_21211.runLocal( self.diracLHCb, self.bkkClient )
    self.assertTrue( res['OK'] )

#     for found, expected in getOutput( 'MergeM' ):
#       self.assertEqual( found, expected )

class MergeMDFSuccess( RegressionTestCase ):
  def test_Regression_Production( self ):
    location20657 = find_all( '20657.xml', '..', 'Regression' )[0]
    j_mergeMDF_20657 = LHCbJob( location20657 )
    j_mergeMDF_20657.setConfigArgs( 'pilot.cfg' )

    res = j_mergeMDF_20657.runLocal( self.diracLHCb, self.bkkClient )
    self.assertTrue( res['OK'] )

# FIXME: to update
class SwimmingSuccess( RegressionTestCase ):
  def test_Regression_Production( self ):
    location31057 = find_all( '31057.xml', '..', 'Regression' )[0]
    j_swimming_31057 = LHCbJob( location31057 )
    j_swimming_31057.setConfigArgs( 'pilot.cfg' )

    res = j_swimming_31057.runLocal( self.diracLHCb, self.bkkClient )
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
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( MCMergeSuccess ) )
#  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( MergeMultStreamsSuccess ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( MergeMDFSuccess ) )
#   suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( SwimmingSuccess ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
