#!/usr/bin/env python
from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from LHCbTestDirac.Utilities.utils import getOutput
from LHCbTestDirac.Utilities.IntegrationTest import IntegrationTest

import unittest

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

    exeScriptLoc = find_all( 'exe-script.py', '.', 'Regression' )[0]
    helloWorldLoc = find_all( 'helloWorld.py', '.', 'Regression' )[0]

    shutil.copyfile( exeScriptLoc, './exe-script.py' )
    shutil.copyfile( helloWorldLoc, './helloWorld.py' )

#    self.j_mc_20160 = LHCbJob( '20160.xml' )
    location31202 = find_all( '31202.xml', '.', 'Regression' )[0]
    self.j_mc_31202 = LHCbJob( location31202 )
    self.j_mc_31202.setConfigArgs( 'pilot.cfg' )

    location20194 = find_all( '20194.xml', '.', 'Regression' )[0]
    self.j_reco_20194 = LHCbJob()
    self.j_reco_20194.setConfigArgs( 'pilot.cfg' )

    location20194old = find_all( '20194_old.xml', '.', 'Regression' )[0]
    self.j_reco_20194_old = LHCbJob( location20194old )
    self.j_reco_20194_old.setConfigArgs( 'pilot.cfg' )

#    self.j_stripp_20349 = LHCbJob( '20349.xml' )
    location31017 = find_all( '31017.xml', '.', 'Regression' )[0]
    self.j_stripp_31017 = LHCbJob( location31017 )
    self.j_stripp_31017.setConfigArgs( 'pilot.cfg' )

    location20752 = find_all( '20752.xml', '.', 'Regression' )[0]
    self.j_merge_20752 = LHCbJob( location20752 )
    self.j_merge_20752.setConfigArgs( 'pilot.cfg' )

    location21211 = find_all( '21211.xml', '.', 'Regression' )[0]
    self.j_merge_21211 = LHCbJob( location21211 )
    self.j_merge_21211.setConfigArgs( 'pilot.cfg' )

    location20657 = find_all( '20657.xml', '.', 'Regression' )[0]
    self.j_mergeMDF_20657 = LHCbJob( location20657 )
    self.j_mergeMDF_20657.setConfigArgs( 'pilot.cfg' )

    location31057 = find_all( '31057.xml', '.', 'Regression' )[0]
    self.j_swimming_31057 = LHCbJob( location31057 )
    self.j_swimming_31057.setConfigArgs( 'pilot.cfg' )

class MCSuccess( RegressionTestCase ):
  def test_execute( self ):
#    res = self.j_mc_20160.runLocal( self.diracLHCb, self.bkkClient )
    res = self.j_mc_31202.runLocal( self.diracLHCb, self.bkkClient )
    self.assertTrue( res['OK'] )

    for found, expected in getOutput( 'MC' ):
      self.assertEqual( found, expected )

class RecoSuccess( RegressionTestCase ):
  def test_execute( self ):
    res = self.j_reco_20194.runLocal( self.diracLHCb, self.bkkClient )
    self.assertTrue( res['OK'] )

    for found, expected in getOutput( 'Reco' ):
      self.assertEqual( found, expected )

class RecoOldSuccess( RegressionTestCase ):
  def test_execute( self ):
    res = self.j_reco_20194_old.runLocal( self.diracLHCb, self.bkkClient )
    self.assertTrue( res['OK'] )

    for found, expected in getOutput( 'Reco_old' ):
      self.assertEqual( found, expected )

class StrippSuccess( RegressionTestCase ):
  def test_execute( self ):
#    res = self.j_stripp_20349.runLocal( self.diracLHCb, self.bkkClient )
    res = self.j_stripp_31017.runLocal( self.diracLHCb, self.bkkClient )
    self.assertTrue( res['OK'] )

    for found, expected in getOutput( 'Stripp' ):
      self.assertEqual( found, expected )

class MergeSuccess( RegressionTestCase ):
  def test_execute( self ):
    res = self.j_merge_20752.runLocal( self.diracLHCb, self.bkkClient )
    self.assertTrue( res['OK'] )

    for found, expected in getOutput( 'Merge' ):
      self.assertEqual( found, expected )

class MergeMultStreamsSuccess( RegressionTestCase ):
  def test_execute( self ):
    res = self.j_merge_21211.runLocal( self.diracLHCb, self.bkkClient )
    self.assertTrue( res['OK'] )

    for found, expected in getOutput( 'MergeM' ):
      self.assertEqual( found, expected )

class MergeMDFSuccess( RegressionTestCase ):
  def test_execute( self ):
    res = self.j_mergeMDF_20657.runLocal( self.diracLHCb, self.bkkClient )
    self.assertTrue( res['OK'] )

class SwimmingSuccess( RegressionTestCase ):
  def test_execute( self ):
    res = self.j_swimming_31057.runLocal( self.diracLHCb, self.bkkClient )
    self.assertTrue( res['OK'] )


#############################################################################
# Test Suite run
#############################################################################

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( RegressionTestCase )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( MCSuccess ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( RecoSuccess ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( StrippSuccess ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( RecoOldSuccess ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( MergeSuccess ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( MergeMultStreamsSuccess ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( MergeMDFSuccess ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( SwimmingSuccess ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
