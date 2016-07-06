""" Tests of ProductionLogAnalysis
"""

import os
import unittest

from LHCbDIRAC.Core.Utilities.ProductionLogs import analyseLogFile, LogError

testsDir = 'LHCbDIRAC/tests/Integration/ProductionXMLLogAnalysis'

class ProductionLogAnalysisTestCase( unittest.TestCase ):
  """ Base class for the ProductionLogAnalysis test cases
  """

  def generalTest( self, testPath, directory, app ):
    """ Args:
	  testPath (str): like "DataReconstruction" (used for creating the path)
	  directory (str): either "ok" or "nok" (used for creating the path)
	  app (str): LHCb app, e.g. "Brunel" (used for creating the file name)
    """

    workPath = os.path.join( os.path.expandvars('$TESTCODE'), testsDir, testPath, directory )

    ls = os.listdir( workPath )

    for fileName in ls:
      if fileName.startswith( app ) and 'log' in fileName:
        if directory == 'ok':
	  res = analyseLogFile( '%s/%s' % ( workPath, fileName ) )
          self.assertEqual( res, True )
        elif directory == 'nok':
	  res = analyseLogFile( '%s/%s' % ( workPath, fileName ) )
          self.assertEqual( res, False )
        else:
	  self.assertRaises( LogError, analyseLogFile, '%s/%s' % ( workPath, fileName ) )


class ProductionLogAnalysisDataReconstruction( ProductionLogAnalysisTestCase ):

  def setUp( self ):
    super( ProductionLogAnalysisDataReconstruction, self ).setUp()

  def test_brunel_ok( self ):
    self.generalTest( 'DataReconstruction', 'ok', 'Brunel' )
  def test_daVinci_ok( self ):
    self.generalTest( 'DataReconstruction', 'ok', 'DaVinci' )

  def test_brunel_nok( self ):
    self.generalTest( 'DataReconstruction', 'nok', 'Brunel' )
#  def test_daVinci_nok( self ):
#    self.generalTest( self.workdir, 'nok', 'DaVinci' )

class ProductionLogAnalysisDataReprocessing( ProductionLogAnalysisTestCase ):

  def setUp( self ):
    super( ProductionLogAnalysisDataReprocessing, self ).setUp()

  def test_brunel_ok( self ):
    self.generalTest( 'DataReprocessing', 'ok', 'Brunel' )
  def test_daVinci_ok( self ):
    self.generalTest( 'DataReprocessing', 'ok', 'DaVinci' )

  def test_brunel_nok( self ):
    self.generalTest( 'DataReprocessing', 'nok', 'Brunel' )
#  def test_daVinci_nok( self ):
#    self.generalTest( self.workdir, 'nok', 'DaVinci' )

class ProductionLogAnalysisDataStripping( ProductionLogAnalysisTestCase ):

  def setUp( self ):
    super( ProductionLogAnalysisDataStripping, self ).setUp()

  def test_daVinci_ok( self ):
    self.generalTest( 'DataStripping', 'ok', 'DaVinci' )

  def test_daVinci_nok( self ):
    self.generalTest( 'DataStripping', 'nok', 'DaVinci' )

class ProductionLogAnalysisSelection( ProductionLogAnalysisTestCase ):

  def setUp( self ):
    super( ProductionLogAnalysisSelection, self ).setUp()

  def test_daVinci_ok( self ):
    self.generalTest( 'Selection', 'ok', 'DaVinci' )

  def test_daVinci_nok( self ):
    self.generalTest( 'Selection', 'nok', 'DaVinci' )

class ProductionLogAnalysisMCSimulation( ProductionLogAnalysisTestCase ):

  def setUp( self ):
    super( ProductionLogAnalysisMCSimulation, self ).setUp()

  def test_brunel_ok( self ):
    self.generalTest( 'MCSimulation', 'ok', 'Brunel' )
  def test_boole_ok( self ):
    self.generalTest( 'MCSimulation', 'ok', 'Boole' )
  def test_gauss_ok( self ):
    self.generalTest( 'MCSimulation', 'ok', 'Gauss' )
#  def test_daVinci_ok( self ):
#    self.generalTest( '', 'ok', 'DaVinci' )

  # def test_brunel_nok( self ):
  #   self.generalTest( 'MCSimulation', 'nok', 'Brunel' )
  #
  # def test_brunel_fail( self ):
  #   self.generalTest( 'MCSimulation', 'fail', 'Brunel' )

class ProductionLogAnalysisMerge( ProductionLogAnalysisTestCase ):

  def setUp( self ):
    super( ProductionLogAnalysisMerge, self ).setUp()

#  def test_brunel_ok( self ):
#    self.generalTest( '', 'ok', 'Brunel' )
#  def test_boole_ok( self ):
#    self.generalTest( '', 'ok', 'Boole' )
#  def test_gauss_ok( self ):
#    self.generalTest( '', 'ok', 'Gauss' )
  def test_daVinci_ok( self ):
    self.generalTest( 'Merge', 'ok', 'DaVinci' )
  def test_lhcb_ok( self ):
    self.generalTest( 'Merge', 'ok', 'LHCb' )

  def test_lhcb_nok( self ):
    self.generalTest( 'Merge', 'nok', 'LHCb' )

class ProductionXMLLogAnalysisRemoval( ProductionLogAnalysisTestCase ):
  pass

class ProductionXMLLogAnalysisReplication( ProductionLogAnalysisTestCase ):
  pass

################################################################################

def run():
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( ProductionLogAnalysisTestCase )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( ProductionLogAnalysisDataReconstruction ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( ProductionLogAnalysisDataReprocessing ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( ProductionLogAnalysisDataStripping ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( ProductionLogAnalysisSelection ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( ProductionLogAnalysisMCSimulation ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( ProductionLogAnalysisMerge ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
  return testResult

################################################################################

if __name__ == '__main__':
  run()

################################################################################
# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
