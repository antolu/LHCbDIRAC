""" Test of XMLSummaries
"""

import os
import unittest

from LHCbDIRAC.Core.Utilities.XMLSummaries import analyseXMLSummary, XMLSummaryError

#pylint: disable=missing-docstring

testsDir = 'LHCbDIRAC/tests/Integration/ProductionXMLLogAnalysis'

class XMLSummaryAnalysisTestCase( unittest.TestCase ):
  """ Base class for the XMLSummaryAnalysis test cases
  """

  def generalTest( self, testPath, directory ):
    """ Args:
      testPath (str): like "DataReconstruction" (used for creating the path)
      directory (str): either "ok" or "nok" (used for creating the path)
    """

    workPath = os.path.join( os.path.expandvars('$TESTCODE'), testsDir, testPath, directory )

    ls = os.listdir( workPath )

    for fileName in ls:
      if fileName.startswith( 'summary' ):
        if directory == 'ok':
          res = analyseXMLSummary( '%s/%s' % ( workPath, fileName ) )
          self.assertEqual( res, True )
        elif directory == 'nok':
          res = analyseXMLSummary( '%s/%s' % ( workPath, fileName ) )
          self.assertEqual( res, False )
        else:
          self.assertRaises( XMLSummaryError, analyseXMLSummary, '%s/%s' % ( workPath, fileName ) )

class XMLSummaryAnalysisDataReconstruction( XMLSummaryAnalysisTestCase ):

  def setUp( self ):
    super( XMLSummaryAnalysisDataReconstruction, self ).setUp()

  def test_brunel_ok( self ):
    self.generalTest( 'DataReconstruction', 'ok' )
  def test_daVinci_ok( self ):
    self.generalTest( 'DataReconstruction', 'ok' )

  def test_brunel_nok( self ):
    self.generalTest( 'DataReconstruction', 'nok' )

  def test_brunel_fail( self ):
    self.generalTest( 'DataReconstruction', 'fail' )

class XMLSummaryAnalysisDataReprocessing( XMLSummaryAnalysisTestCase ):

  def setUp( self ):
    super( XMLSummaryAnalysisDataReprocessing, self ).setUp()

  def test_brunel_ok( self ):
    self.generalTest( 'DataReprocessing', 'ok' )
  def test_daVinci_ok( self ):
    self.generalTest( 'DataReprocessing', 'ok' )

  def test_brunel_nok( self ):
    self.generalTest( 'DataReprocessing', 'nok' )
#  def test_daVinci_nok( self ):
#    self.generalTest( 'DataReprocessing', 'nok', 'DaVinci' )

class XMLSummaryAnalysisDataStripping( XMLSummaryAnalysisTestCase ):

  def setUp( self ):
    super( XMLSummaryAnalysisDataStripping, self ).setUp()

  def test_daVinci_ok( self ):
    self.generalTest( 'DataStripping', 'ok' )

  def test_daVinci_nok( self ):
    self.generalTest( 'DataStripping', 'nok' )

class XMLSummaryAnalysisSelection( XMLSummaryAnalysisTestCase ):

  def setUp( self ):
    super( XMLSummaryAnalysisSelection, self ).setUp()

  def test_daVinci_ok( self ):
    self.generalTest( 'Selection', 'ok' )

  def test_daVinci_nok( self ):
    self.generalTest( 'Selection', 'nok' )

class XMLSummaryAnalysisMCSimulation( XMLSummaryAnalysisTestCase ):

  def setUp( self ):
    super( XMLSummaryAnalysisMCSimulation, self ).setUp()

  def test_brunel_ok( self ):
    self.generalTest( 'MCSimulation', 'ok' )
  def test_boole_ok( self ):
    self.generalTest( 'MCSimulation', 'ok' )
  def test_gauss_ok( self ):
    self.generalTest( 'MCSimulation', 'ok' )
#  def test_daVinci_ok( self ):
#    self.generalTest( 'MCSimulation', 'ok' )

  # def test_brunel_nok( self ):
  #   self.generalTest( 'MCSimulation', 'nok' )
#  def test_daVinci_nok( self ):
#    self.generalTest( 'MCSimulation', 'nok' )

class XMLSummaryAnalysisMerge( XMLSummaryAnalysisTestCase ):

  def setUp( self ):
    super( XMLSummaryAnalysisMerge, self ).setUp()

#  def test_brunel_ok( self ):
#    self.generalTest( 'Merge', 'ok' )
#  def test_boole_ok( self ):
#    self.generalTest( 'Merge', 'ok' )
#  def test_gauss_ok( self ):
#    self.generalTest( 'Merge', 'ok' )
  def test_daVinci_ok( self ):
    self.generalTest( 'Merge', 'ok' )
  def test_lhcb_ok( self ):
    self.generalTest('Merge', 'ok' )

  def test_lhcb_nok( self ):
    self.generalTest( 'Merge', 'nok' )

class ProductionXMLLogAnalysisRemoval( XMLSummaryAnalysisTestCase ):
  pass

class ProductionXMLLogAnalysisReplication( XMLSummaryAnalysisTestCase ):
  pass

################################################################################

def run():
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( XMLSummaryAnalysisTestCase )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( XMLSummaryAnalysisDataReconstruction ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( XMLSummaryAnalysisDataReprocessing ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( XMLSummaryAnalysisDataStripping ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( XMLSummaryAnalysisSelection ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( XMLSummaryAnalysisMCSimulation ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( XMLSummaryAnalysisMerge ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
  return testResult

################################################################################

if __name__ == '__main__':
  run()

################################################################################
# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
