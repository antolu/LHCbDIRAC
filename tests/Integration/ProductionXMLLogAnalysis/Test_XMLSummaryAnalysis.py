""" Test of XMLSummaries
"""

import os
import unittest

from LHCbDIRAC.Core.Utilities.XMLSummaries import analyseXMLSummary, XMLSummaryError

class XMLSummaryAnalysisTestCase( unittest.TestCase ):
  """ Base class for the XMLSummaryAnalysis test cases
  """
  def setUp( self ):

    self.workdir = os.getcwd() + '/LHCbDIRAC/tests/Integration/ProductionXMLLogAnalysis'

  def generalTest( self, workdir, directory ):

    workdir = '%s/%s' % ( workdir, directory )

    try:
      ls = os.listdir( workdir )
    except OSError:
      workdir = os.path.expandvars('$TESTCODE') + '/LHCbDIRAC/tests/Integration/ProductionXMLLogAnalysis'
      ls = os.listdir( '%s/%s' % ( workdir, directory ) )

    for filename in ls:
      if filename.startswith( 'summary' ):
        print "filename = ", filename
        if directory == 'ok':
          res = analyseXMLSummary( '%s/%s' % ( workdir, filename ) )
          self.assertEqual( res, True )
        elif directory == 'nok':
          res = analyseXMLSummary( '%s/%s' % ( workdir, filename ) )
          self.assertEqual( res, False )
        else:
          self.assertRaises( XMLSummaryError, analyseXMLSummary, '%s/%s' % ( workdir, filename ) )

class XMLSummaryAnalysisDataReconstruction( XMLSummaryAnalysisTestCase ):

  def setUp( self ):
    super( XMLSummaryAnalysisDataReconstruction, self ).setUp()

  def test_brunel_ok( self ):
    self.generalTest( self.workdir + '/DataReconstruction', 'ok' )
  def test_daVinci_ok( self ):
    self.generalTest( self.workdir + '/DataReconstruction', 'ok' )

  def test_brunel_nok( self ):
    self.generalTest( self.workdir + '/DataReconstruction', 'nok' )

  def test_brunel_fail( self ):
    self.generalTest( self.workdir + '/DataReconstruction', 'fail' )

class XMLSummaryAnalysisDataReprocessing( XMLSummaryAnalysisTestCase ):

  def setUp( self ):
    super( XMLSummaryAnalysisDataReprocessing, self ).setUp()

  def test_brunel_ok( self ):
    self.generalTest( self.workdir + '/DataReprocessing', 'ok' )
  def test_daVinci_ok( self ):
    self.generalTest( self.workdir + '/DataReprocessing', 'ok' )

  def test_brunel_nok( self ):
    self.generalTest( self.workdir + '/DataReprocessing', 'nok' )
#  def test_daVinci_nok( self ):
#    self.generalTest( self.workdir, 'nok', 'DaVinci' )

class XMLSummaryAnalysisDataStripping( XMLSummaryAnalysisTestCase ):

  def setUp( self ):
    super( XMLSummaryAnalysisDataStripping, self ).setUp()

#  def test_brunel_ok( self ):
#    self.generalTest( self.workdir, 'ok', 'Brunel' )
  def test_daVinci_ok( self ):
    self.generalTest( self.workdir + '/DataStripping', 'ok' )

#  def test_brunel_nok( self ):
#    self.generalTest( self.workdir, 'nok', 'Brunel' )
  def test_daVinci_nok( self ):
    self.generalTest( self.workdir + '/DataStripping', 'nok' )

class XMLSummaryAnalysisSelection( XMLSummaryAnalysisTestCase ):

  def setUp( self ):
    super( XMLSummaryAnalysisSelection, self ).setUp()

  def test_daVinci_ok( self ):
    self.generalTest( self.workdir + '/Selection', 'ok' )

  def test_daVinci_nok( self ):
    self.generalTest( self.workdir + '/Selection', 'nok' )

class XMLSummaryAnalysisMCSimulation( XMLSummaryAnalysisTestCase ):

  def setUp( self ):
    super( XMLSummaryAnalysisMCSimulation, self ).setUp()

  def test_brunel_ok( self ):
    self.generalTest( self.workdir + '/MCSimulation', 'ok' )
  def test_boole_ok( self ):
    self.generalTest( self.workdir + '/MCSimulation', 'ok' )
  def test_gauss_ok( self ):
    self.generalTest( self.workdir + '/MCSimulation', 'ok' )
#  def test_daVinci_ok( self ):
#    self.generalTest( self.workdir, 'ok', 'DaVinci' )

  # def test_brunel_nok( self ):
  #   self.generalTest( self.workdir, 'nok' )
#  def test_daVinci_nok( self ):
#    self.generalTest( self.workdir, 'nok', 'DaVinci' )

class XMLSummaryAnalysisMerge( XMLSummaryAnalysisTestCase ):

  def setUp( self ):
    super( XMLSummaryAnalysisMerge, self ).setUp()

#  def test_brunel_ok( self ):
#    self.generalTest( self.workdir, 'ok', 'Brunel' )
#  def test_boole_ok( self ):
#    self.generalTest( self.workdir, 'ok', 'Boole' )
#  def test_gauss_ok( self ):
#    self.generalTest( self.workdir, 'ok', 'Gauss' )
  def test_daVinci_ok( self ):
    self.generalTest( self.workdir + '/Merge', 'ok' )
  def test_lhcb_ok( self ):
    self.generalTest( self.workdir + '/Merge', 'ok' )

  def test_lhcb_nok( self ):
    self.generalTest( self.workdir + '/Merge', 'nok' )

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
