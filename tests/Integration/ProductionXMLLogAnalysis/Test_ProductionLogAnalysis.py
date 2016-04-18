import os, unittest

from LHCbDIRAC.Core.Utilities.ProductionLogs import analyseLogFile, LogError

class ProductionLogAnalysisTestCase( unittest.TestCase ):
  """ Base class for the ProductionLogAnalysis test cases
  """
  def setUp( self ):
    self.workdir = os.getcwd() + '/LHCbDIRAC/tests/Integration/ProductionXMLLogAnalysis'

  def generalTest( self, workdir, directory, app ):

    workdir = '%s/%s' % ( workdir, directory )

    for filename in os.listdir( workdir ):
      if filename.startswith( app ) and 'log' in filename:
        print "filename = ", filename
        if directory == 'ok':
          res = analyseLogFile( '%s/%s' % ( workdir, filename ) )
          self.assertEqual( res, True )
        elif directory == 'nok':
          res = analyseLogFile( '%s/%s' % ( workdir, filename ) )
          self.assertEqual( res, False )
        else:
          self.assertRaises( LogError, analyseLogFile, '%s/%s' % ( workdir, filename ) )


class ProductionLogAnalysisDataReconstruction( ProductionLogAnalysisTestCase ):

  def setUp( self ):
    super( ProductionLogAnalysisDataReconstruction, self ).setUp()
    self.workdir += '/DataReconstruction'

  def test_brunel_ok( self ):
    self.generalTest( self.workdir, 'ok', 'Brunel' )
  def test_daVinci_ok( self ):
    self.generalTest( self.workdir, 'ok', 'DaVinci' )

  def test_brunel_nok( self ):
    self.generalTest( self.workdir, 'nok', 'Brunel' )
#  def test_daVinci_nok( self ):
#    self.generalTest( self.workdir, 'nok', 'DaVinci' )

class ProductionLogAnalysisDataReprocessing( ProductionLogAnalysisTestCase ):

  def setUp( self ):
    super( ProductionLogAnalysisDataReprocessing, self ).setUp()
    self.workdir += '/DataReprocessing'

  def test_brunel_ok( self ):
    self.generalTest( self.workdir, 'ok', 'Brunel' )
  def test_daVinci_ok( self ):
    self.generalTest( self.workdir, 'ok', 'DaVinci' )

  def test_brunel_nok( self ):
    self.generalTest( self.workdir, 'nok', 'Brunel' )
#  def test_daVinci_nok( self ):
#    self.generalTest( self.workdir, 'nok', 'DaVinci' )

class ProductionLogAnalysisDataStripping( ProductionLogAnalysisTestCase ):

  def setUp( self ):
    super( ProductionLogAnalysisDataStripping, self ).setUp()
    self.workdir += '/DataStripping'

  def test_daVinci_ok( self ):
    self.generalTest( self.workdir, 'ok', 'DaVinci' )

  def test_daVinci_nok( self ):
    self.generalTest( self.workdir, 'nok', 'DaVinci' )

class ProductionLogAnalysisSelection( ProductionLogAnalysisTestCase ):

  def setUp( self ):
    super( ProductionLogAnalysisSelection, self ).setUp()
    self.workdir += '/Selection'

  def test_daVinci_ok( self ):
    self.generalTest( self.workdir, 'ok', 'DaVinci' )

  def test_daVinci_nok( self ):
    self.generalTest( self.workdir, 'nok', 'DaVinci' )

class ProductionLogAnalysisMCSimulation( ProductionLogAnalysisTestCase ):

  def setUp( self ):
    super( ProductionLogAnalysisMCSimulation, self ).setUp()
    self.workdir += '/MCSimulation'

  def test_brunel_ok( self ):
    self.generalTest( self.workdir, 'ok', 'Brunel' )
  def test_boole_ok( self ):
    self.generalTest( self.workdir, 'ok', 'Boole' )
  def test_gauss_ok( self ):
    self.generalTest( self.workdir, 'ok', 'Gauss' )
#  def test_daVinci_ok( self ):
#    self.generalTest( self.workdir, 'ok', 'DaVinci' )

  def test_brunel_nok( self ):
    self.generalTest( self.workdir, 'nok', 'Brunel' )

  def test_brunel_fail( self ):
    self.generalTest( self.workdir, 'fail', 'Brunel' )

class ProductionLogAnalysisMerge( ProductionLogAnalysisTestCase ):

  def setUp( self ):
    super( ProductionLogAnalysisMerge, self ).setUp()
    self.workdir += '/Merge'

#  def test_brunel_ok( self ):
#    self.generalTest( self.workdir, 'ok', 'Brunel' )
#  def test_boole_ok( self ):
#    self.generalTest( self.workdir, 'ok', 'Boole' )
#  def test_gauss_ok( self ):
#    self.generalTest( self.workdir, 'ok', 'Gauss' )
  def test_daVinci_ok( self ):
    self.generalTest( self.workdir, 'ok', 'DaVinci' )
  def test_lhcb_ok( self ):
    self.generalTest( self.workdir, 'ok', 'LHCb' )

  def test_lhcb_nok( self ):
    self.generalTest( self.workdir, 'nok', 'LHCb' )

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
