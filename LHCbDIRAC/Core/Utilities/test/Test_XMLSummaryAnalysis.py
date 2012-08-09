import os, unittest

from LHCbDIRAC.Core.Utilities.XMLSummaries import analyseXMLSummary

__RCSID__ = "$Id$"

class XMLSummaryAnalysisTestCase( unittest.TestCase ):
  """ Base class for the XMLSummaryAnalysis test cases
  """
  def setUp( self ):
    
    self.workdir = '/tmp/ProductionXMLLogAnalysis'
    #self.workdir = os.getcwd() + '/ProductionXMLLogAnalysis'

  def generalTest( self, workdir, status ):

    result = status == 'ok'

    workdir = '%s/%s' % ( workdir, status )

    for filename in os.listdir( workdir ):
      if filename.startswith( 'summary' ):
        print "filename = ", filename
        res = analyseXMLSummary( '%s/%s' % ( workdir, filename ) )
        if res[ 'OK' ] != result:
          print '   %s' % filename
          print res[ 'Message' ]
        self.assertEqual( res[ 'OK' ], result )

class XMLSummaryAnalysisDataReconstruction( XMLSummaryAnalysisTestCase ):

  def setUp( self ):
    super( XMLSummaryAnalysisDataReconstruction, self ).setUp()
    self.workdir += '/DataReconstruction'

  def test_brunel_ok( self ):
    self.generalTest( self.workdir, 'ok' )
  def test_daVinci_ok( self ):
    self.generalTest( self.workdir, 'ok' )

  def test_brunel_nok( self ):
    self.generalTest( self.workdir, 'nok' )
#  def test_daVinci_nok( self ):  
#    self.generalTest( self.workdir, 'nok', 'DaVinci' )  

class XMLSummaryAnalysisDataReprocessing( XMLSummaryAnalysisTestCase ):

  def setUp( self ):
    super( XMLSummaryAnalysisDataReprocessing, self ).setUp()
    self.workdir += '/DataReprocessing'

  def test_brunel_ok( self ):
    self.generalTest( self.workdir, 'ok' )
  def test_daVinci_ok( self ):
    self.generalTest( self.workdir, 'ok' )

  def test_brunel_nok( self ):
    self.generalTest( self.workdir, 'nok' )
#  def test_daVinci_nok( self ):  
#    self.generalTest( self.workdir, 'nok', 'DaVinci' )  

class XMLSummaryAnalysisDataStripping( XMLSummaryAnalysisTestCase ):

  def setUp( self ):
    super( XMLSummaryAnalysisDataStripping, self ).setUp()
    self.workdir += '/DataStripping'

#  def test_brunel_ok( self ):  
#    self.generalTest( self.workdir, 'ok', 'Brunel' )     
  def test_daVinci_ok( self ):
    self.generalTest( self.workdir, 'ok' )

#  def test_brunel_nok( self ):  
#    self.generalTest( self.workdir, 'nok', 'Brunel' )  
  def test_daVinci_nok( self ):
    self.generalTest( self.workdir, 'nok' )

class XMLSummaryAnalysisSelection( XMLSummaryAnalysisTestCase ):

  def setUp( self ):
    super( XMLSummaryAnalysisSelection, self ).setUp()
    self.workdir += '/Selection'

  def test_daVinci_ok( self ):
    self.generalTest( self.workdir, 'ok' )

  def test_daVinci_nok( self ):
    self.generalTest( self.workdir, 'nok' )

class XMLSummaryAnalysisMCSimulation( XMLSummaryAnalysisTestCase ):

  def setUp( self ):
    super( XMLSummaryAnalysisMCSimulation, self ).setUp()
    self.workdir += '/MCSimulation'

  def test_brunel_ok( self ):
    self.generalTest( self.workdir, 'ok' )
  def test_boole_ok( self ):
    self.generalTest( self.workdir, 'ok' )
  def test_gauss_ok( self ):
    self.generalTest( self.workdir, 'ok' )
#  def test_daVinci_ok( self ):  
#    self.generalTest( self.workdir, 'ok', 'DaVinci' )

  def test_brunel_nok( self ):
    self.generalTest( self.workdir, 'nok' )
#  def test_daVinci_nok( self ):  
#    self.generalTest( self.workdir, 'nok', 'DaVinci' )  

class XMLSummaryAnalysisMerge( XMLSummaryAnalysisTestCase ):

  def setUp( self ):
    super( XMLSummaryAnalysisMerge, self ).setUp()
    self.workdir += '/Merge'

#  def test_brunel_ok( self ):  
#    self.generalTest( self.workdir, 'ok', 'Brunel' )     
#  def test_boole_ok( self ):  
#    self.generalTest( self.workdir, 'ok', 'Boole' )
#  def test_gauss_ok( self ):  
#    self.generalTest( self.workdir, 'ok', 'Gauss' )
  def test_daVinci_ok( self ):
    self.generalTest( self.workdir, 'ok' )
  def test_lhcb_ok( self ):
    self.generalTest( self.workdir, 'ok' )

  def test_lhcb_nok( self ):
    self.generalTest( self.workdir, 'nok' )

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
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
