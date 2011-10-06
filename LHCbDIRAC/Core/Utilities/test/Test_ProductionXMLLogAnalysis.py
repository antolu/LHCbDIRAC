import os, unittest

from LHCbDIRAC.Core.Utilities.ProductionXMLLogAnalysis import analyseXMLLogFile

__RCSID__ = "$Id:  $"

class ProductionXMLLogAnalysisTestCase( unittest.TestCase ):
  """ Base class for the ProductionXMLLogAnalysis test cases
  """
  def setUp( self ):
    self.workdir = os.getcwd() + '/ProductionXMLLogAnalysis' 

  def generalTest( self, workdir, status, app ):  
    
    result = status == 'ok'
        
    workdir = '%s/%s' % ( workdir, status )
    
    for filename in os.listdir( workdir ):
      if filename.startswith( app ):  
        res = analyseXMLLogFile( '%s/%s' % ( workdir, filename ) )    
        if res[ 'OK' ] != result:
          print '   %s' % filename
        self.assertEqual( res[ 'OK' ], result )
  
class ProductionXMLLogAnalysisDataReconstruction( ProductionXMLLogAnalysisTestCase ):
  
  def setUp( self ):
    super( ProductionXMLLogAnalysisDataReconstruction, self ).setUp()
    self.workdir += '/DataReconstruction'   
     
  def test_brunel_ok( self ):  
    self.generalTest( self.workdir, 'ok', 'Brunel' )     
  def test_daVinci_ok( self ):  
    self.generalTest( self.workdir, 'ok', 'DaVinci' )

  def test_brunel_nok( self ):  
    self.generalTest( self.workdir, 'nok', 'Brunel' )  
#  def test_daVinci_nok( self ):  
#    self.generalTest( self.workdir, 'nok', 'DaVinci' )  

class ProductionXMLLogAnalysisDataReprocessing( ProductionXMLLogAnalysisTestCase ):
  
  def setUp( self ):
    super( ProductionXMLLogAnalysisDataReprocessing, self ).setUp()
    self.workdir += '/DataReprocessing'   
     
  def test_brunel_ok( self ):  
    self.generalTest( self.workdir, 'ok', 'Brunel' )     
  def test_daVinci_ok( self ):  
    self.generalTest( self.workdir, 'ok', 'DaVinci' )

  def test_brunel_nok( self ):  
    self.generalTest( self.workdir, 'nok', 'Brunel' )  
#  def test_daVinci_nok( self ):  
#    self.generalTest( self.workdir, 'nok', 'DaVinci' )  

class ProductionXMLLogAnalysisDataStripping( ProductionXMLLogAnalysisTestCase ):
  
  def setUp( self ):
    super( ProductionXMLLogAnalysisDataStripping, self ).setUp()
    self.workdir += '/DataStripping'   
     
#  def test_brunel_ok( self ):  
#    self.generalTest( self.workdir, 'ok', 'Brunel' )     
  def test_daVinci_ok( self ):  
    self.generalTest( self.workdir, 'ok', 'DaVinci' )

#  def test_brunel_nok( self ):  
#    self.generalTest( self.workdir, 'nok', 'Brunel' )  
  def test_daVinci_nok( self ):  
    self.generalTest( self.workdir, 'nok', 'DaVinci' )  

class ProductionXMLLogAnalysisMCSimulation( ProductionXMLLogAnalysisTestCase ):
  
  def setUp( self ):
    super( ProductionXMLLogAnalysisMCSimulation, self ).setUp()
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
#  def test_daVinci_nok( self ):  
#    self.generalTest( self.workdir, 'nok', 'DaVinci' )  

class ProductionXMLLogAnalysisMerge( ProductionXMLLogAnalysisTestCase ):
  
  def setUp( self ):
    super( ProductionXMLLogAnalysisMerge, self ).setUp()
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

class ProductionXMLLogAnalysisRemoval( ProductionXMLLogAnalysisTestCase ):
  pass

class ProductionXMLLogAnalysisReplication( ProductionXMLLogAnalysisTestCase ):
  pass

################################################################################

def run():
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( ProductionXMLLogAnalysisTestCase )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( ProductionXMLLogAnalysisDataReconstruction ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( ProductionXMLLogAnalysisDataReprocessing ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( ProductionXMLLogAnalysisDataStripping ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( ProductionXMLLogAnalysisMCSimulation ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( ProductionXMLLogAnalysisMerge ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
  return testResult

################################################################################

if __name__ == '__main__':
  run()  
  
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF  