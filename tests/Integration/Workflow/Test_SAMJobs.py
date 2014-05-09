from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

import unittest

from LHCbTestDirac.Utilities.IntegrationTest import IntegrationTest

from DIRAC import gLogger, gConfig

from LHCbDIRAC.SAMSystem.Client.DiracSAM import DiracSAM

class SAMJobTestCase( IntegrationTest ):
  """ Base class for the SAMJob test cases
  """
  def setUp( self ):
    super( IntegrationTest, self ).setUp()

    self.diracSAM = DiracSAM()
    self.subLogger = gLogger.getSubLogger( __file__ )

    self.ce = gConfig.getValue( 'LocalSite/GridCE', 'ce201.cern.ch' )
    self.subLogger.debug( self.ce )

class SAMSuccess( SAMJobTestCase ):
  def test_execute( self ):

    res = self.diracSAM.submitNewSAMJob( ce = self.ce, runLocal = True )
    self.assertTrue( res['OK'] )

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( SAMJobTestCase )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( SAMSuccess ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
