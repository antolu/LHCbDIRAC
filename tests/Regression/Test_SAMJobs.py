from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

import unittest

from LHCbTestDirac.Regression.utils import cleanTestDir

from DIRAC import gLogger, gConfig

from LHCbDIRAC.SAMSystem.Client.DiracSAM import DiracSAM

class SAMJobTestCase( unittest.TestCase ):
  ''' Base class for the SAMJob test cases
  '''
  def setUp( self ):
    cleanTestDir()

    gLogger.setLevel( 'DEBUG' )
    self.diracSAM = DiracSAM()
    self.subLogger = gLogger.getSubLogger( __file__ )

    self.ce = gConfig.getValue( 'LocalSite/GridCE' )
    self.subLogger.debug( self.ce )

class SAMSuccess( SAMJobTestCase ):
  def test_execute( self ):

    res = self.diracSAM.submitSAMJob( ce = self.ce, mode = 'local' )
    self.assertTrue( res['OK'] )

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( SAMJobTestCase )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( SAMSuccess ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
