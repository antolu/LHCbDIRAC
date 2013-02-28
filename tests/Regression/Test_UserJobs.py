from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

import os.path
import unittest

from LHCbTestDirac.Regression.utils import cleanTestDir

from DIRAC import gLogger
from LHCbDIRAC.Interfaces.API.LHCbJob import LHCbJob
from LHCbDIRAC.Interfaces.API.DiracLHCb import DiracLHCb

gLogger.setLevel( 'DEBUG' )

cwd = os.path.realpath( '.' )

class UserJobTestCase( unittest.TestCase ):
  ''' Base class for the UserJob test cases
  '''
  def setUp( self ):
    cleanTestDir()

    gLogger.setLevel( 'DEBUG' )
    self.dLHCb = DiracLHCb()


class HelloWorldSuccess( UserJobTestCase ):
  def test_execute( self ):

    lhcbJob = LHCbJob()

    lhcbJob.setName( "helloWorld-test" )
    lhcbJob.setExecutable( "exe-script.py" )
    res = lhcbJob.runLocal( self.dLHCb )
    self.assertTrue( res['OK'] )

class GaudirunSuccess( UserJobTestCase ):
  def test_execute( self ):

    lhcbJob = LHCbJob()

    lhcbJob.setName( "gaudirun-test" )
    lhcbJob.setApplication( 'Gauss', 'v41r4',
                            "$APPCONFIGOPTS/Gauss/Beam3500GeV-md100-MC11-nu2.py;$DECFILESROOT/options/12133041.py;$LBPYTHIAROOT/options/Pythia.py;$APPCONFIGOPTS/Gauss/G4PL_LHEP_EmNoCuts.py",
                            events = 2 )

    res = lhcbJob.runLocal( self.dLHCb )
    self.assertTrue( res['OK'] )


if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( UserJobTestCase )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( HelloWorldSuccess ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( GaudirunSuccess ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
