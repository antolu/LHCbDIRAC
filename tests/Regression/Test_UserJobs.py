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

    helloJ = LHCbJob()

    helloJ.setName( "helloWorld-test" )
    helloJ.setExecutable( "exe-script.py" )
    res = helloJ.runLocal( self.dLHCb )
    self.assertTrue( res['OK'] )

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( UserJobTestCase )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( HelloWorldSuccess ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
