from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

import os.path
import unittest

from TestLHCbDIRAC.Regression.utils import cleanTestDir, getOutput

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
