import unittest
from DIRAC import gLogger
from DIRAC.Core.Base.Script import parseCommandLine
from LHCbTestDirac.Utilities.utils import cleanTestDir

class IntegrationTest( unittest.TestCase ):
  """ Base class for the integration and regression tests
  """

  def setUp( self ):
    cleanTestDir()
    parseCommandLine()

    gLogger.setLevel( 'DEBUG' )

  def tearDown( self ):
    cleanTestDir()
