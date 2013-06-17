import unittest
from DIRAC import gLogger
from LHCbTestDirac.Utilities.utils import cleanTestDir

class IntegrationTest( unittest.TestCase ):
  """ Base class for the integration and regression tests
  """

  def setUp( self ):
    cleanTestDir()
    gLogger.setLevel( 'DEBUG' )

  def tearDown( self ):
    pass

