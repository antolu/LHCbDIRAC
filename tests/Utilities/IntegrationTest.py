""" doc
"""

import unittest
from DIRAC import gLogger
from DIRAC.DataManagementSystem.Client.DataManager import DataManager
from LHCbDIRAC.Interfaces.API.DiracLHCb import DiracLHCb
try:
  from LHCbDIRAC.tests.Utilities.utils import cleanTestDir
except ImportError:
  from tests.Utilities.utils import cleanTestDir

class IntegrationTest( unittest.TestCase ):
  """ Base class for the integration and regression tests
  """

  def setUp( self ):
    cleanTestDir()
    self.dirac = DiracLHCb()
    gLogger.setLevel( 'DEBUG' )
    print "\n\n****************************************************** Start running test"

  def tearDown( self ):
    cleanTestDir()
    # pass


class FailingUserJobTestCase( IntegrationTest ):
  """ Base class for the faing jobs test cases
  """
  def setUp( self ):
    super( FailingUserJobTestCase, self ).setUp()

    dm = DataManager()
    res = dm.removeFile( ['/lhcb/testCfg/testVer/LOG/00012345/0006/00012345_00067890.tar',
                          '/lhcb/testCfg/testVer/SIM/00012345/0006/00012345_00067890_1.sim'],
                         force = True )
    if not res['OK']:
      print "Could not remove files", res['Message']
      exit( 1 )
