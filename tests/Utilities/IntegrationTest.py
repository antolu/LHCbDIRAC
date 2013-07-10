import unittest
from DIRAC import gLogger
from LHCbTestDirac.Utilities.utils import cleanTestDir
from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager
from LHCbDIRAC.Interfaces.API.DiracLHCb import DiracLHCb

class IntegrationTest( unittest.TestCase ):
  """ Base class for the integration and regression tests
  """

  def setUp( self ):
    cleanTestDir()
    self.dirac = DiracLHCb()
    gLogger.setLevel( 'DEBUG' )

  def tearDown( self ):
    cleanTestDir()


class FailingUserJobTestCase( IntegrationTest ):
  """ Base class for the faing jobs test cases
  """
  def setUp( self ):
    super( IntegrationTest, self ).setUp()

    rm = ReplicaManager()
    res = rm.removeFile( ['/lhcb/testCfg/testVer/LOG/00012345/0006/00012345_00067890.tar',
                          '/lhcb/testCfg/testVer/SIM/00012345/0006/00012345_00067890_1.sim'],
                        force = True )
    if not res['OK']:
      print "Could not remove files", res['Message']
      exit( 1 )

