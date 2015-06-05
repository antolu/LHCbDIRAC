""" This submits user jobs using a second user, for which a proxy is downloaded locally
    This means that to run this test you need to have the KARMA!
"""

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from DIRAC import gLogger
from DIRAC.Core.Security.ProxyInfo import getProxyInfo
from DIRAC.DataManagementSystem.Client.DataManager import DataManager

from TestDIRAC.System.unitTestUserJobs import GridSubmissionTestCase as DIRACGridSubmissionTestCase
from TestDIRAC.Utilities.utils import find_all

from LHCbDIRAC.Interfaces.API.LHCbJob import LHCbJob
from LHCbDIRAC.Interfaces.API.DiracLHCb import DiracLHCb
from LHCbTestDirac.Integration.Workflow.Test_UserJobs import createJob

gLogger.setLevel( 'VERBOSE' )

jobsSubmittedList = []

class GridSubmissionTestCase( unittest.TestCase ):
  """ Base class
  """
  def setUp( self ):
    self.dirac = DiracLHCb()

    result = getProxyInfo()
    if result['Value']['group'] not in ['diracAdmin']:
      print "GET A diracAdmin GROUP"
      exit( 1 )

    res = DataManager().getReplicas( ['/lhcb/user/f/fstagni/test/testInputFileSingleLocation.txt',
                                      '/lhcb/user/f/fstagni/test/testInputFile.txt'] )
    if not res['OK']:
      print "DATAMANAGER.getRepicas failure: %s" % res['Message']
      exit( 1 )
    if res['Value']['Failed']:
      print "DATAMANAGER.getRepicas failed for someting: %s" % res['Value']['Failed']
      exit( 1 )

    replicas = res['Value']['Successful']
    if replicas['/lhcb/user/f/fstagni/test/testInputFile.txt'].keys() != ['CERN-USER', 'IN2P3-USER']:
      print "/lhcb/user/f/fstagni/test/testInputFile.txt locations are not correct"
    if replicas['/lhcb/user/f/fstagni/test/testInputFileSingleLocation.txt'].keys() != ['CERN-USER']:
      print "/lhcb/user/f/fstagni/test/testInputFileSingleLocation.txt locations are not correct"

  def tearDown( self ):
    pass

class LHCbsubmitSuccess( GridSubmissionTestCase, DIRACGridSubmissionTestCase ):

  def test_LHCbsubmit( self ):

    print "**********************************************************************************************************"

