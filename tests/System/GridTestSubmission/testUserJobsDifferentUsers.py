""" This submits user jobs using a second user, for which a proxy is downloaded locally
    This means that to run this test you need to have the KARMA!
"""

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

# FIXME: use this to submit on behalf of another user
from DIRAC.
from DIRAC.Core.Utilities.Proxy                                      import executeWithUserProxy

import unittest
import time

from DIRAC import gLogger
from DIRAC.Core.Security.ProxyInfo import getProxyInfo
from DIRAC.ResourceStatusSystem.Client.ResourceStatus import ResourceStatus
from DIRAC.DataManagementSystem.Client.DataManager import DataManager


from TestDIRAC.System.unitTestUserJobs import GridSubmissionTestCase as DIRACGridSubmissionTestCase
from TestDIRAC.Utilities.utils import find_all

from LHCbDIRAC.Interfaces.API.LHCbJob import LHCbJob
from LHCbDIRAC.Interfaces.API.DiracLHCb import DiracLHCb
from LHCbTestDirac.Integration.Workflow.Test_UserJobs import createJob
from LHCbTestDirac.Utilities.testJobDefinitions import *

gLogger.setLevel( 'VERBOSE' )

jobsSubmittedList = []

class GridSubmissionTestCase( unittest.TestCase ):
  """ Base class for the Regression test cases
  """
  def setUp( self ):
    self.dirac = DiracLHCb()

    result = getProxyInfo()
    if result['Value']['group'] not in ['lhcb_user', 'dirac_user']:
      print "GET A USER GROUP"
      exit( 1 )

    result = ResourceStatus().getStorageElementStatus( 'PIC-USER', 'WriteAccess' )
    if result['Value']['PIC-USER']['WriteAccess'].lower() != 'banned':
      print "BAN PIC-USER in writing! and then restart this test"
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

    res = helloWorldTestT2s()
    self.assert_( res['OK'] )
    jobsSubmittedList.append( res['Value'] )

    res = helloWorldTestCERN()
    self.assert_( res['OK'] )
    jobsSubmittedList.append( res['Value'] )

    res = helloWorldTestSLC6()
    self.assert_( res['OK'] )
    jobsSubmittedList.append( res['Value'] )

    res = helloWorldTestSLC5()
    self.assert_( res['OK'] )
    jobsSubmittedList.append( res['Value'] )

    res = jobWithOutput()
    self.assert_( res['OK'] )
    jobsSubmittedList.append( res['Value'] )

    res = jobWithOutputAndPrepend()
    self.assert_( res['OK'] )
    jobsSubmittedList.append( res['Value'] )

    res = jobWithOutputAndPrependWithUnderscore()
    self.assert_( res['OK'] )
    jobsSubmittedList.append( res['Value'] )

#############################################################################
# Test Suite run
#############################################################################

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( GridSubmissionTestCase )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( LHCbsubmitSuccess ) )
#   suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( monitorSuccess ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
