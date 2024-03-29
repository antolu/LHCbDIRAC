""" This submits user jobs using a second user, for which a proxy is downloaded locally
    This means that to run this test you need to have the KARMA!
"""

# pylint: disable=wrong-import-position,unused-wildcard-import,wildcard-import

import unittest

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from DIRAC import gLogger

from DIRAC.Core.Security.ProxyInfo import getProxyInfo
from DIRAC.ResourceStatusSystem.Client.ResourceStatus import ResourceStatus
from DIRAC.DataManagementSystem.Client.DataManager import DataManager
from DIRAC.tests.System.unitTestUserJobs import GridSubmissionTestCase as DIRACGridSubmissionTestCase

from LHCbDIRAC.Interfaces.API.DiracLHCb import DiracLHCb

# from DIRAC.tests.Utilities.utils import find_all

try:
  from LHCbDIRAC.tests.Utilities.testJobDefinitions import *
except ImportError:
  from tests.Utilities.testJobDefinitions import *

gLogger.setLevel('VERBOSE')

jobsSubmittedList = []


class GridSubmissionTestCase(unittest.TestCase):
  """ Base class for the Regression test cases
  """

  def setUp(self):
    self.dirac = DiracLHCb()

    result = getProxyInfo()
    if result['Value']['group'] not in ['lhcb_admin']:
      print "GET A ADMIN GROUP"
      exit(1)

    result = ResourceStatus().getElementStatus('PIC-USER', 'StorageElement', 'WriteAccess')
    if result['Value']['PIC-USER']['WriteAccess'].lower() != 'banned':
      print "BAN PIC-USER in writing! and then restart this test"
      exit(1)

    res = DataManager().getReplicas(['/lhcb/user/f/fstagni/test/testInputFileSingleLocation.txt',
                                     '/lhcb/user/f/fstagni/test/testInputFile.txt'])
    if not res['OK']:
      print "DATAMANAGER.getRepicas failure: %s" % res['Message']
      exit(1)
    if res['Value']['Failed']:
      print "DATAMANAGER.getRepicas failed for something: %s" % res['Value']['Failed']
      exit(1)

    replicas = res['Value']['Successful']
    if replicas['/lhcb/user/f/fstagni/test/testInputFile.txt'].keys() != ['CERN-USER', 'IN2P3-USER']:
      print "/lhcb/user/f/fstagni/test/testInputFile.txt locations are not correct"
    if replicas['/lhcb/user/f/fstagni/test/testInputFileSingleLocation.txt'].keys() != ['CERN-USER']:
      print "/lhcb/user/f/fstagni/test/testInputFileSingleLocation.txt locations are not correct"

  def tearDown(self):
    pass


class LHCbsubmitSuccess(GridSubmissionTestCase, DIRACGridSubmissionTestCase):

  def test_LHCbsubmit(self):

    for uName, uGroup in [('chaen', 'lhcb_user'), ('zmathe', 'lhcb_admin')]:

      res = helloWorldTestT2s(proxyUserName=uName, proxyUserGroup=uGroup)  # pylint: disable=unexpected-keyword-arg
      self.assertTrue(res['OK'])
      jobsSubmittedList.append(res['Value'])

      res = helloWorldTestCERN(proxyUserName=uName, proxyUserGroup=uGroup)  # pylint: disable=unexpected-keyword-arg
      self.assertTrue(res['OK'])
      jobsSubmittedList.append(res['Value'])

      res = helloWorldTestSLC6(proxyUserName=uName, proxyUserGroup=uGroup)  # pylint: disable=unexpected-keyword-arg
      self.assertTrue(res['OK'])
      jobsSubmittedList.append(res['Value'])

      res = helloWorldTestSLC5(proxyUserName=uName, proxyUserGroup=uGroup)  # pylint: disable=unexpected-keyword-arg
      self.assertTrue(res['OK'])
      jobsSubmittedList.append(res['Value'])

#       res = jobWithOutput( proxyUserName = uName, proxyUserGroup = uGroup )
#       self.assertTrue(res['OK'])
#       jobsSubmittedList.append( res['Value'] )
#
#       res = jobWithOutputAndPrepend( proxyUserName = uName, proxyUserGroup = uGroup )
#       self.assertTrue(res['OK'])
#       jobsSubmittedList.append( res['Value'] )
#
#       jobWithOutputAndPrependWithUnderscore( proxyUserName = uName, proxyUserGroup = uGroup )
#
#       res = jobWithOutputAndReplication( proxyUserName = uName, proxyUserGroup = uGroup )
#       self.assertTrue(res['OK'])
#       jobsSubmittedList.append( res['Value'] )
#
#       res = jobWith2OutputsToBannedSE( proxyUserName = uName, proxyUserGroup = uGroup )
#       self.assertTrue(res['OK'])
#       jobsSubmittedList.append( res['Value'] )

      res = jobWithSingleInputData(proxyUserName=uName, proxyUserGroup=uGroup)  # pylint: disable=unexpected-keyword-arg
      self.assertTrue(res['OK'])
      jobsSubmittedList.append(res['Value'])

      res = jobWithSingleInputDataSpreaded(
          proxyUserName=uName, proxyUserGroup=uGroup)  # pylint: disable=unexpected-keyword-arg
      self.assertTrue(res['OK'])
      jobsSubmittedList.append(res['Value'])

      res = gaussJob(proxyUserName=uName, proxyUserGroup=uGroup)  # pylint: disable=unexpected-keyword-arg
      self.assertTrue(res['OK'])
      jobsSubmittedList.append(res['Value'])

      res = booleJob(proxyUserName=uName, proxyUserGroup=uGroup)  # pylint: disable=unexpected-keyword-arg
      self.assertTrue(res['OK'])
      jobsSubmittedList.append(res['Value'])

      res = gaudiApplicationScriptJob(
          proxyUserName=uName, proxyUserGroup=uGroup)  # pylint: disable=unexpected-keyword-arg
      self.assertTrue(res['OK'])
      jobsSubmittedList.append(res['Value'])

      res = wrongJob(proxyUserName=uName, proxyUserGroup=uGroup)  # pylint: disable=unexpected-keyword-arg
      self.assertTrue(res['OK'])
      jobsSubmittedList.append(res['Value'])


#############################################################################
# Test Suite run
#############################################################################

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(GridSubmissionTestCase)
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(LHCbsubmitSuccess))
#   suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( monitorSuccess ) )
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)
