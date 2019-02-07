''' This is a test of the chain
    ResourceManagementClient -> ResourceManagementHandler -> ResourceManagementDB
    It supposes that the DB is present, and that the service is running

'''

# pylint: disable=invalid-name,wrong-import-position

import unittest
import sys
import datetime

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from DIRAC import gLogger
from DIRAC.tests.Integration.ResourceStatusSystem.Test_ResourceManagement import ResourceManagementClientChain as \
    DIRACResourceManagementClientChain
from LHCbDIRAC.ResourceStatusSystem.Client.ResourceManagementClient import ResourceManagementClient

gLogger.setLevel('DEBUG')

# DIRACResourceManagementClientChain()

rsClient = ResourceManagementClient()

Datetime = datetime.datetime.now()


class LHCbResourceManagementTestCase(unittest.TestCase):

  def setUp(self):
    self.rmClient = ResourceManagementClient()

  def tearDown(self):
    pass


class LHCbResourceManagementClientChain(LHCbResourceManagementTestCase, DIRACResourceManagementClientChain):

  def test_addAndRemove(self):

    # TEST addOrModifyMonitoringTest
    # ...............................................................................

    res = rsClient.addOrModifyMonitoringTest('TestName1234', 'serviceURI', 'siteName', 'serviceFlavour',
                                             'metricStatus', 'summaryData', Datetime, Datetime)
    self.assertTrue(res['OK'])

    res = rsClient.selectMonitoringTest('TestName1234')
    self.assertTrue(res['OK'])
    # check if the name that we got is equal to the previously added 'TestName1234'
    self.assertEqual(res['Value'][0][1], 'TestName1234')

    # TEST deleteMonitoringTest
    # ...............................................................................

    res = rsClient.deleteMonitoringTest('TestName1234')
    self.assertTrue(res['OK'])

    res = rsClient.selectMonitoringTest('TestName1234')
    self.assertTrue(res['OK'])
    self.assertFalse(res['Value'])

    # TEST addOrModifyJobAccountingCache
    # ...............................................................................

    res = rsClient.addOrModifyJobAccountingCache('TestName1234')
    self.assertTrue(res['OK'])

    res = rsClient.selectJobAccountingCache('TestName1234')
    self.assertTrue(res['OK'])
    # check if the name that we got is equal to the previously added 'TestName1234'
    self.assertEqual(res['Value'][0][0], 'TestName1234')

    # TEST deleteJobAccountingCache
    # ...............................................................................

    res = rsClient.deleteJobAccountingCache('TestName1234')
    self.assertTrue(res['OK'])

    res = rsClient.selectJobAccountingCache('TestName1234')
    self.assertTrue(res['OK'])
    self.assertFalse(res['Value'])

    # TEST addOrModifyPilotAccountingCache
    # ...............................................................................

    res = rsClient.addOrModifyPilotAccountingCache('TestName1234')
    self.assertTrue(res['OK'])

    res = rsClient.selectPilotAccountingCache('TestName1234')
    self.assertTrue(res['OK'])
    # check if the name that we got is equal to the previously added 'TestName1234'
    self.assertEqual(res['Value'][0][0], 'TestName1234')

    # TEST deletePilotAccountingCache
    # ...............................................................................

    res = rsClient.deletePilotAccountingCache('TestName1234')
    self.assertTrue(res['OK'])

    res = rsClient.selectPilotAccountingCache('TestName1234')
    self.assertTrue(res['OK'])
    self.assertFalse(res['Value'])


if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(LHCbResourceManagementTestCase)
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(LHCbResourceManagementClientChain))
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)
  sys.exit(not testResult.wasSuccessful())

# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
