""" This submits user jobs and then starts a thread that checks their results
"""

# pylint: disable=wrong-import-position,unused-wildcard-import,wildcard-import,invalid-name,missing-docstring

import unittest

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from DIRAC import gLogger
from DIRAC.Core.Security.ProxyInfo import getProxyInfo
from DIRAC.ResourceStatusSystem.Client.ResourceStatus import ResourceStatus
from DIRAC.DataManagementSystem.Client.DataManager import DataManager

from DIRAC.tests.System.unitTestUserJobs import GridSubmissionTestCase as DIRACGridSubmissionTestCase

from DIRAC.tests.Utilities.testJobDefinitions import *
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

    result = getProxyInfo()
    if result['Value']['group'] not in ['lhcb_user', 'dirac_user']:
      print "GET A USER GROUP"
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

    res = helloWorld()
    self.assertTrue(res['OK'])
    jobsSubmittedList.append(res['Value'])

    res = helloWorldTestT2s()
    self.assertTrue(res['OK'])
    jobsSubmittedList.append(res['Value'])

    res = helloWorldTestCERN()
    self.assertTrue(res['OK'])
    jobsSubmittedList.append(res['Value'])

    res = helloWorldTestIN2P3()
    self.assertTrue(res['OK'])
    jobsSubmittedList.append(res['Value'])

    res = helloWorldTestGRIDKA()
    self.assertTrue(res['OK'])
    jobsSubmittedList.append(res['Value'])

    res = helloWorldTestSSH()
    self.assertTrue(res['OK'])
    jobsSubmittedList.append(res['Value'])

    res = helloWorldTestSSHCondor()
    self.assertTrue(res['OK'])
    jobsSubmittedList.append(res['Value'])

    res = helloWorldTestARC()
    self.assertTrue(res['OK'])
    jobsSubmittedList.append(res['Value'])

    res = helloWorldTestVAC()
    self.assertTrue(res['OK'])
    jobsSubmittedList.append(res['Value'])

    res = helloWorldTestCLOUD()
    self.assertTrue(res['OK'])
    jobsSubmittedList.append(res['Value'])

#     res = helloWorldTestBOINC()
#     self.assertTrue(res['OK'])
#     jobsSubmittedList.append( res['Value'] )

    res = helloWorldTestSLC6()
    self.assertTrue(res['OK'])
    jobsSubmittedList.append(res['Value'])

    res = helloWorldTestSLC5()
    self.assertTrue(res['OK'])
    jobsSubmittedList.append(res['Value'])

    res = jobWithOutput()
    self.assertTrue(res['OK'])
    jobsSubmittedList.append(res['Value'])

    res = jobWithOutputAndPrepend()
    self.assertTrue(res['OK'])
    jobsSubmittedList.append(res['Value'])

    jobWithOutputAndPrependWithUnderscore()

    res = jobWithOutputAndReplication()
    self.assertTrue(res['OK'])
    jobsSubmittedList.append(res['Value'])

    res = jobWith2OutputsToBannedSE()
    self.assertTrue(res['OK'])
    jobsSubmittedList.append(res['Value'])

    res = jobWithSingleInputData()
    self.assertTrue(res['OK'])
    jobsSubmittedList.append(res['Value'])

    res = jobWithSingleInputDataCERN()
    self.assertTrue(res['OK'])
    jobsSubmittedList.append(res['Value'])

    res = jobWithSingleInputDataRAL()
    self.assertTrue(res['OK'])
    jobsSubmittedList.append(res['Value'])

    res = jobWithSingleInputDataIN2P3()
    self.assertTrue(res['OK'])
    jobsSubmittedList.append(res['Value'])

    res = jobWithSingleInputDataGRIDKA()
    self.assertTrue(res['OK'])
    jobsSubmittedList.append(res['Value'])

    res = jobWithSingleInputDataRRCKI()
    self.assertTrue(res['OK'])
    jobsSubmittedList.append(res['Value'])

    res = jobWithSingleInputDataSARA()
    self.assertTrue(res['OK'])
    jobsSubmittedList.append(res['Value'])

    res = jobWithSingleInputDataNIKHEF()
    self.assertTrue(res['OK'])
    jobsSubmittedList.append(res['Value'])

    res = jobWithSingleInputDataPIC()
    self.assertTrue(res['OK'])
    jobsSubmittedList.append(res['Value'])

    res = jobWithInputDataAndAncestor()
    self.assertTrue(res['OK'])
    jobsSubmittedList.append(res['Value'])

    res = jobWithSingleInputDataSpreaded()
    self.assertTrue(res['OK'])
    jobsSubmittedList.append(res['Value'])

    res = gaussJob()
    self.assertTrue(res['OK'])
    jobsSubmittedList.append(res['Value'])

    res = booleJob()
    self.assertTrue(res['OK'])
    jobsSubmittedList.append(res['Value'])

    res = gaudiApplicationScriptJob()
    self.assertTrue(res['OK'])
    jobsSubmittedList.append(res['Value'])

    res = daVinciLHCbScriptJob_v41r2()
    self.assertTrue(res['OK'])
    jobsSubmittedList.append(res['Value'])

    res = daVinciLHCbScriptJob_v42r1()
    self.assertTrue(res['OK'])
    jobsSubmittedList.append(res['Value'])

    res = wrongJob()
    self.assertTrue(res['OK'])
    jobsSubmittedList.append(res['Value'])

    res = booleJobWithConf()
    self.assertTrue(res['OK'])
    jobsSubmittedList.append(res['Value'])


########################################################################################
#
# class monitorSuccess( GridSubmissionTestCase ):
#
#   def test_monitor( self ):
#
#     toRemove = []
#     fail = False
#
#     # we will check every 10 minutes, up to 6 hours
#     counter = 0
#     while counter < 36:
#       jobStatus = self.dirac.status( jobsSubmittedList )
#       self.assertTrue( jobStatus['OK'] )
#       for jobID in jobsSubmittedList:
#         status = jobStatus['Value'][jobID]['Status']
#         minorStatus = jobStatus['Value'][jobID]['MinorStatus']
#         if status == 'Done':
#           self.assertTrue( minorStatus in ['Execution Complete', 'Requests Done'] )
#           jobsSubmittedList.remove( jobID )
#           res = self.dirac.getJobOutputLFNs( jobID )
#           if res['OK']:
#             lfns = res['Value']
#             toRemove += lfns
#         if status in ['Failed', 'Killed', 'Deleted']:
#           fail = True
#           jobsSubmittedList.remove( jobID )
#       if jobsSubmittedList:
#         time.sleep( 600 )
#         counter = counter + 1
#       else:
#         break
#
#     # removing produced files
#     res = self.dirac.removeFile( toRemove )
#     self.assertTrue(res['OK'])
#
#     if fail:
#       self.assertFalse( True )

#############################################################################
# Test Suite run
#############################################################################

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(GridSubmissionTestCase)
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(LHCbsubmitSuccess))
#   suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( monitorSuccess ) )
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)
