''' This is a test of the chain
    ResourceManagementClient -> ResourceManagementHandler -> ResourceManagementDB
    It supposes that the DB is present, and that the service is running

    this is pytest!
'''

import datetime
from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from DIRAC import gLogger
from DIRAC.tests.Integration.ResourceStatusSystem.Test_ResourceManagement import test_addAndRemove as ResourceManagementClientChain
from LHCbDIRAC.ResourceStatusSystem.Client.ResourceManagementClient import ResourceManagementClient

gLogger.setLevel('DEBUG')

ResourceManagementClientChain()

rsClient = ResourceManagementClient()

Datetime = datetime.datetime.now()

def test_addAndRemove():



  # TEST addOrModifyMonitoringTest
  # ...............................................................................

  res = rsClient.addOrModifyMonitoringTest('TestName1234', 'serviceURI', 'siteName', 'serviceFlavour',
                                         'metricStatus', 'summaryData', Datetime, Datetime)
  assert res['OK'] == True

  res = rsClient.selectMonitoringTest('TestName1234')
  assert res['OK'] == True
  #check if the name that we got is equal to the previously added 'TestName1234'
  assert res['Value'][0][7] == 'TestName1234'


  # TEST deleteMonitoringTest
  # ...............................................................................

  res = rsClient.deleteMonitoringTest('TestName1234')
  assert res['OK'] == True

  res = rsClient.selectMonitoringTest('TestName1234')
  assert res['OK'] == True
  assert not res['Value']


  # TEST addOrModifyJobAccountingCache
  # ...............................................................................

  res = rsClient.addOrModifyJobAccountingCache('TestName1234')

  assert res['OK'] == True

  res = rsClient.selectJobAccountingCache('TestName1234')
  assert res['OK'] == True
  #check if the name that we got is equal to the previously added 'TestName1234'
  assert res['Value'][0][3] == 'TestName1234'


  # TEST deleteJobAccountingCache
  # ...............................................................................

  res = rsClient.deleteJobAccountingCache('TestName1234')
  assert res['OK'] == True

  res = rsClient.selectJobAccountingCache('TestName1234')
  assert res['OK'] == True
  assert not res['Value']



  # TEST addOrModifyPilotAccountingCache
  # ...............................................................................

  res = rsClient.addOrModifyPilotAccountingCache('TestName1234')
  assert res['OK'] == True

  res = rsClient.selectPilotAccountingCache('TestName1234')
  assert res['OK'] == True
  #check if the name that we got is equal to the previously added 'TestName1234'
  assert res['Value'][0][0] == 'TestName1234'


  # TEST deletePilotAccountingCache
  # ...............................................................................

  res = rsClient.deletePilotAccountingCache('TestName1234')
  assert res['OK'] == True

  res = rsClient.selectPilotAccountingCache('TestName1234')
  assert res['OK'] == True
  assert not res['Value']


  # TEST addOrModifyEnvironmentCache
  # ...............................................................................

  res = rsClient.addOrModifyEnvironmentCache('TestName1234', 'environment', 'siteName',
                                           'arguments', Datetime, Datetime)
  assert res['OK'] == True

  res = rsClient.selectEnvironmentCache('TestName1234')
  assert res['OK'] == True
  #check if the name that we got is equal to the previously added 'TestName1234'
  assert res['Value'][0][4] == 'TestName1234'


  # TEST deleteEnvironmentCache
  # ...............................................................................

  res = rsClient.deleteEnvironmentCache('TestName1234')
  assert res['OK'] == True

  res = rsClient.selectEnvironmentCache('TestName1234')
  assert res['OK'] == True
  assert not res['Value']

# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
