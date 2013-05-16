''' Test_BKK_DB_OracleBookkeepingDB
'''

import mock
import unittest

import LHCbDIRAC.BookkeepingSystem.DB.OracleBookkeepingDB as moduleTested

__RCSID__ = ''

################################################################################

class OracleBookkeepingDB_TestCase(unittest.TestCase):
  """
  """
  def setUp(self):
    '''
    Setup
    '''

    self.moduleTested = moduleTested
    self.testClass = self.moduleTested.OracleBookkeepingDB()

  def tearDown(self):
    '''
    Tear down
    '''

    del self.moduleTested
    del self.testClass

################################################################################

class OracleBookkeepingDB_Success(OracleBookkeepingDB_TestCase):
  """
  """
  def test_instantiate(self):
    ''' tests that we can instantiate one object of the tested class
    '''

    module = self.testClass
    self.assertEqual('OracleBookkeepingDB', module.__class__.__name__)

  def test_buildRunNumbers(self):
    """It test the method which used to build the conditions when runnumbers is a
    list/number, and end run and start run is a number
    """
    runnumbers = [1, 3, 4]
    startRunID = None
    endRunID = None
    condition = ''
    tables = ''
    retVal = self.testClass._OracleBookkeepingDB__buildRunnumbers(runnumbers, startRunID, endRunID, condition, tables)
    self.assert_(retVal['OK'])
    self.assert_(retVal['Value'])
    self.assertEqual(retVal['Value'], (' and  (  j.runnumber=1 or  j.runnumber=3 or  j.runnumber=4 ) ', ''))

    startRunID = 1
    retVal = self.testClass._OracleBookkeepingDB__buildRunnumbers(runnumbers, startRunID, endRunID, condition, tables)
    self.assert_(retVal['OK'])
    self.assert_(retVal['Value'])
    self.assertEqual((' and  (  j.runnumber=1 or  j.runnumber=3 or  j.runnumber=4 ) ', ''), retVal['Value'])

    startRunID = None
    endRunID = 1
    retVal = self.testClass._OracleBookkeepingDB__buildRunnumbers(runnumbers, startRunID, endRunID, condition, tables)
    self.assert_(retVal['OK'])
    self.assert_(retVal['Value'])
    self.assertEqual((' and  (  j.runnumber=1 or  j.runnumber=3 or  j.runnumber=4 ) ', ''), retVal['Value'])

    startRunID = 1
    endRunID = 2
    runnumbers = [33, 44]
    retVal = self.testClass._OracleBookkeepingDB__buildRunnumbers(runnumbers, startRunID, endRunID, condition, tables)
    self.assert_(retVal['OK'])
    self.assert_(retVal['Value'])
    self.assertEqual((' and (j.runnumber>=1 and j.runnumber<=2 or  (  j.runnumber=33 or  j.runnumber=44 ))', ''),
                     retVal['Value'])

    for i in [[], None]:
      runnumbers = i
      startRunID = 1
      endRunID = 2
      retVal = self.testClass._OracleBookkeepingDB__buildRunnumbers(runnumbers, startRunID, endRunID, condition, tables)
      self.assert_(retVal['OK'])
      self.assert_(retVal['Value'])
      self.assertEqual((' and j.runnumber>=1 and j.runnumber<=2', ''), retVal['Value'])


