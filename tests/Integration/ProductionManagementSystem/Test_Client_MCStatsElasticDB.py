"""
This tests the chain
MCStatsElasticDBClient > MCStatsElasticDBHandler > MCStatsElasticDB

It assumes the server is running
"""
import unittest
import json
import time

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from LHCbDIRAC.ProductionManagementSystem.Client.MCStatsClient import MCStatsClient


class TestClientMCStatsTestCase(unittest.TestCase):
  def setUp(self):
    self.id1 = 1
    self.id2 = 2
    self.falseID = 3

    self.data1 = {
        "Errors": {
            "ID": {
                "wmsID": "6",
                "ProductionID": "5",
                "JobID": self.id1
            },
            "Error1": 10,
            "Error2": 5,
            "Error3": 3
        }
    }

    self.data2 = {
        "Errors": {
            "ID": {
                "wmsID": "6",
                "ProductionID": "5",
                "JobID": self.id2
            },
            "Error1": 7,
            "Error2": 9
        }
    }

    # This is needed to convert '' to ""
    self.data1 = json.dumps(self.data1)
    self.data2 = json.dumps(self.data2)

    self.typeName = 'test'
    self.indexName = 'mcstatsdb'

    # Note: index is created without self.indexName
    self.mcStatsClient = MCStatsClient()

  def tearDown(self):
    self.mcStatsClient.deleteIndex(self.indexName)
    self.mcStatsClient = None


class MCHandlerClientChain(TestClientMCStatsTestCase):
  def test_setAndGetandRemove(self):

    # Set

    # Set data1
    result = self.mcStatsClient.set(self.indexName, self.typeName, self.data1)
    self.assertTrue(result['OK'])

    # Set data2
    result = self.mcStatsClient.set(self.indexName, self.typeName, self.data2)
    self.assertTrue(result['OK'])

    time.sleep(1)

    # Get data1
    result = self.mcStatsClient.get(self.indexName, self.id1)
    self.assertTrue(result['OK'])
    self.assertEqual(result['Value'], self.data1)

    # Get data2
    result = self.mcStatsClient.get(self.indexName, self.id2)
    self.assertTrue(result['OK'])
    self.assertEqual(result['Value'], self.data2)

    # Get empty
    result = self.mcStatsClient.get(self.indexName, self.falseID)
    self.assertTrue(result['OK'])
    self.assertEqual(result['Value'], '{}')

    # Remove

    # Remove data1
    self.mcStatsClient.remove(self.indexName, self.id1)
    time.sleep(3)
    result = self.mcStatsClient.get(self.indexName, self.id1)
    self.assertTrue(result['OK'])
    self.assertEqual(result['Value'], '{}')

    # Remove data2
    self.mcStatsClient.remove(self.indexName, self.id2)
    time.sleep(3)
    result = self.mcStatsClient.get(self.indexName, self.id2)
    self.assertTrue(result['OK'])
    self.assertEqual(result['Value'], '{}')

    # # Remove empty
    self.mcStatsClient.remove(self.indexName, self.falseID)
    time.sleep(5)
    result = self.mcStatsClient.get(self.indexName, self.falseID)
    self.assertTrue(result['OK'])
    self.assertEqual(result['Value'], '{}')


if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(TestClientMCStatsTestCase)
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(MCHandlerClientChain))
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)
