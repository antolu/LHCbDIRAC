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
        "Log_output": {
            "ID": {
                "JobID": self.id1,
                "TransformationID": "TransID_test1",
                "ProductionID": "ProdID_test1"
            },
            "Errors": {
                "Counter": 2,
                "Error type": "Test error 1",
                "Events": [
                    {
                        "runnr": "Run 1",
                        "eventnr": "Evt 1"
                    },
                    {
                        "runnr": "Run 2",
                        "eventnr": "Evt 2"
                    }
                ]
            }
        }
    }

    self.data2 = {
        "Log_output": {
            "ID": {
                "JobID": self.id2,
                "TransformationID": "TransID_test2",
                "ProductionID": "ProdID_test2"
            },
            "Errors": [
                {
                    "Counter": 4,
                    "Error type": "Test error 1",
                    "Events": [
                        {
                            "runnr": "Run 1",
                            "eventnr": "Evt 1"
                        },
                        {
                            "runnr": "Run 2",
                            "eventnr": "Evt 2"
                        },
                        {
                            "runnr": "Run 3",
                            "eventnr": "Evt 3"
                        },
                        {
                            "runnr": "Run 4",
                            "eventnr": "Evt 4"
                        }
                    ]
                },
                {
                    "Counter": 2,
                    "Error type": "Test error 2",
                    "Events": [
                        {
                            "runnr": "Run 1",
                            "eventnr": "Evt 1"
                        },
                        {
                            "runnr": "Run 2",
                            "eventnr": "Evt 2"
                        }
                    ]
                }
            ]
        }
    }

    # This is needed to convert '' to ""
    self.data1 = json.dumps(self.data1)
    self.data2 = json.dumps(self.data2)

    self.typeName = 'test-client'
    self.indexName = 'mcstatsdb-client'

    # Note: index is created without self.indexName
    self.mcStatsClient = MCStatsClient()

  def tearDown(self):
    pass


class MCHandlerClientChain(TestClientMCStatsTestCase):
  def test_setAndGetandRemove(self):

    # Set

    # Set data1
    result = self.mcStatsClient.set(self.typeName, self.data1)
    self.assertTrue(result['OK'])

    # Set data2
    result = self.mcStatsClient.set(self.typeName, self.data2)
    self.assertTrue(result['OK'])

    time.sleep(5)

    # Get data1
    result = self.mcStatsClient.get(self.id1)
    self.assertTrue(result['OK'])
    self.assertEqual(result['Value'], self.data1)

    # Get data2
    result = self.mcStatsClient.get(self.id2)
    self.assertTrue(result['OK'])
    self.assertEqual(result['Value'], self.data2)

    # Get empty
    result = self.mcStatsClient.get(self.falseID)
    self.assertTrue(result['OK'])
    self.assertEqual(result['Value'], '{}')

    # Remove

    # Remove data1
    self.mcStatsClient.remove(self.id1)
    time.sleep(3)
    result = self.mcStatsClient.get(self.id1)
    self.assertTrue(result['OK'])
    self.assertEqual(result['Value'], '{}')

    # Remove data2
    self.mcStatsClient.remove(self.id2)
    time.sleep(3)
    result = self.mcStatsClient.get(self.id2)
    self.assertTrue(result['OK'])
    self.assertEqual(result['Value'], '{}')

    # # Remove empty
    self.mcStatsClient.remove(self.falseID)
    time.sleep(5)
    result = self.mcStatsClient.get(self.falseID)
    self.assertTrue(result['OK'])
    self.assertEqual(result['Value'], '{}')


if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(TestClientMCStatsTestCase)
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(MCHandlerClientChain))
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)
