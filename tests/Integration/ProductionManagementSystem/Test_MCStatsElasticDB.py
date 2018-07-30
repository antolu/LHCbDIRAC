"""
Tests set(), get() and remove() from MCStatsElasticDB
"""

import time
import json
import unittest

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()
from DIRAC import gLogger
from LHCbDIRAC.ProductionManagementSystem.DB.MCStatsElasticDB import MCStatsElasticDB


class MCStatsElasticDBTestCase(unittest.TestCase):
  def __init__(self, *args, **kwargs):
    super(MCStatsElasticDBTestCase, self).__init__(*args, **kwargs)

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

    self.typeName = 'test'
    self.indexName = 'mcstatsdb'

  def setUp(self):
    gLogger.setLevel('DEBUG')
    self.db = MCStatsElasticDB()

  def tearDown(self):
    self.db.deleteIndex(self.indexName)
    self.db = None


class TestMCStatsElasticDB(MCStatsElasticDBTestCase):

  def test_setandGetandRemove(self):

    # Set

    # Set data1
    result = self.db.set(self.typeName, self.data1)
    self.assertTrue(result['OK'])

    # Set data2
    result = self.db.set(self.typeName, self.data2)
    self.assertTrue(result['OK'])

    # Data insertion is not instantaneous, so sleep is needed
    time.sleep(5)

    # Get

    # Get data1
    result = self.db.get(self.id1)
    self.assertTrue(result['OK'])
    self.assertEqual(result['Value'], self.data1)

    # Get data2
    result = self.db.get(self.id2)
    self.assertTrue(result['OK'])
    self.assertEqual(result['Value'], self.data2)

    # Get empty
    result = self.db.get(self.falseID)
    self.assertTrue(result['OK'])
    self.assertEqual(result['Value'], '{}')

    # Remove

    # Remove data1
    self.db.remove(self.id1)
    time.sleep(5)
    result = self.db.get(self.id1)
    self.assertTrue(result['OK'])
    self.assertEqual(result['Value'], '{}')

    # Remove data2
    self.db.remove(self.id2)
    time.sleep(5)
    result = self.db.get(self.id2)
    self.assertTrue(result['OK'])
    self.assertEqual(result['Value'], '{}')

    # Remove empty
    self.db.remove(self.falseID)
    time.sleep(5)
    result = self.db.get(self.falseID)
    self.assertTrue(result['OK'])
    self.assertEqual(result['Value'], '{}')


if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(MCStatsElasticDBTestCase)
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(TestMCStatsElasticDB))
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)
