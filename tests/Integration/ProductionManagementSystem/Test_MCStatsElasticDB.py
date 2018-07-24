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

    self.ID1 = 1
    self.ID2 = 2
    self.falseID = 3

    self.data1 = {
        "Errors": {
            "ID": {
                "JobID": self.ID1,
                "TransformationID": "TransID_test1",
                "ProductionID": "ProdID_test1"
            },
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

    self.data2 = {
        "Errors":
        {
            "ID": {
                "JobID": self.ID2,
                "TransformationID": "TransID_test2",
                "ProductionID": "ProdID_test2"
            },
            "Counter": 4,
            "Error type": "Test error 2",
            "Events":
            [
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
        }
    }

    # This is needed to convert '' to ""
    self.data1 = json.dumps(self.data1)
    self.data2 = json.dumps(self.data2)

    self.typeName = 'test'
    self.indexName = 'mcstatsdb'

  def setUp(self):
    gLogger.setLevel('DEBUG')
    self.DB = MCStatsElasticDB()

  def tearDown(self):
    self.DB.deleteIndex(self.indexName)
    self.MCStatsElasticDB = None


class TestMCStatsElasticDB(MCStatsElasticDBTestCase):

  def test_SetandGetandRemove(self):

    ############ Set

    # Set data1
    result = self.DB.set(self.typeName, self.data1)
    self.assertTrue(result['OK'])

    # Set data2
    result = self.DB.set(self.typeName, self.data2)
    self.assertTrue(result['OK'])

    # Data insertion is not instantaneous, so sleep is needed
    time.sleep(1)

    ############ Get

    # Get data1
    result = self.DB.get(self.ID1)
    self.assertTrue(result['OK'])
    self.assertEqual(result['Value'], self.data1)

    # Get data2
    result = self.DB.get(self.ID2)
    self.assertTrue(result['OK'])
    self.assertEqual(result['Value'], self.data2)

    # Get empty 
    result = self.DB.get(self.falseID)
    self.assertTrue(result['OK'])
    self.assertEqual(result['Value'], '{}')

    ############ Remove

    # Remove data1
    self.DB.remove(self.ID1)
    time.sleep(1)
    result = self.DB.get(self.ID1)
    self.assertTrue(result['OK'])
    self.assertEqual(result['Value'], '{}')

    # Remove data2
    self.DB.remove(self.ID2)
    time.sleep(1)
    result = self.DB.get(self.ID2)
    self.assertTrue(result['OK'])
    self.assertEqual(result['Value'], '{}')

    # Remove empty
    self.DB.remove(self.falseID)
    time.sleep(1)
    result = self.DB.get(self.falseID)
    self.assertTrue(result['OK'])
    self.assertEqual(result['Value'], '{}')

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(MCStatsElasticDBTestCase)
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(TestMCStatsElasticDB))
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)
