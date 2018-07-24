import unittest, json

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

    self.data = {
        "Errors": {
            "ID": {
                "JobID": self.ID1,
                "TransformationID": "TransID_test2",
                "ProductionID": "ProdID_test2"
            },
            "Counter": 2,
            "Error type": "Test error 1",
            "Events": [
                {
                    "runnr": "1",
                    "eventnr": "1"
                },
                {
                    "runnr": "2",
                    "eventnr": "2"
                }
            ]
        }
    }

    self.moreData = {
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
    self.data = json.dumps(self.data)
    self.moreData = json.dumps(self.moreData)

    self.typeName = 'test'
    self.indexName = 'mcstatsdb'

  def setUp(self):
    gLogger.setLevel('DEBUG')
    self.DB = MCStatsElasticDB()

  def tearDown(self):
    self.MCStatsElasticDB = False


class TestMCStatsElasticDB(MCStatsElasticDBTestCase):

  def test_set(self):

    # Test setting data
    result = self.DB.set(self.typeName, self.data)
    self.assertTrue(result['OK'])

    # Test setting data
    result = self.DB.set(self.typeName, self.moreData)
    self.assertTrue(result['OK'])

  #def test_get(self):
    result = self.DB.get(self.ID1)
    # Test get function is OK
    self.assertTrue(result['OK'])
    # Test if get function retrieves correct informatio
    self.assertEqual(result['Value'], self.data)

    result = self.DB.get(self.ID2)
    self.assertTrue(result['OK'])
    self.assertEqual(result['Value'], self.moreData)

    # Test if get function on non-existant data works
    result = self.DB.get(self.falseID)
    self.assertTrue(result['OK'])
    self.assertEqual(result['Value'], '{}')

  # def test_remove(self):
    # Delete result
    result = self.DB.remove(self.ID1)
    self.assertTrue(result['OK'])

    # Make sure we get empty dict after delete
    result = self.DB.get(self.ID1)
    self.assertTrue(result['OK'])
    self.assertEqual(result['Value'], '{}')

    # Make sure we cannot delete again
    result = self.DB.remove(self.ID1)
    self.assertFalse(result['OK'])

    result = self.DB.remove(self.ID2)
    self.assertTrue(result['OK'])

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(MCStatsElasticDBTestCase)
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(TestMCStatsElasticDB))
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)
