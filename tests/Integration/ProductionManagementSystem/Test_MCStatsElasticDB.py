import unittest
import datetime
import time


from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()
from DIRAC import gLogger
from LHCbDIRAC.ProductionManagementSystem.DB.MCStatsElasticDB import MCStatsElasticDB

class MCStatsElasticDBTestCase(unittest.TestCase):
  def __init__(self, *args, **kwargs):

    super(MCStatsElasticDBTestCase, self).__init__(*args, **kwargs)

    self.data = {
              "Errors": {
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
                "Errors": {
                  "Counter" : 4,
                      "Error type": "Test error 2", 
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
                }
            }

    self.typeName = 'test'
    self.indexName = 'mcstatsdb'

  def setUp(self):
    gLogger.setLevel('DEBUG')
    self.DB = MCStatsElasticDB()

  def tearDown(self):
    self.MCStatsElasticDB = False

class TestMCStatsElasticDB(MCStatsElasticDBTestCase):

  def test_set(self):
    result = self.DB.set( self.typeName, self.data)
    self.assertTrue(result['OK'])
    result = self.DB.set( self.typeName, self.moreData)
    self.assertTrue(result['OK'])

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(MCStatsElasticDBTestCase)
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TestMCStatsElasticDB ) )
  testResult = unittest.TextTestRunner(verbosity = 2).run(suite)
