import unittest
import datetime
import time


from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()
from DIRAC import gLogger
from LHCbDIRAC.ProductionManagementSystem.DB.MCStatsElasticDB import MCStatsElasticDB
#from DIRAC.Core.Utilities.MCStatsDB import MCStatsDB fix this path

class MCStatsElasticDBTestCase(unittest.TestCase):
	def __init__(self, *args, **kwargs):

		super(MCStatsElasticDBTestCase, self).__init__(*args, **kwargs)

		self.data = {
					    "Errors": {
					            "Counter": 2, 
					            "Error type": "G4Exception", 
					            "Events": [
					                {
					                    "runnr": "", 
					                    "eventnr": ""
					                }, 
					                {
					                    "runnr": "", 
					                    "eventnr": ""
					                }
					            ]
					    }					  
					}

		self.moreData = {
						    "Errors": {
						    	"Counter" : 4,
					            "Error type": "ERROR Gap not found!", 
					            "Events": [
					                {
					                    "runnr": "  Run 7380129", 
					                    "eventnr": "Evt 21990686"
					                }, 
					                {
					                    "runnr": "  Run 7380129", 
					                    "eventnr": "Evt 21990706"
					                }, 
					                {
					                    "runnr": "  Run 7380129", 
					                    "eventnr": "Evt 21990726"
					                }, 
					                {
					                    "runnr": "  Run 7380129", 
					                    "eventnr": "Evt 21990756"
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








						
