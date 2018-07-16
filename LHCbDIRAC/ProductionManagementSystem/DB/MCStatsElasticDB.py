"""
A database wrapper for ElasticDB
author: Lara Sheik
version: 10/07/18
"""
from DIRAC.Core.Base.ElasticDB import ElasticDB as DB
from DIRAC import gLogger, S_OK, S_ERROR # Do I need these? Aren't these included in EDB?
import os

class MCStatsElasticDB(DB):
	def __init__(self):
		#super(MCStatsDB, self).__init__(self, 'MCStatsDB', 'WorkloadManagement/MCStatsDB') # How do I initialise this?	
		DB.__init__(self, 'MCStatsDB', 'WorkloadManagement/MCStatsDB')
		self.indexName = 'mcstatsdb'
		#self.docName = 'LogErr'		# We assume the type of the data is from LogErr
		self.mapping = {
			'Errors' : {
				'properties' : {
					'Counter' : {'type' : 'integer'},
					'Error type' : {'type' : 'text'},
					'Events' : {
						'properties' : {
							'runnr' : {'type' : 'text'},
							'eventnr' : {'type' : 'text'}
						}
					}
				}
			}
		}

		createIndex = self.createIndex(self.indexName, self.mapping, None)
		if not createIndex['OK']:
			gLogger.error("ERROR: Couldn't create index")
		else:
			gLogger.notice("Creating index: ",self.indexName)

#############################################################################

	def setFromFile(self, typeName, jsonName = 'errors2.json'):
		'Inserts data into specified index using the data from a json file'
		data = ''
		if os.path.exists(jsonName):
			with open(jsonName, 'r') as f: # file must be in same directory...
				data = f.read()
			gLogger.notice('Creating data in index ', self.indexName)

		else:
			gLogger.error("ERROR: Couldn't find file")
			return S_ERROR()

		self.index(self.indexName, typeName, data)

	def set(self, typeName, data):
		'Inserts data into specified index using data given in argument'
		result = self.index(self.indexName, typeName, data)
		if self.exists(self.indexName) and result['OK']:
			gLogger.notice('Inserting data in index', self.indexName)
		else:
			gLogger.error("ERROR: Couldn't find file")
		return result

#############################################################################

	def get(self, indexName):

		pass


#############################################################################

	def remove(self, indexName):

		pass
