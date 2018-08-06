"""
A database wrapper for ElasticDB
"""
import json
from elasticsearch import Elasticsearch
from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Core.Base.ElasticDB import ElasticDB as DB
ES = Elasticsearch()
# import os


class MCStatsElasticDB(DB):
  def __init__(self, indexName='mcstatsdb'):
    DB.__init__(self, 'MCStatsDB', 'ProductionManagement/MCStatsDB')
    self.indexName = indexName
    # self.typeName = 'LogErr'    # We assume the type of the data is from LogErr
    self.mapping = {
        "Log_output": {
            "properties": {
                "ID": {
                    "properties": {
                        "JobID": {"type": "text"},
                        "TransformationID": {"type": "text"},
                        "ProductionID": {"type": "text"}
                    }
                },
                "Errors": {
                    "properties": {
                        "Counter": {"type": "integer"},
                        "Error type": {"type": "text"},
                        "Events": {
                            "properties": {
                                "runnr": {"type": "text"},
                                "eventnr": {"type": "text"}
                            }
                        }
                    }
                }
            }
        }  
    }


    createIndex = self.createIndex(self.indexName, self.mapping, None)
    if not createIndex['OK']:
      gLogger.error("ERROR: Couldn't create index")
    else:
      gLogger.notice("Creating index: ", self.indexName)

#############################################################################

    # Not sure if this function is needed, but I'll keep it for the time being

    # def setFromFile(self, typeName, jsonName = 'errors.json'):
    #   """
    #   Inserts data into specified index using the data from a json file.

    #   :param str typeName: The type in the index in ElasticSearch
    #   :param str jsonName: The json file name

    #   """
    #   data = ''
    #   if os.path.exists(jsonName):
    #     with open(jsonName, 'r') as f: # file must be in same directory...
    #       data = f.read()
    #     gLogger.notice('Creating data in index ', self.indexName)

    #   else:
    #     gLogger.error("ERROR: Couldn't find file")
    #     return S_ERROR()

    #   self.index(self.indexName, typeName, data)

#############################################################################

  def set(self, typeName, data):
    """
    Inserts data into specified index using data given in argument

    :param str typeName: The type in the index in ElasticSearch
    :param dict data: The data to be inserted in ElasticSearch in JSON format
    """
    result = self.index(self.indexName, typeName, data)
    if self.exists(self.indexName) and result['OK']:
      gLogger.notice('Inserting data in index:', self.indexName)
    else:
      gLogger.error("ERROR: Couldn't insert data")
    return result

#############################################################################

  def get(self, jobID):
    """
    Retrieves data given a specific JobID

    :param str JobID: The JobID Of the data in elasticsearch
    """

    query = {
        "query": {
            "bool": {
                "must": {
                    "match": {
                        "Log_output.ID.JobID": jobID
                    }
                }
            }
        }
    }

    gLogger.notice('Getting results for JobID: ', jobID)
    result = self.query('mcstatsdb*', query)

    resultDict = {}
    if not result['OK']:
      return S_ERROR(result)

    sources = result['Value']['hits']['hits']
    for source in sources:
      data = source['_source']
      resultDict.update(data)
    resultDict = json.dumps(resultDict)
    return S_OK(resultDict)
#############################################################################

  def remove(self, jobID):
    """
    Removes data given a specific JobID

    :param str JobID: The JobID Of the data in elasticsearch
    """
    query = {
        "query": {
            "bool": {
                "must": {
                    "match": {
                        "Log_output.ID.JobID": jobID
                    }
                }
            }
        }
    }

    gLogger.notice('Attempting to delete data with JobID: ', jobID)
    try:
      ES.delete_by_query(index = self.indexName, body=query)
    except Exception as inst:
      gLogger.error("ERROR: Couldn't delete data")
      return S_ERROR(inst)
    return S_OK('Successfully deleted data with JobID: %s' % jobID)
