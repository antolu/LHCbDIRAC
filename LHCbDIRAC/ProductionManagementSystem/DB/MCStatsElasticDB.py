"""
A database wrapper for ElasticDB to insert data into elasticsearch from Gauss & Boole simulations
"""
import json
from elasticsearch import Elasticsearch
from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Core.Base.ElasticDB import ElasticDB as DB
ES = Elasticsearch()


class MCStatsElasticDB(DB):
  def __init__(self, indexName='mcstatsdb'):
    DB.__init__(self, 'MCStatsDB', 'ProductionManagement/MCStatsDB')
    # self.typeName = 'LogErr'    # We assume the type of the data is from LogErr
    # self.mapping = {
    #     "Log_output": {
    #         "properties": {
    #             "ID": {
    #                 "properties": {
    #                     "JobID": {"type": "text"},
    #                     "TransformationID": {"type": "text"},
    #                     "ProductionID": {"type": "text"}
    #                 }
    #             },
    #             "Errors": {
    #                 "properties": {
    #                     "Counter": {"type": "integer"},
    #                     "Error type": {"type": "text"},
    #                     "Events": {
    #                         "properties": {
    #                             "runnr": {"type": "text"},
    #                             "eventnr": {"type": "text"}
    #                         }
    #                     }
    #                 }
    #             }
    #         }
    #     }
    # }

#############################################################################

  def set(self, indexName, typeName, data):
    """
    Inserts data into specified index using data given in argument

    :param str indexName: the name of the index in ELasticSearch
    :param str typeName: The type in the index in ElasticSearch
    :param dict data: The data to be inserted in ElasticSearch in JSON format

    :returns: S_OK/S_ERROR as result of indexing
    """
    result = self.index(indexName, typeName, data)
    if self.exists(indexName) and result['OK']:
      gLogger.notice('Inserting data in index:', indexName)
    else:
      gLogger.error("ERROR: Couldn't insert data", result['Message'])
    return result

#############################################################################

  def get(self, indexName, jobID):
    """
    Retrieves data given a specific JobID

    :param str indexName: the name of the index in ELasticSearch
    :param str JobID: The JobID Of the data in elasticsearch

    :returns: S_OK/S_ERROR
    """

    query = {
        "query": {
            "bool": {
                "must": {
                    "match": {
                        "Errors.ID.JobID": jobID
                    }
                }
            }
        }
    }

    gLogger.notice('Getting results for JobID %s in index %s' % (jobID, indexName))
    result = self.query(indexName + '*', query)

    if not result['OK']:
      return S_ERROR(result)

    resultDict = {}
    sources = result['Value']['hits']['hits']
    for source in sources:
      data = source['_source']
      resultDict.update(data)
    return S_OK(json.dumps(resultDict))

#############################################################################

  def remove(self, indexName, jobID):
    """
    Removes data given a specific JobID

    :param str indexName: the name of the index in ELasticSearch
    :param str JobID: The JobID Of the data in elasticsearch
    """
    query = {
        "query": {
            "bool": {
                "must": {
                    "match": {
                        "Errors.ID.JobID": jobID
                    }
                }
            }
        }
    }

    gLogger.notice('Attempting to delete data with JobID: %s in index %s' % (jobID, indexName))
    return self.deleteByQuery(indexName, query)
