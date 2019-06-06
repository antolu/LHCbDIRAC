###############################################################################
# (c) Copyright 2019 CERN for the benefit of the LHCb Collaboration           #
#                                                                             #
# This software is distributed under the terms of the GNU General Public      #
# Licence version 3 (GPL Version 3), copied verbatim in the file "LICENSE".   #
#                                                                             #
# In applying this licence, CERN does not waive the privileges and immunities #
# granted to it by virtue of its status as an Intergovernmental Organization  #
# or submit itself to any jurisdiction.                                       #
###############################################################################
"""
A database wrapper for ElasticDB to insert data into elasticsearch from Gauss & Boole simulations
"""

from DIRAC import S_OK
from DIRAC.Core.Base.ElasticDB import ElasticDB


class MCStatsElasticDB(ElasticDB):
  """ Exposes interface to Elastic DB index lhcb-mcstatsdb
  """

  def __init__(self):
    """ Simple constructor, just initialize MCStatsElasticDB
    """

    super(MCStatsElasticDB, self).__init__('MCStatsElasticDB', 'ProductionManagement/MCStatsElasticDB')

    # self.typeName = 'LogErr'    # We assume the type of the data is from LogErr
    # self.mapping = {
    #     "Log_output": {
    #         "properties": {
    #             "ID": {
    #                 "properties": {
    #                     "wmsID": {"type": "integer"},
    #                     "JobID": {"type": "text"},
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

  def set(self, indexName, typeName, data):
    """
    Inserts data into specified index using data given in argument

    :param str indexName: the name of the index in ELasticSearch
    :param str typeName: The type in the index in ElasticSearch
    :param dict data: The data to be inserted in ElasticSearch in JSON format

    :returns: S_OK/S_ERROR as result of indexing
    """
    self.log.debug('Inserting data in index %s' % indexName)
    result = self.index(indexName, typeName, data)
    if not result['OK']:
      self.log.error("ERROR: Couldn't insert data", result['Message'])
    return result

  def get(self, indexName, jobID):
    """
    Retrieves data given a specific WMS JobID

    :param str indexName: the name of the index in ELasticSearch
    :param int JobID: The WMS JobID of the data in elasticsearch

    :returns: S_OK/S_ERROR
    """

    query = {
        "query": {
            "bool": {
                "must": {
                    "match": {
                        "Errors.ID.wmsID": jobID
                    }
                }
            }
        }
    }

    self.log.debug('Getting results for JobID %s in index %s' % (jobID, indexName))
    result = self.query(indexName + '*', query)

    if not result['OK']:
      return result

    resultDict = {}
    sources = result['Value']['hits']['hits']
    for source in sources:
      data = source['_source']
      resultDict.update(data)
    return S_OK(resultDict)

  def remove(self, indexName, jobID):
    """
    Removes data given a specific WMS JobID

    :param str indexName: the name of the index in ELasticSearch
    :param int JobID: The JobID of the data in elasticsearch
    """
    query = {
        "query": {
            "bool": {
                "must": {
                    "match": {
                        "Errors.ID.wmsID": jobID
                    }
                }
            }
        }
    }

    self.log.debug('Attempting to delete data with JobID: %s in index %s' % (jobID, indexName))
    return self.deleteByQuery(indexName, query)
