"""
This tests the chain
MCStatsElasticDBClient > MCStatsElasticDBHandler > MCStatsElasticDB

It assumes the server is running
"""

import json
import time

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from LHCbDIRAC.ProductionManagementSystem.Client.MCStatsClient import MCStatsClient


id1 = 1
id2 = 2
falseID = 3

data1 = {
    "Errors": {
        "ID": {
            "wmsID": "6",
            "ProductionID": "5",
            "JobID": id1
        },
        "Error1": 10,
        "Error2": 5,
        "Error3": 3
    }
}

data2 = {
    "Errors": {
        "ID": {
            "wmsID": "6",
            "ProductionID": "5",
            "JobID": id2
        },
        "Error1": 7,
        "Error2": 9
    }
}

typeName = 'test'

mcStatsClient = MCStatsClient()
mcStatsClient.indexName = 'lhcb-mclogerrors'


def test_setAndGetandRemove():

  # Set

  # Set data1
  result = mcStatsClient.set(typeName, data1)
  assert result['OK'] is True

  # Set data2
  result = mcStatsClient.set(typeName, data2)
  assert result['OK'] is True

  time.sleep(1)

  # Get data1
  result = mcStatsClient.get(id1)
  assert result['OK'] is True
  assert result['Value'] == data1

  # Get data2
  result = mcStatsClient.get(id2)
  assert result['OK'] is True
  assert result['Value'] == data2

  # Get empty
  result = mcStatsClient.get(falseID)
  assert result['OK'] is True
  assert result['Value'] == '{}'

  # Remove

  # Remove data1
  mcStatsClient.remove(id1)
  time.sleep(3)
  result = mcStatsClient.get(id1)
  assert result['OK'] is True
  assert result['Value'] == '{}'

  # Remove data2
  mcStatsClient.remove(id2)
  time.sleep(3)
  result = mcStatsClient.get(id2)
  assert result['OK'] is True
  assert result['Value'] == '{}'

  # # Remove empty
  mcStatsClient.remove(falseID)
  time.sleep(5)
  result = mcStatsClient.get(falseID)
  assert result['OK'] is True
  assert result['Value'] == '{}'

  result = mcStatsClient.deleteIndex('lhcb-mclogerrors')
  assert result['OK'] is True
