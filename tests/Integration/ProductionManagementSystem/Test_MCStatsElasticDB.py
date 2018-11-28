"""
Tests set(), get() and remove() from MCStatsElasticDB
"""

import time

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from DIRAC import gLogger
from LHCbDIRAC.ProductionManagementSystem.DB.MCStatsElasticDB import MCStatsElasticDB


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
indexName1 = 'mcstatsdb1'
indexName2 = 'mcstatsdb2'

gLogger.setLevel('DEBUG')
db = MCStatsElasticDB()


def test_setandGetandRemove():

  # Set

  # Set data1
  result = db.set(indexName1, typeName, data1)
  time.sleep(1)
  assert result['OK'] is True
  # Set data2
  result = db.set(indexName1, typeName, data2)
  time.sleep(1)
  assert result['OK'] is True

  # Data insertion is not instantaneous, so sleep is needed
  time.sleep(1)

  # Get

  # Get data1 from index1
  result = db.get(indexName1, id1)
  assert result['OK'] is True
  assert result['Value'] == data1

  # Get data2 from index1
  result = db.get(indexName1, id2)
  assert result['OK'] is True
  assert result['Value'] == data2

  # Get data1 from index2 (false)
  result = db.get(indexName2, id1)
  assert result['OK'] is True
  assert result['Value'] == '{}'

  # Get empty
  result = db.get(indexName1, falseID)
  assert result['OK'] is True
  assert result['Value'] == '{}'

  # Remove

  # Remove data1 from index1
  db.remove(indexName1, id1)
  time.sleep(1)
  result = db.get(indexName1, id1)
  assert result['OK'] is True
  assert result['Value'] == '{}'

  # Remove data2 from index1
  db.remove(indexName1, id2)
  time.sleep(1)
  result = db.get(indexName1, id2)
  assert result['OK'] is True
  assert result['Value'] == '{}'

  # Remove empty
  db.remove(indexName1, falseID)
  time.sleep(1)
  result = db.get(indexName1, falseID)
  assert result['OK'] is True
  assert result['Value'] == '{}'

  # Remove the index
  result = db.deleteIndex(indexName1)
  assert result['OK'] is True
