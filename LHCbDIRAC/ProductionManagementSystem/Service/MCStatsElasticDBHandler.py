""" DIRAC service that expose access for MCStatsElasticDB (ElasticSearch DB)
"""

__RCSID__ = "$Id$"

from DIRAC import gLogger, S_OK
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from LHCbDIRAC.ProductionManagementSystem.DB.MCStatsElasticDB import MCStatsElasticDB


def initializeMCStatsElasticDBHandler(_serviceinfo):
  global mcStatsDB
  mcStatsDB = MCStatsElasticDB()
  return S_OK()


class MCStatsElasticDBHandler(RequestHandler):
  """ Tiny service for setting/getting/removing data from ElasticSearch MCStats DB
  """

  types_set = [basestring, basestring, basestring]

  def export_set(self, indexName, typeName, data):

    gLogger.notice('Called set() with typeName = %s' % typeName)
    return mcStatsDB.set(indexName, typeName, data)

  types_get = [basestring, int]

  def export_get(self, indexName, jobID):

    gLogger.notice('Called get() with jobID = %s' % jobID)
    return mcStatsDB.get(indexName, jobID)

  types_remove = [basestring, int]

  def export_remove(self, indexName, jobID):

    gLogger.notice('Called remove() with jobID = %s' % jobID)
    return mcStatsDB.remove(indexName, jobID)
