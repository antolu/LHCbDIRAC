"""
Handler for MCStatsElasticDB
"""

from DIRAC import gLogger, S_OK
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from LHCbDIRAC.ProductionManagementSystem.DB.MCStatsElasticDB import MCStatsElasticDB

__RCSID__ = "$Id$"

#global mcStatsDB
mcStatsDB = False

def initializeMCStatsElasticDBHandler(_serviceinfo):
  global mcStatsDB
  mcStatsDB = MCStatsElasticDB()
  return S_OK()

class MCStatsElasticDBHandler(RequestHandler):
  def __init__(self, *args, **kargs):
    RequestHandler.__init__(self, *args, **kargs)

  types_set = [ basestring, basestring ]
  def export_set(self, typeName, data):

    gLogger.notice('Called set() with typeName = %s' %typeName)
    return mcStatsDB.set(typeName, data)

  types_get = [ int ]
  def export_get(self, jobID):

    gLogger.notice('Called get() with jobID = %s' %jobID)
    return mcStatsDB.get(jobID)

  types_remove = [ int ]
  def export_remove(self, jobID):

    gLogger.notice('Called remove() with jobID = %s' %jobID)
    return mcStatsDB.remove(jobID)
