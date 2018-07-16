import threading # Do i need this??

from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from LHCbDIRAC.ProductionManagementSystem.DB.MCStatsElasticDB import MCStatsElasticDB

__RCSID__ = "$Id$"

MCStatsElasticDB = False

def initializeMCStatsHandler(self):
  global MCStatsElasticDB
  MCStatsElasticDB = MCStatsElasticDB()
  return S_OK()

class MCStatsElasticDBHandler(RequestHandler):
  def __init__(self, *args, **kargs):
    RequestHandler.__init__(self, *args, **kargs)

    self.DB = MCStatsElasticDB
    self.lock = threading.lock() # What is this??

  types_set = [ dict ]
  def export_set(self, typeName, data):

    gLogger.notice('Called set() with typeName = %s' % typeName)
    return self.DB.set(typeName, data)

  types_get = [ dict ]
  def export_get(self):

    gLogger.notice('Called get()' )
    return self.DB.get()

  types_remove = [ dict ]
  def export_remove(self):

    gLogger.notice('Called remove()' )
    return self.DB.remove()