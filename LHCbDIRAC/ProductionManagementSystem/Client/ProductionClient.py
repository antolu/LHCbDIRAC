""" Client class to access the production manager service
"""
# $Id: ProductionClient.py,v 1.1 2009/09/08 12:50:39 acsmith Exp $
__RCSID__ = "$Revision: 1.1 $"

from DIRAC  import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.Utilities.List import breakListIntoChunks
from DIRAC.Core.DISET.RPCClient import RPCClient
import types

class ProductionClient:

  def __init__(self):
    pass

  def getProductionsWithStatus(self,status):
    server = RPCClient('ProductionManagement/ProductionManager',timeout=120)
    return server.getTransformationWithStatus(status)

  def getAllProductions(self):
    server = RPCClient('ProductionManagement/ProductionManager',timeout=120)
    return server.getAllProductions()
