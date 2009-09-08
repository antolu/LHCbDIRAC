""" Client class to access the production manager service
"""
# $Id: ProductionClient.py,v 1.2 2009/09/08 13:04:14 acsmith Exp $
__RCSID__ = "$Revision: 1.2 $"

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

  def getParameters(self,prodID,pname=''):
    server = RPCClient('ProductionManagement/ProductionManager',timeout=120)
    res = server.getTransformation(int(prodID))
    if not res['OK']: 
      return res
    paramDict = res['Value']
    if paramDict.has_key('Additional'):
      paramDict.update(paramDict.pop('Additional'))
    return S_OK(paramDict)
