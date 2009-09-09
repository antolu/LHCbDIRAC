""" Client class to access the production manager service
"""
# $Id: ProductionClient.py,v 1.5 2009/09/09 15:09:55 acsmith Exp $
__RCSID__ = "$Revision: 1.5 $"

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
    if pname:
      if paramDict.has_key(pname):
        return S_OK(paramDict[pname])
      else:
        return S_ERROR('Parameter %s not found for production' % pname)
    return S_OK(paramDict)
  
  def setProductionStatus(self,prodID,status):
    server = RPCClient('ProductionManagement/ProductionManager',timeout=120)
    return server.setTransformationStatus(prodID, status)

  def selectWMSJobs(self,prodID,statusList=[],newer=0):
    server = RPCClient('ProductionManagement/ProductionManager',timeout=120)
    return server.selectWMSJobs(prodID,statusList,newer)

  def cleanProduction(self,prodID):
    server = RPCClient('ProductionManagement/ProductionManager',timeout=120)
    return server.cleanProduction(prodID)

  def getProduction(self,prodID):
    server = RPCClient('ProductionManagement/ProductionManager',timeout=120)
    return server.getTransformation(prodID)

  def getProductionStatus(self,prodID):
    server = RPCClient('ProductionManagement/ProductionManager',timeout=120)
    res = server.getTransformation(prodID)
    if not res['OK']:
      return res
    return S_OK(res['Value']['Status'])
