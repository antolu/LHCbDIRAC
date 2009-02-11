# $Id: ProductionRequestHandler.py,v 1.1 2009/02/11 10:52:33 azhelezo Exp $
"""
ProductionRequestHandler is the implementation of
the Production Request service
"""
__RCSID__ = "$Revision: 1.1 $"

import re

from types import *
import threading
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.ProductionManagementSystem.DB.ProductionRequestDB import ProductionRequestDB
from DIRAC.Core.DISET.RPCClient import RPCClient

# This is a global instance of the ProductionRequestDB class
productionRequestDB = False

def initializeProductionRequestHandler( serviceInfo ):
  global productionRequestDB
  productionRequestDB = ProductionRequestDB()
  return S_OK()

class ProductionRequestHandler( RequestHandler ):

  def __init__(self,*args,**kargs):

    RequestHandler.__init__(self, *args,**kargs)

    self.database = productionRequestDB
    self.lock = threading.Lock()


  def __clientCredentials(self):
    creds = self.getRemoteCredentials()
    group = creds.get('group','(unknown)')
    if 'DN' in creds:
      cn = re.search('/CN=([^/]+)',creds['DN'])
      if cn:
        return { 'User':cn.group(1), 'Group':group }
    return { 'User':creds.get('username','Anonymous'), 'Group':group }

  types_createProductionRequest = [DictType]
  def export_createProductionRequest(self,requestDict):
    """ Create production request
    """
    creds = self.__clientCredentials()
    if not 'MasterID' in requestDict or 'RequestAuthor' in requestDict:
      requestDict['RequestAuthor'] = creds['User']
    return self.database.createProductionRequest(requestDict)

  types_getProductionRequest = [ListType]
  def export_getProductionRequest(self,requestIDList):
    """ Get production request(s) specified by the list of requestIDs
        AZ!!: not tested !! 
    """
    if not requestIDList:
      return S_OK({})
    result = self.database.getProductionRequest(requestIDList)
    if not result['OK']:
      return result
    rows = {}
    for row in result['Value']:
      id = row.pop('RequestID')
      rows[id] = row
    return S_OK(rows)

  types_getProductionRequestList = [LongType,StringType,StringType,
                                    LongType,LongType]
  def export_getProductionRequestList(self,subrequestFor,
                                      sortBy,sortOrder,offset,limit):
    """ Get production requests in list format (for portal grid)
    """
    return self.database.getProductionRequest([],subrequestFor,
                                             sortBy,sortOrder,
                                             offset,limit)

  types_updateProductionRequest = [LongType,DictType]
  def export_updateProductionRequest(self,requestID,requestDict):
    """ Update production request specified by requestID
    """
    creds = self.__clientCredentials()
    return self.database.updateProductionRequest(requestID,requestDict,creds)

  types_deleteProductionRequest = [LongType]
  def export_deleteProductionRequest(self,requestID):
    """ Delete production request specified by requestID
    """
    return self.database.deleteProductionRequest(requestID)

  types_getProductionProgressList = [LongType]
  def export_getProductionProgressList(self,requestID):
    """ Return the list of associated with requestID productions
    """
    return self.database.getProductionProgress(requestID)

  types_addProductionToRequest = [DictType]
  def export_addProductionToRequest(self,pdict):
    """ Associate production to request
    """
    return self.database.addProductionToRequest(pdict)

  types_removeProductionFromRequest = [LongType]
  def export_removeProductionFromRequest(self,productionID):
    """ Deassociate production
    """
    return self.database.removeProductionFromRequest(productionID)

  types_useProductionForRequest = [LongType,BooleanType]
  def export_useProductionForRequest(self,productionID,used):
    """ Set Used flags for production
    """
    return self.database.useProductionForRequest(productionID,used)

  types_getRequestHistory = [LongType]
  def export_getRequestHistory(self,requestID):
    """ Return the list of state changes for the request
    """
    return self.database.getRequestHistory(requestID)

  types_getTrackedProductions = []
  def export_getTrackedProductions(self):
    """ Return the list of productions in active requests
    """
    return self.database.getTrackedProductions()

  types_updateTrackedProductions = [ListType]
  def export_updateTrackedProductions(self,update):
    """ Update tracked productions (used by Agent)
    """
    return self.database.updateTrackedProductions(update)
