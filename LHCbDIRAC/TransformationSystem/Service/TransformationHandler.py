########################################################################
# $Id: TransformationHandler.py 18968 2009-12-03 10:33:19Z acsmith $
########################################################################
""" DISET request handler for the LHCbDIRAC/TransformationDB. """

__RCSID__ = "$Id: TransformationHandler.py 18968 2009-12-03 10:33:19Z acsmith $"

from DIRAC                                                      import  gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.DISET.RequestHandler                            import  RequestHandler
from LHCbDIRAC.TransformationSystem.DB.TransformationDB         import  TransformationDB
import DIRAC.TransformationSystem.Service.TransformationHandler as      DIRACTransformationHandler
from types import *

class TransformationHandler(DIRACTransformationHandler.TransformationHandler):
  
  types_addBookkeepingQuery = [ DictType ]
  def export_addBookkeepingQuery(self, queryDict):
    res = self.database.addBookkeepingQuery(queryDict)
    return self.__parseRes(res)

  types_getBookkeepingQueryForTransformation = [ [LongType, IntType, StringType] ]
  def export_getBookkeepingQueryForTransformation(self, transName):
    res = self.database.getBookkeepingQueryForTransformation(transName)
    return self.__parseRes(res)
    
  types_getBookkeepingQuery = [ [LongType, IntType] ]
  def export_getBookkeepingQuery(self, queryID):
    res = self.database.getBookkeepingQuery(queryID)
    return self.__parseRes(res)
    
  types_deleteBookkeepingQuery = [ [LongType, IntType] ]
  def export_deleteBookkeepingQuery(self, queryID):
    res = self.database.deleteBookkeepingQuery(queryID)
    return self.__parseRes(res)
  
  types_setTransformationQuery = [ [LongType, IntType, StringType], [LongType, IntType] ]
  def export_setTransformationQuery(self,transName, queryID):
    res = self.database.setTransformationQuery(transName, queryID)
    return self.__parseRes(res)
  
  types_createTransformationQuery = [ [LongType, IntType, StringType], DictType ]
  def createTransformationQuery(self,transName,queryDict):
    res = self.database.createTransformationQuery(transName, queryDict)                     
    return self.__parseRes(res)

  def __parseRes(self,res):
    if not res['OK']:
      gLogger.error(res['Message'])
    return res
