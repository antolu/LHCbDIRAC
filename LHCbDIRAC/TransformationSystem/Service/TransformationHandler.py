""" DISET request handler base class for the TransformationDB.
"""
from DIRAC                                                      import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.DISET.RequestHandler                            import RequestHandler
from DIRAC.TransformationSystem.DB.TransformationDB             import TransformationDB
from DIRAC.TransformationSystem.Service.TransformationHandler   import TransformationHandler
from types import *

class TransformationHandler(TransformationHandler):

  types_addBookkeepingQuery = [ DictType ]
  def export_addBookkeepingQuery( self, queryDict ):
    result = self.database.addBookkeepingQuery(queryDict)
    if not result['OK']:
      gLogger.error(result['Message'])
    return result  

  types_getBookkeepingQueryForTransformation = [ [LongType, IntType, StringType]]
  def export_getBookkeepingQueryForTransformation( self, id_ ):
    result = self.database.getBookkeepingQueryForTransformation(id_)
    if not result['OK']:
      gLogger.error(result['Message'])
    return result 
    
  types_getBookkeepingQuery = [ [LongType, IntType]]
  def export_getBookkeepingQuery( self, id_ ):
    result = self.database.getBookkeepingQuery(id_)
    if not result['OK']:
      gLogger.error(result['Message'])
    return result   
  
  types_deleteBookkeepingQuery = [ [LongType, IntType]]
  def export_deleteBookkeepingQuery( self, id_ ):
    result = self.database.deleteBookkeepingQuery(id_)
    if not result['OK']:
      gLogger.error(result['Message'])
    return result  
