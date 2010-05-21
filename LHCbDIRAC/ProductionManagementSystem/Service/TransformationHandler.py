########################################################################
# $Id: TransformationHandler.py 18968 2009-12-03 10:33:19Z acsmith $
########################################################################
""" DISET request handler for the LHCbDIRAC/TransformationDB. """

__RCSID__ = "$Id: TransformationHandler.py 18968 2009-12-03 10:33:19Z acsmith $"

from DIRAC                                                      import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.DISET.RequestHandler                            import RequestHandler
from DIRAC.TransformationSystem.Service.TransformationHandler   import TransformationHandler as DIRACTransformationHandler
from types import *

class TransformationHandler(DIRACTransformationHandler):

  def __init__(self,*args,**kargs):
    DIRACTransformationHandler.__init__(self, *args,**kargs)

  #############################################################################
  #
  # Managing the BkQueries table
  #
  
  types_createTransformationQuery = [ [LongType, IntType, StringType], DictType ]
  def export_createTransformationQuery(self,transName,queryDict):
    authorDN = self._clientTransport.peerCredentials['DN']
    res = self.database.createTransformationQuery(transName, queryDict,author=authorDN)
    return self.__parseRes(res)

  types_deleteTransformationBookkeepingQuery = [ [LongType, IntType, StringType] ]
  def export_deleteTransformationBookkeepingQuery(self, transName):
    authorDN = self._clientTransport.peerCredentials['DN']
    res = self.database.deleteTransformationBookkeepingQuery(transName,author=authorDN)
    return self.__parseRes(res)  

  types_deleteBookkeepingQuery = [ [LongType, IntType] ]
  def export_deleteBookkeepingQuery(self, queryID):
    res = self.database.deleteBookkeepingQuery(queryID)
    return self.__parseRes(res)
  
  types_getBookkeepingQueryForTransformation = [ [LongType, IntType, StringType] ]
  def export_getBookkeepingQueryForTransformation(self, transName):
    res = self.database.getBookkeepingQueryForTransformation(transName)
    return self.__parseRes(res)

  #############################################################################
  #
  # Managing the TransformationRuns table
  #
  
  types_getTransformationRuns = []
  def export_getTransformationRuns(self,condDict={}, orderAttribute=None, limit=None):
    res = self.database.getTransformationRuns(condDict=condDict, orderAttribute=orderAttribute,limit=limit) 
    return self.__parseRes(res)

  types_getTransformationRunsSummaryWeb = [DictType, ListType, IntType, IntType]
  def export_getTransformationRunsSummaryWeb(self,selectDict,sortList,startItem,maxItems):
    return self.__getTableSummaryWeb('TransformationRuns',selectDict,sortList,startItem,maxItems,selectColumns=['TransformationID','RunNumber','SelectedSite','Status'],timeStamp='LastUpdate',statusColumn='Status')  

  types_addTransformationRunFiles = [[LongType, IntType, StringType], [LongType, IntType],ListType]
  def export_addTransformationRunFiles(self,transName,runID,lfns):
    return self.database.addTransformationRunFiles(transName,runID,lfns)

  types_setTransformationRunsSite = [[LongType, IntType, StringType], [LongType, IntType],StringTypes]
  def export_setTransformationRunsSite(self,transID,runID,assignedSE):
    return self.database.setTransformationRunsSite(transID,runID,assignedSE)

