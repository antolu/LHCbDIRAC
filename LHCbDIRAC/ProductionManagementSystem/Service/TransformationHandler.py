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

  types_setBookkeepingQueryEndRunForTransformation = [ [LongType, IntType, StringType] , [LongType, IntType]]
  def export_setBookkeepingQueryEndRunForTransformation(self, transName, runNumber):
    res = self.database.setBookkeepingQueryEndRunForTransformation(transName, runNumber)
    return self.__parseRes(res)

  #############################################################################
  #
  # Managing the TransformationRuns table
  #
  
  types_getTransformationRuns = []
  def export_getTransformationRuns(self,condDict={}, orderAttribute=None, limit=None):
    res = self.database.getTransformationRuns(condDict=condDict, orderAttribute=orderAttribute,limit=limit) 
    return self.__parseRes(res)

  types_getTransformationRunStats = [ListType]
  def export_getTransformationRunStats(self,transIDs):
    res = self.database.getTransformationRunStats(transIDs)
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

  types_getTransformationRunsSummaryWeb = [DictType, ListType, IntType, IntType]
  def export_getTransformationRunsSummaryWeb(self, selectDict, sortList, startItem, maxItems):
    """ Get the summary of the transformation run information for a given page in the generic format """

    # Obtain the timing information from the selectDict
    last_update = selectDict.get('LastUpdate',None)    
    if last_update:
      del selectDict['LastUpdate']
    fromDate = selectDict.get('FromDate',None)    
    if fromDate:
      del selectDict['FromDate']
    if not fromDate:
      fromDate = last_update  
    toDate = selectDict.get('ToDate',None)    
    if toDate:
      del selectDict['ToDate']  
    # Sorting instructions. Only one for the moment.
    if sortList:
      orderAttribute = sortList[0][0]+":"+sortList[0][1]
    else:
      orderAttribute = None

    # Get the transformations that match the selection
    res = self.database.getTransformationRuns(condDict=selectDict,older=toDate, newer=fromDate, orderAttribute=orderAttribute)
    if not res['OK']:
      return self.__parseRes(res)

    # Prepare the standard structure now within the resultDict dictionary
    resultDict = {}
    trList = res['Records']
    # Create the total records entry
    nTrans = len(trList)
    resultDict['TotalRecords'] = nTrans
    # Create the ParameterNames entry
    paramNames = res['ParameterNames']
    resultDict['ParameterNames'] = paramNames

    # Add the job states to the ParameterNames entry
    # TODO Removed for the moment
    #taskStateNames   = ['Created','Running','Submitted','Failed','Waiting','Done','Stalled']
    #resultDict['ParameterNames'] += ['Jobs_'+x for x in taskStateNames]
    # Add the file states to the ParameterNames entry
    fileStateNames  = ['PercentProcessed','Processed','Unused','Assigned','Total','Problematic']
    resultDict['ParameterNames'] += ['Files_'+x for x in fileStateNames]

    # Get the transformations which are within the selected window
    if nTrans == 0:
      return S_OK(resultDict)
    ini = startItem
    last = ini + maxItems
    if ini >= nTrans:
      return S_ERROR('Item number out of range')
    if last > nTrans:
      last = nTrans
    transList = trList[ini:last]
    if not transList:
      return S_OK(resultDict)

    # Obtain the run statistics for the requested transformations
    transIDs = []
    for transRun in transList:
      transRunDict = dict(zip(paramNames,transRun))
      transID = int(transRunDict['TransformationID'])
      if not transID in transIDs:
        transIDs.append(transID)
    res = self.database.getTransformationRunStats(transIDs)
    if not res['OK']:
      return res
    transRunStatusDict = res['Value']
      
    statusDict = {}
    # Add specific information for each selected transformation/run
    for transRun in transList:
      transRunDict = dict(zip(paramNames,transRun))
      transID = transRunDict['TransformationID']
      runID = transRunDict['RunNumber']
      # Update the status counters
      status = transRunDict['Status']
      if not statusDict.has_key(status):
        statusDict[status] = 0
      statusDict[status] += 1

      # Populate the run file statistics
      fileDict = transRunStatusDict[transID][runID]
      if fileDict['Total'] == 0:
        fileDict['PercentProcessed']  = 0
      else:
        processed = fileDict.get('Processed')
        if not processed:
          processed = 0
        fileDict['PercentProcessed'] = "%.1f" % ((processed*100.0)/fileDict['Total'])
      for state in fileStateNames:
        if fileDict and fileDict.has_key(state):
          transRun.append(fileDict[state])
        else:
          transRun.append(0)

      # Get the statistics on the number of jobs for the transformation
      # TODO Removed for the moment
      #res = self.database.getTransformationTaskRunStats(transID)
      #taskDict = {}
      #if res['OK'] and res['Value']:
      #  taskDict = res['Value']
      #for state in taskStateNames:
      #  if taskDict and taskDict.has_key(state):
      #    trans.append(taskDict[state])
      #  else:
      #    trans.append(0)

    resultDict['Records'] = transList
    resultDict['Extras'] = statusDict
    return S_OK(resultDict)
