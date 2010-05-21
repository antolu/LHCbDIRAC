""" DIRAC Transformation DB

    Transformation database is used to collect and serve the necessary information
    in order to automate the task of job preparation for high level transformations.
    This class is typically used as a base class for more specific data processing
    databases
"""

__RCSID__ = "$Id: TransformationDB.py 18429 2009-11-20 10:30:43Z acsmith $"

import re,time,types,string

from DIRAC                                              import gLogger, S_OK, S_ERROR
from DIRAC.Core.Base.DB                                 import DB
from DIRAC.TransformationSystem.DB.TransformationDB     import TransformationDB as DIRACTransformationDB
from DIRAC.Core.Utilities.List                          import intListToString
import threading,copy
from types import *

MAX_ERROR_COUNT = 3

class TransformationDB(DIRACTransformationDB):

  def __init__(self, dbname, dbconfig, maxQueueSize=10 ):
    """ The standard constructor takes the database name (dbname) and the name of the
        configuration section (dbconfig)
    """
    DIRACTransformationDB.__init__(self,dbname, dbconfig, maxQueueSize)
    self.lock = threading.Lock()
    self.dbname = dbname
    self.queryFields = ['SimulationConditions','DataTakingConditions','ProcessingPass','FileType','EventType','ConfigName','ConfigVersion','ProductionID','DataQualityFlag','StartRun','EndRun']
    self.intFields = ['EventType','ProductionID','StartRun','EndRun']

    self.transRunParams = ['TransformationID','RunNumber','SelectedSite','Status','LastUpdate'] 
    self.TRANSFILEPARAMS.append("RunNumber")

  #############################################################################
  #
  # Managing the BkQueries table
  #

  def deleteBookkeepingQuery(self,bkQueryID,connection=False):
    """ Delete the specified query from the database
    """
    connection = self.__getConnection(connection)
    req = "SELECT COUNT(*) FROM AdditionalParameters WHERE ParameterName = 'BkQueryID' AND ParameterValue = %d" % bkQueryID
    res = self._query(req)
    if not res['OK']:
      return res
    count = res['Value'][0][0]
    if count != 0:
      return S_OK()
    req = 'DELETE FROM BkQueries WHERE BkQueryID=%d' % int(bkQueryID)
    return self._update(req,connection)

  def createTransformationQuery(self,transName,queryDict,author='',connection=False):
    """ Create the supplied BK query and associate it to the transformation """
    connection = self.__getConnection(connection)
    res = self.__addBookkeepingQuery(queryDict,connection=connection)
    if not res['OK']:
      return res
    bkQueryID = res['Value']
    res = self.__setTransformationQuery(transName,bkQueryID,author=author,connection=connection)
    if not res['OK']:
      return res
    return S_OK(bkQueryID)

  def deleteTransformationBookkeepingQuery(self,transName,author='',connection=False):
    """ Delete the transformation BkQuery additional parameters and delete the BkQuery if not used elsewhere """
    res = self._getConnectionTransID(connection,transName)
    if not res['OK']:
      return res
    connection = res['Value']['Connection']
    transID = res['Value']['TransformationID']
    res = self.__getTransformationBkQueryID(transID,connection=connection)
    if not res['OK']:
      return res
    bkQueryID = res['Value']
    res = self.deleteTransformationParameter(transID,'BkQueryID',author=author,connection=connection)
    if not res['OK']: 
      return res
    return self.deleteBookkeepingQuery(bkQueryID,connection=connection)
   
  def getBookkeepingQueryForTransformation(self,transName,connection=False):
    """ Get the BK query associated to the transformation """
    connection = self.__getConnection(connection)
    res = self.__getTransformationBkQueryID(transName,connection=connection)
    if not res['OK']:
      return res
    return self.__getBookkeepingQuery(res['Value'],connection=connection)

  def __getTransformationBkQueryID(self,transName,connection=False):
    res = self.getTransformationParameters(transName,['BkQueryID'],connection=connection)
    if not res['OK']:   
      return res
    bkQueryID = int(res['Value'])
    return S_OK(bkQueryID)

  def __setTransformationQuery(self,transName,bkQueryID,author='',connection=False):
    """ Set the bookkeeping query ID of the transformation specified by transID """
    return self.setTransformationParameter(transName,'BkQueryID',bkQueryID,author=author,connection=connection)
  
  def __addBookkeepingQuery(self,queryDict,connection=False):
    """ Add a new Bookkeeping query specification
    """
    connection = self.__getConnection(connection)
    values = []
    for field in self.queryFields:
      if not field in queryDict.keys():
        value = 'All'        
      else:
        value = queryDict[field]
        if type(value) in (IntType,LongType,FloatType):
          value = str(value)
        if type(value) in (ListType,TupleType):
          value = [str(x) for x in value]          
          value = ';;;'.join(value) 
      values.append(value)

    # Check for the already existing queries first
    selections = []
    for i in range(len(self.queryFields)):
      selections.append("%s= BINARY '%s'" % (self.queryFields[i],values[i]))
    selectionString = ' AND '.join(selections)
    req = "SELECT BkQueryID FROM BkQueries WHERE %s" % selectionString
    result = self._query(req,connection)
    if not result['OK']:
      return result
    if result['Value']:
      bkQueryID = result['Value'][0][0]
      return S_OK(bkQueryID)

    # Insert the new bk query
    values = ["'%s'" % x for x in values]
    req = "INSERT INTO BkQueries (%s) VALUES (%s)" % (','.join(self.queryFields),','.join(values))
    res = self._update(req,connection)
    if not res['OK']:
      return res
    queryID = res['lastRowId']
    return S_OK(queryID)

  def __getBookkeepingQuery(self,bkQueryID=0,connection=False):
    """ Get the bookkeeping query parameters, if bkQueyID is 0 then get all the queries
    """
    connection = self.__getConnection(connection)
    fieldsString = ','.join(self.queryFields)
    if bkQueryID:
      req = "SELECT BkQueryID,%s FROM BkQueries WHERE BkQueryID=%d" % (fieldsString,int(bkQueryID))
    else:
      req = "SELECT BkQueryID,%s FROM BkQueries" % (fieldsString)
    result = self._query(req,connection)
    if not result['OK']:
      return result
    if not result['Value']:
      return S_ERROR('BkQuery %d not found' % int(bkQueryID))
    resultDict = {}
    for row in result['Value']:
      bkDict = {}
      for parameter,value in zip(['BkQueryID']+self.queryFields,row):
        if parameter == 'BkQueryID':
          rowQueryID = value
        elif value != 'All':
          if re.search(';;;',str(value)):
            value = value.split(';;;')
          if parameter in self.intFields:
            if type(value) in StringTypes:
              value = long(value)
            if type(value) == ListType:
              value = [int(x) for x in value]
            if not value:
              continue
          bkDict[parameter] = value
      resultDict[rowQueryID] = bkDict
    if bkQueryID:
      return S_OK(bkDict)
    else:
      return S_OK(resultDict)

  #############################################################################
  #
  # Managing the TransformationRuns table
  #
  
  def getTransformationRuns(self,condDict={},orderAttribute=None, limit=None,connection=False):
    connection = self.__getConnection(connection)
    selectDict = {}
    for key in condDict.keys():
      if key in self.transRunParams:
        selectDict[key] = condDict[key]
    req = "SELECT %s FROM TransformationRuns %s" % (intListToString(self.transRunParams),self.buildCondition(selectDict,orderAttribute=orderAttribute,limit=limit))
    res = self._query(req,connection)
    if not res['OK']:
      return res
    webList = []
    resultList = []
    for row in res['Value']:
      # Prepare the structure for the web
      rList = []
      transDict = {}
      count = 0
      for item in row:
        transDict[self.transRunParams[count]] = item
        count += 1
        if type(item) not in [IntType,LongType]:
          rList.append(str(item))
        else:
          rList.append(item)
      webList.append(rList)
      resultList.append(transDict) 
    result = S_OK(resultList)
    result['Records'] = webList
    result['ParameterNames'] = copy.copy(self.transRunParams)
    return result

  def addTransformationRunFiles(self,transName,runID,lfns,connection=False):
    """ Add the RunID to the TransformationFiles table """
    if not lfns:
      return S_ERROR('Zero length LFN list')
    res = self._getConnectionTransID(connection,transName)
    if not res['OK']:
      return res
    connection = res['Value']['Connection']
    transID = res['Value']['TransformationID']
    res = self.__getFileIDsForLfns(lfns,connection=connection)
    if not res['OK']:
      return res
    fileIDs,lfnFilesIDs = res['Value']
    req = "UPDATE TransformationFiles SET RunNumber = %d WHERE TransformationID = %d AND FileID IN (%s)" % (runID,transID,intListToString(fileIDs.keys()))
    res = self._update(req,connection)
    if not res['OK']:
      return res
    successful = {}
    failed = {}
    for fileID in fileIDs.keys():
      lfn = fileIDs[fileID]
      successful[lfn] = "Added"
    for lfn in lfns:
      if not lfn in successful.keys():
        failed[lfn] = "Missing"
    # Now update the TransformationRuns to include the newly updated files
    req = "UPDATE TransformationRuns SET LastUpdate = UTC_TIMESTAMP() WHERE TransformationID = %d and RunNumber = %d" % (transID,runID)
    res = self._update(req,connection)
    if not res['OK']:
      gLogger.error("Failed to update TransformationRuns table with LastUpdate",res['Message'])
    elif not res['Value']:
      self.__insertTransformationRun(transID,runID,connection=connection)
    resDict = {'Successful':successful,'Failed':failed}
    return S_OK(resDict)

  def setTransformationRunsSite(self,transName,runID,selectedSite,connection=False):
    res = self._getConnectionTransID(connection,transName)
    if not res['OK']:
      return res
    connection = res['Value']['Connection']
    transID = res['Value']['TransformationID']
    req = "UPDATE TransformationRuns SET SelectedSite = '%s', LastUpdate = UTC_TIMESTAMP() WHERE TransformationID = %d and RunNumber = %d" % (selectedSite,transID,runID)
    res = self._update(req,connection)
    if not res['OK']:
      gLogger.error("Failed to update TransformationRuns table with SelectedSite",res['Message'])
    elif not res['Value']:
      res = self.__insertTransformationRun(transID,runID,selectedSite=selectedSite,connection=connection)
    return res

  def __insertTransformationRun(self,transID,runID,selectedSite='',connection=False):
    req = "INSERT INTO TransformationRuns (TransformationID,RunNumber,Status,LastUpdate) VALUES (%d,%d,'Active',UTC_TIMESTAMP())" % (transID,runID)
    if selectedSite:
      req = "INSERT INTO TransformationRuns (TransformationID,RunNumber,SelectedSite,Status,LastUpdate) VALUES (%d,%d,'%s','Active',UTC_TIMESTAMP())" % (transID,runID,selectedSite)
    res = self._update(req,connection)
    if not res['OK']:
      gLogger.error("Failed to insert to TransformationRuns table",res['Message'])    
    return res
