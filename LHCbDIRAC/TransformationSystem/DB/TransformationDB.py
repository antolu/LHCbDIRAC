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
import DIRAC.TransformationSystem.DB.TransformationDB   as DIRACTransformationDB

import threading
from types import *

MAX_ERROR_COUNT = 3

#############################################################################

class TransformationDB(DIRACTransformationDB.TransformationDB):

  def __init__(self, dbname, dbconfig, maxQueueSize=10 ):
    """ The standard constructor takes the database name (dbname) and the name of the
        configuration section (dbconfig)
    """
    DIRACTransformationDB.TransformationDB.__init__(self,dbname, dbconfig, maxQueueSize)
    self.lock = threading.Lock()
    self.dbname = dbname
    self.filters = self.__getFilters()

  def addBookkeepingQuery(self,queryDict):
    """ Add a new Bookkeeping query specification
    """
    queryFields = ['SimulationConditions','DataTakingConditions','ProcessingPass','FileType','EventType',
                   'ConfigName','ConfigVersion','ProductionID','DataQualityFlag']
    parameters = []
    values = []
    qvalues = []
    for field in queryFields:
      if field in queryDict.keys():
        parameters.append(field)
        if field == 'ProductionID' or field == 'EventType':
          values.append(str(queryDict[field]))
          qvalues.append(str(queryDict[field]))
        else:
          values.append("'"+queryDict[field]+"'")
          qvalues.append(queryDict[field])
      else:
        if field == 'ProductionID' or field == 'EventType':
          qvalues.append(0)
        else:
          qvalues.append('All')
    # Check for the already existing queries first
    selections = []
    for i in range(len(queryFields)):
      selections.append(queryFields[i]+"='"+str(qvalues[i])+"'")
    selectionString = ' AND '.join(selections)
    req = "SELECT BkQueryID FROM BkQueries WHERE %s" % selectionString
    result = self._query(req)
    if not result['OK']:
      return result
    if result['Value']:
      bkQueryID = result['Value'][0][0]
      return S_OK(bkQueryID)
    req = "INSERT INTO BkQueries (%s) VALUES (%s)" % (','.join(parameters),','.join(values))
    self.lock.acquire()
    result = self._getConnection()
    if result['OK']:
      connection = result['Value']
    else:
      return S_ERROR('Failed to get connection to MySQL: '+result['Message'])
    res = self._update(req,connection)
    if not res['OK']:
      self.lock.release()
      return res
    req = "SELECT LAST_INSERT_ID();"
    res = self._query(req,connection)
    self.lock.release()
    if not res['OK']:
      return res
    queryID = int(res['Value'][0][0])
    return S_OK(queryID)

  def getBookkeepingQuery(self,bkQueryID=0):
    """ Get the bookkeeping query parameters, if bkQueyID is 0 then get all the queries
    """
    queryFields = ['SimulationConditions','DataTakingConditions','ProcessingPass',
                   'FileType','EventType','ConfigName','ConfigVersion','ProductionID','DataQualityFlag']

    fieldsString = ','.join(queryFields)

    if bkQueryID:
      req = "SELECT BkQueryID,%s FROM BkQueries WHERE BkQueryID=%d" % (fieldsString,int(bkQueryID))
    else:
      req = "SELECT BkQueryID,%s FROM BkQueries" % (fieldsString,)
    result = self._query(req)
    if not result['OK']:
      return result
    if not result['Value']:
      return S_ERROR('BkQuery %d not found' % int(bkQueryID))
    resultDict = {}
    for row in result['Value']:
      bkDict = {}
      for parameter,value in zip(['BkQueryID']+queryFields,row):
        bkDict[parameter] = value
      resultDict[bkDict['BkQueryID']] = bkDict
    if bkQueryID:
      return S_OK(bkDict)
    else:
      return S_OK(resultDict)

  def deleteBookkeepingQuery(self,bkQueryID):
    """ Delete the specified query from the database
    """
    req = 'DELETE FROM BkQueries WHERE BkQueryID=%d' % int(bkQueryID)
    return self._update(req)

  def setTransformationQuery(self,transName,bkQueryID):
    """ Set the bookkeeping query ID of the transformation specified by transID """
    return self.addTransformationParameter(transName,'BkQueryID',bkQueryIDs)
  
  def createTransformationQuery(self,transName,queryDict):
    """ Create the supplied BK query and associate it to the transformation """
    res = self.addBookkeepingQuery(queryDict)
    if not res['OK']:
      return res
    bkQueryID = res['Value']
    return self.setTransformationQuery(transName,bkQueryID)
   
  def getBookkeepingQueryForTransformation(self,transName):
    """ Get the BK query associated to the transformation """
    res = self.getTransformationParameters(transName,['BkQueryID'])
    if not res['OK']:
      return res
    bkQueryID = result['Value']
    return self.getBookkeepingQuery(bkQueryID)
