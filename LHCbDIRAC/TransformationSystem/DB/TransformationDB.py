""" DIRAC Transformation DB

    Transformation database is used to collect and serve the necessary information
    in order to automate the task of job preparation for high level transformations.
    This class is typically used as a base class for more specific data processing
    databases
"""

__RCSID__ = "$Id$"

import re

from DIRAC                                              import gLogger, S_OK, S_ERROR
from DIRAC.TransformationSystem.DB.TransformationDB     import TransformationDB as DIRACTransformationDB
from DIRAC.Core.Utilities.List                          import intListToString
from LHCbDIRAC.Workflow.Utilities.Utils                 import makeRunList
import threading, copy
from types import IntType, LongType, FloatType, ListType, TupleType, StringTypes

MAX_ERROR_COUNT = 3

class TransformationDB( DIRACTransformationDB ):
  """ Extension of the DIRAC Transformation DB
  """

  def __init__( self, dbname, dbconfig, maxQueueSize = 10 ):
    """ The standard constructor takes the database name (dbname) and the name of the
        configuration section (dbconfig)
    """
    DIRACTransformationDB.__init__( self, dbname, dbconfig, maxQueueSize )
    self.lock = threading.Lock()
    self.dbname = dbname
    self.queryFields = ['SimulationConditions', 'DataTakingConditions', 'ProcessingPass', 'FileType',
                        'EventType', 'ConfigName', 'ConfigVersion', 'ProductionID', 'DataQualityFlag',
                         'StartRun', 'EndRun', 'Visible', 'RunNumbers', 'TCK']
    self.intFields = ['EventType', 'ProductionID', 'StartRun', 'EndRun']
    self.transRunParams = ['TransformationID', 'RunNumber', 'SelectedSite', 'Status', 'LastUpdate']
    self.TRANSFILEPARAMS.append( "RunNumber" )
    self.TASKSPARAMS.append( "RunNumber" )

  def cleanTransformation( self, transName, author = '', connection = False ):
    """ Clean the transformation specified by name or id
        Extends DIRAC one for deleting the unused runs metadata
    """
    res = self._getConnectionTransID( connection, transName )
    if not res['OK']:
      return res
    connection = res['Value']['Connection']
    transID = res['Value']['TransformationID']

    # deleting runs metadata
    req = "SELECT DISTINCT RunNumber FROM TransformationRuns WHERE TransformationID = %s" % transID
    res = self._query( req )
    if not res['OK']:
      gLogger.error( "Failure executing %s" % str( req ) )
      return res
    runsMaybeToDelete = [r[0] for r in res['Value']]

    req = "SELECT DISTINCT RunNumber FROM TransformationRuns WHERE TransformationID != %s" % transID
    res = self._query( req )
    if not res['OK']:
      gLogger.error( "Failure executing %s" % str( req ) )
      return res
    runsToKeep = [r[0] for r in res['Value']]

    runIDsToBeDeleted = list( set( runsToKeep ) - set( runsMaybeToDelete ) )

    res = self.deleteRunsMetadata( runIDsToBeDeleted, connection )
    if not res['OK']:
      return res

    # delete files, tasks, taskinputs and parameters as for base DIRAC class 
    res = self.__deleteTransformationFiles( transID, connection = connection )
    if not res['OK']:
      return res
    res = self.__deleteTransformationTasks( transID, connection = connection )
    if not res['OK']:
      return res
    res = self.__deleteTransformationTaskInputs( transID, connection = connection )
    if not res['OK']:
      return res
    res = self.setTransformationParameter( transID, 'Status', 'Cleaned', author = author, connection = connection )
    if not res['OK']:
      return res
    message = "Transformation Cleaned"
    self.__updateTransformationLogging( transID, message, author, connection = connection )
    return S_OK( transID )


  def getTasksForSubmission( self, transName, numTasks = 1, site = "", statusList = ['Created'],
                             older = None, newer = None, connection = False ):
    """ extends base class including the run metadata
    """
    tasksDict = DIRACTransformationDB.getTasksForSubmission( self, transName, numTasks, site, statusList,
                                                             older, newer, connection )
    if not tasksDict['OK']:
      return tasksDict

    tasksDict = tasksDict['Value']
    runNumbers = []
    for taskForSumbission in tasksDict.values():
      if 'RunNumber' in taskForSumbission.keys():
        run = taskForSumbission['RunNumber']
        if run:
          if run not in runNumbers:
            runNumbers.append( run )

    if runNumbers:
      runsMetadata = self.getRunsMetadata( runNumbers, connection )
      if not runsMetadata['OK']:
        return runsMetadata

      runsMetadata = runsMetadata['Value']
      for taskForSumbission in tasksDict.values():
        try:
          taskForSumbission['RunMetadata'] = runsMetadata[taskForSumbission['RunNumber']]
        except KeyError:
          continue

    return S_OK( tasksDict )


  #############################################################################
  #
  # Managing the BkQueries table
  #

  def deleteBookkeepingQuery( self, bkQueryID, connection = False ):
    """ Delete the specified query from the database
    """
    connection = self.__getConnection( connection )
    req = "SELECT COUNT(*) FROM AdditionalParameters WHERE ParameterName = 'BkQueryID' AND ParameterValue = %d" % bkQueryID
    res = self._query( req )
    if not res['OK']:
      return res
    count = res['Value'][0][0]
    if count != 0:
      return S_OK()
    req = 'DELETE FROM BkQueries WHERE BkQueryID=%d' % int( bkQueryID )
    return self._update( req, connection )

  def createTransformationQuery( self, transName, queryDict, author = '', connection = False ):
    """ Create the supplied BK query and associate it to the transformation """
    connection = self.__getConnection( connection )
    res = self.__addBookkeepingQuery( queryDict, connection = connection )
    if not res['OK']:
      print "not res ", res#canc
      return res
    bkQueryID = res['Value']
    res = self.__setTransformationQuery( transName, bkQueryID, author = author, connection = connection )
    if not res['OK']:
      return res
    return S_OK( bkQueryID )

  def deleteTransformationBookkeepingQuery( self, transName, author = '', connection = False ):
    """ Delete the transformation BkQuery additional parameters and delete the BkQuery if not used elsewhere """
    res = self._getConnectionTransID( connection, transName )
    if not res['OK']:
      return res
    connection = res['Value']['Connection']
    transID = res['Value']['TransformationID']
    res = self.__getTransformationBkQueryID( transID, connection = connection )
    if not res['OK']:
      print "Esce qui"
      return res
    bkQueryID = res['Value']
    res = self.deleteTransformationParameter( transID, 'BkQueryID', author = author, connection = connection )
    if not res['OK']:
      return res
    return self.deleteBookkeepingQuery( bkQueryID, connection = connection )

  def getBookkeepingQueryForTransformation( self, transName, connection = False ):
    """ Get the BK query associated to the transformation """
    connection = self.__getConnection( connection )
    res = self.__getTransformationBkQueryID( transName, connection = connection )
    if not res['OK']:
      return res
    return self.__getBookkeepingQuery( res['Value'], connection = connection )

  def convertBookkeepingQueryRunListTransformation( self, transName, connection = False ):
    res = self._getConnectionTransID( connection, transName )
    if not res['OK']:
      return S_ERROR("Failed to get Connection to TransformationDB")
    connection = res['Value']['Connection']
    transID = res['Value']['TransformationID']
    res = self.__getTransformationBkQueryID( transID, connection = connection )
    if not res['OK']:
      return S_ERROR("Cannot retrieve BkQueryID")
    bkQueryID = res['Value']
    res = self.__getBookkeepingQuery( bkQueryID, connection = connection )
    if not res['OK']:
      return S_ERROR("Cannot retrieve BkQuery")
    bkQuery = res['Value']
    startRun = bkQuery.get( 'StartRun' )
    endRun = bkQuery.get( 'EndRun' )
    runInput=str(startRun)+":"+str(endRun)
    res = makeRunList(str(runInput))
    runs=res['Value']
    if len(runs)>999:
      return S_ERROR("RunList bigger the 1000 not allowed because of Oracle limitations!!!")
    self.__setTransformationQuery(transID, bkQueryID)
    req1 = "UPDATE BkQueries SET StartRun = %d WHERE BkQueryID = %d" % ( 0, bkQueryID )
    req2 = "UPDATE BkQueries SET EndRun = %d WHERE BkQueryID = %d" % ( 0, bkQueryID )
    value = ';;;'.join( runs )
    req3 = "UPDATE BkQueries SET RunNumbers = '%s' WHERE BkQueryID = %d" % ( value, bkQueryID )
    self._update( req1, connection )
    self._update( req2, connection )
    self._update( req3, connection )
    return S_OK(runs)

  def setBookkeepingQueryEndRunForTransformation( self, transName, EndRun, connection = False ):
    res = self._getConnectionTransID( connection, transName )
    if not res['OK']:
      return S_ERROR("Failed to get Connection to TransformationDB")
    connection = res['Value']['Connection']
    transID = res['Value']['TransformationID']
    res = self.__getTransformationBkQueryID( transID, connection = connection )
    if not res['OK']:
      return S_ERROR("Cannot retrieve BkQueryID")
    bkQueryID = res['Value']
    res = self.__getBookkeepingQuery( bkQueryID, connection = connection )
    if not res['OK']:
      return S_ERROR("Cannot retrieve BkQuery")
    RunInQuery = res['Value']['RunNumbers']
    if isinstance(bkQuery['RunNumbers'], str):
      RunInQuery = [bkQuery['RunNumbers']]
    else:
      RunInQuery = bkQuery['RunNumbers'] 
    if EndRun<int(RunInQuery[-1]):
      return S_ERROR("Cannot decrease the end run!")
    if str(EndRun) not in RunInQuery:
      s = str(RunInQuery[-1])+':'+str(EndRun)
      res = makeRunList(str(s))
      runs=res['Value']
      for r in runs:
        if r not in RunInQuery:
          RunInQuery.append(r)
          RunInQuery.sort()
    if len(RunInQuery)>999:
      return S_ERROR("RunList bigger the 1000 not allowed because of Oracle limitations!!!")    
    value = ';;;'.join( RunInQuery )
    req = "UPDATE BkQueries SET RunNumbers = '%s' WHERE BkQueryID = %d" % ( value, bkQueryID )
    self._update( req, connection )
    return S_OK()

  def setBookkeepingQueryStartRunForTransformation( self, transName, StartRun, connection = False ):
    res = self._getConnectionTransID( connection, transName )
    if not res['OK']:
      return S_ERROR("Failed to get Connection to TransformationDB")
    connection = res['Value']['Connection']
    transID = res['Value']['TransformationID']
    res = self.__getTransformationBkQueryID( transID, connection = connection )
    if not res['OK']:
      return S_ERROR("Cannot retrieve BkQueryID")
    bkQueryID = res['Value']
    res = self.__getBookkeepingQuery( bkQueryID, connection = connection )
    if not res['OK']:
      return S_ERROR("Cannot retrieve BkQuery")
    RunInQuery = res['Value']['RunNumbers']
    if isinstance(bkQuery['RunNumbers'], str):
      RunInQuery = [bkQuery['RunNumbers']]
    else:
      RunInQuery = bkQuery['RunNumbers'] 
    if StartRun>int(RunInQuery[0]):
      return S_ERROR("Cannot increase the start run!")
    if str(StartRun) not in RunInQuery:
      s = str(StartRun)+':'+str(RunInQuery[0])
      res = makeRunList(str(s))
      runs=res['Value']
      for r in runs:
        if r not in RunInQuery:
          RunInQuery.append(r)
          RunInQuery.sort()
    if len(RunInQuery)>999:
      return S_ERROR("RunList bigger the 1000 not allowed because of Oracle limitations!!!")    
    value = ';;;'.join( RunInQuery )
    req = "UPDATE BkQueries SET RunNumbers = '%s' WHERE BkQueryID = %d" % ( value, bkQueryID )
    self._update( req, connection )
    return S_OK()

  def addBookkeepingQueryRunListTransformation( self, transName, runList, connection = False ):
    res = self._getConnectionTransID( connection, transName )
    if not res['OK']:
      return S_ERROR("Failed to get Connection to TransformationDB")
    connection = res['Value']['Connection']
    transID = res['Value']['TransformationID']
    res = self.__getTransformationBkQueryID( transID, connection = connection )
    if not res['OK']:
      return S_ERROR("Cannot retrieve BkQueryID")
    bkQueryID = res['Value']
    res = self.__getBookkeepingQuery( bkQueryID, connection = connection )
    if not res['OK']:
      return S_ERROR("Cannot retrieve BkQuery")
    bkQuery = res['Value']
    RunInQuery = bkQuery['RunNumbers']
    res = makeRunList(str(runList))
    runs=res['Value']
    for r in runs:
      if r not in RunInQuery:
         RunInQuery.append(r)
    RunInQuery.sort()
    if len(RunInQuery)>999:
      return S_ERROR("RunList bigger the 1000 not allowed because of Oracle limitations!!!")    
    value = ';;;'.join( RunInQuery )
    req = "UPDATE BkQueries SET RunNumbers = '%s' WHERE BkQueryID = %d" % ( value, bkQueryID )
    self._update( req, connection )
    return S_OK()
  
  def __getTransformationBkQueryID( self, transName, connection = False ):
    res = self.getTransformationParameters( transName, ['BkQueryID'], connection = connection )
    if not res['OK']:
      return res
    bkQueryID = int( res['Value'] )
    return S_OK( bkQueryID )

  def __setTransformationQuery( self, transName, bkQueryID, author = '', connection = False ):
    """ Set the bookkeeping query ID of the transformation specified by transID """
    return self.setTransformationParameter( transName, 'BkQueryID', bkQueryID, author = author, connection = connection )


  def __addBookkeepingQuery( self, queryDict, connection = False ):
    """ Add a new Bookkeeping query specification
    """
    connection = self.__getConnection( connection )
    values = []
    for field in self.queryFields:
      if not field in queryDict.keys():
        if field in ['StartRun', 'EndRun']:
          value = 0
        else:
          value = 'All'
      else:
        value = queryDict[field]
                        
        if type( value ) in ( IntType, LongType, FloatType ):
          value = str( value )
        if type( value ) in ( ListType, TupleType ):
          value = [str( x ) for x in value]
          value = ';;;'.join( value )
      values.append( value )

    # Insert the new bk query
    values = ["'%s'" % x for x in values]
    req = "INSERT INTO BkQueries (%s) VALUES (%s)" % ( ','.join( self.queryFields ), ','.join( values ) )
    res = self._update( req, connection )
    if not res['OK']:
      return res
    queryID = res['lastRowId']
    return S_OK( queryID )

         

  def __getBookkeepingQuery( self, bkQueryID = 0, connection = False ):
    """ Get the bookkeeping query parameters, if bkQueyID is 0 then get all the queries
    """
    connection = self.__getConnection( connection )
    fieldsString = ','.join( self.queryFields )
    if bkQueryID:
      req = "SELECT BkQueryID,%s FROM BkQueries WHERE BkQueryID=%d" % ( fieldsString, int( bkQueryID ) )
    else:
      req = "SELECT BkQueryID,%s FROM BkQueries" % ( fieldsString )

    result = self._query( req, connection )
    if not result['OK']:
      return result
    if not result['Value']:
      return S_ERROR( 'BkQuery %d not found' % int( bkQueryID ) )
    resultDict = {}
    for row in result['Value']:
      bkDict = {}
      for parameter, value in zip( ['BkQueryID'] + self.queryFields, row ):
        if parameter == 'BkQueryID':
          rowQueryID = value
        elif value != 'All':
          if re.search( ';;;', str( value ) ):
            value = value.split( ';;;' )
          if parameter in self.intFields:
            if type( value ) in StringTypes:
              value = long( value )
            if type( value ) == ListType:
              value = [int( x ) for x in value]
            if not value:
              continue
          bkDict[parameter] = value
      resultDict[rowQueryID] = bkDict
    if bkQueryID:
      return S_OK( bkDict )
    else:
      return S_OK( resultDict )

  # 
  # extends DIRAC.__insertExistingTransformationFiles
  # Does not add userSE and adds runNumber
  # 
  def __insertExistingTransformationFiles( self, transID, fileTuples, connection = False ):
    req = "INSERT INTO TransformationFiles (TransformationID,Status,TaskID,FileID,TargetSE,LastUpdate,RunNumber) VALUES"
    candidates = False
    for ft in fileTuples:
      lfn, originalID, fileID, status, taskID, targetSE, usedSE, errorCount, lastUpdate, insertTime, runNumber = ft[:11]
      if status != 'Unused':
        candidates = True
        if not re.search( '-', status ):
          status = "%s-%d" % ( status, originalID )
          if taskID:
            taskID = str( int( originalID ) ).zfill( 8 ) + '_' + str( int( taskID ) ).zfill( 8 )
        req = "%s (%d,'%s','%s',%d,'%s',UTC_TIMESTAMP(),%d)," % ( req, transID, status, taskID, fileID, targetSE, runNumber )
    req = req.rstrip( "," )
    if not candidates:
      return S_OK()
    return self._update( req, connection )

  #############################################################################
  #
  # Managing the TransformationTasks table
  #

  def addTaskForTransformation( self, transID, lfns = [], se = 'Unknown', connection = False ):
    """ Create a new task with the supplied files for a transformation.
    """
    res = self._getConnectionTransID( connection, transID )
    if not res['OK']:
      return res
    connection = res['Value']['Connection']
    transID = res['Value']['TransformationID']
    # Be sure the all the supplied LFNs are known to the database for the supplied transformation
    fileIDs = []
    runID = 0
    if lfns:
      res = self.getTransformationFiles( condDict = {'TransformationID':transID, 'LFN':lfns}, connection = connection )
      if not res['OK']:
        return res
      foundLfns = []
      allAvailable = True
      for fileDict in res['Value']:
        fileIDs.append( fileDict['FileID'] )
        runID = fileDict.get( 'RunNumber' )
        if not runID:
          runID = 0
        lfn = fileDict['LFN']
        foundLfns.append( lfn )
        if fileDict['Status'] != 'Unused':
          allAvailable = False
          gLogger.error( "Supplied file not in Unused status but %s" % fileDict['Status'], lfn )
      for lfn in lfns:
        if not lfn in foundLfns:
          allAvailable = False
          gLogger.error( "Supplied file not found for transformation" % lfn )
      if not allAvailable:
        return S_ERROR( "Not all supplied files available in the transformation database" )

    # Insert the task into the jobs table and retrieve the taskID
    self.lock.acquire()
    req = "INSERT INTO TransformationTasks(TransformationID, ExternalStatus, ExternalID, \
    TargetSE, CreationTime, LastUpdateTime, RunNumber) VALUES\
     (%d,'%s','%d','%s', UTC_TIMESTAMP(), UTC_TIMESTAMP(),%d);" % ( transID, 'Created', 0, se, runID )
    res = self._update( req, connection )
    if not res['OK']:
      self.lock.release()
      gLogger.error( "Failed to publish task for transformation", res['Message'] )
      return res
    res = self._query( "SELECT LAST_INSERT_ID();", connection )
    self.lock.release()
    if not res['OK']:
      return res
    taskID = int( res['Value'][0][0] )
    gLogger.verbose( "Published task %d for transformation %d." % ( taskID, transID ) )
    # If we have input data then update their status, and taskID in the transformation table
    if lfns:
      res = self.__insertTaskInputs( transID, taskID, lfns, connection = connection )
      if not res['OK']:
        self.__removeTransformationTask( transID, taskID, connection = connection )
        return res
      res = self.__assignTransformationFile( transID, taskID, se, fileIDs, connection = connection )
      if not res['OK']:
        self.__removeTransformationTask( transID, taskID, connection = connection )
        return res
    return S_OK( taskID )

  #############################################################################
  #
  # Managing the TransformationRuns table
  #

  def getTransformationRuns( self, condDict = {}, older = None, newer = None, timeStamp = 'LastUpdate',
                             orderAttribute = None, limit = None, connection = False ):
    connection = self.__getConnection( connection )
    selectDict = {}
    for key in condDict.keys():
      if key in self.transRunParams:
        selectDict[key] = condDict[key]
    req = "SELECT %s FROM TransformationRuns %s" % ( intListToString( self.transRunParams ),
                                                     self.buildCondition( selectDict,
                                                                          older = older,
                                                                          newer = newer,
                                                                          timeStamp = timeStamp,
                                                                          orderAttribute = orderAttribute,
                                                                          limit = limit ) )
    res = self._query( req, connection )
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
        if type( item ) not in [IntType, LongType]:
          rList.append( str( item ) )
        else:
          rList.append( item )
      webList.append( rList )
      resultList.append( transDict )
    result = S_OK( resultList )
    result['Records'] = webList
    result['ParameterNames'] = copy.copy( self.transRunParams )
    return result

  def getTransformationRunStats( self, transIDs, connection = False ):
    connection = self.__getConnection( connection )
    res = self.getCounters( 'TransformationFiles',
                            ['TransformationID', 'RunNumber', 'Status'],
                            {'TransformationID':transIDs} )
    if not res['OK']:
      return res
    transRunStatusDict = {}
    for attrDict, count in res['Value']:
      transID = attrDict['TransformationID']
      runID = attrDict['RunNumber']
      status = attrDict['Status']
      if not transRunStatusDict.has_key( transID ):
        transRunStatusDict[transID] = {}
      if not transRunStatusDict[transID].has_key( runID ):
        transRunStatusDict[transID][runID] = {}
        transRunStatusDict[transID][runID]['Total'] = 0
      transRunStatusDict[transID][runID][status] = count
      transRunStatusDict[transID][runID]['Total'] += count
    return S_OK( transRunStatusDict )

  def addTransformationRunFiles( self, transName, runID, lfns, connection = False ):
    """ Add the RunID to the TransformationFiles table """
    if not lfns:
      return S_ERROR( 'Zero length LFN list' )
    res = self._getConnectionTransID( connection, transName )
    if not res['OK']:
      return res
    connection = res['Value']['Connection']
    transID = res['Value']['TransformationID']
    res = self.__getFileIDsForLfns( lfns, connection = connection )
    if not res['OK']:
      return res
    fileIDs, lfnFilesIDs = res['Value']
    req = "UPDATE TransformationFiles SET RunNumber = %d \
    WHERE TransformationID = %d AND FileID IN (%s)" % ( runID, transID, intListToString( fileIDs.keys() ) )
    res = self._update( req, connection )
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
    req = "UPDATE TransformationRuns SET LastUpdate = UTC_TIMESTAMP() \
    WHERE TransformationID = %d and RunNumber = %d" % ( transID, runID )
    res = self._update( req, connection )
    if not res['OK']:
      gLogger.error( "Failed to update TransformationRuns table with LastUpdate", res['Message'] )
    elif not res['Value']:
      self.__insertTransformationRun( transID, runID, connection = connection )
    resDict = {'Successful':successful, 'Failed':failed}
    return S_OK( resDict )

  def setTransformationRunStatus( self, transName, runIDs, status, connection = False ):
    if not runIDs:
      return S_OK()
    if type( runIDs ) != ListType:
      runIDs = [runIDs]
    res = self._getConnectionTransID( connection, transName )
    if not res['OK']:
      return res
    connection = res['Value']['Connection']
    transID = res['Value']['TransformationID']
    req = "UPDATE TransformationRuns SET Status = '%s', LastUpdate = UTC_TIMESTAMP() \
    WHERE TransformationID = %d and RunNumber in (%s)" % ( status, transID, intListToString( runIDs ) )
    res = self._update( req, connection )
    if not res['OK']:
      gLogger.error( "Failed to update TransformationRuns table with Status", res['Message'] )
    return res

  def setTransformationRunsSite( self, transName, runID, selectedSite, connection = False ):
    res = self._getConnectionTransID( connection, transName )
    if not res['OK']:
      return res
    connection = res['Value']['Connection']
    transID = res['Value']['TransformationID']
    req = "UPDATE TransformationRuns SET SelectedSite = '%s', LastUpdate = UTC_TIMESTAMP() \
    WHERE TransformationID = %d and RunNumber = %d" % ( selectedSite, transID, runID )
    res = self._update( req, connection )
    if not res['OK']:
      gLogger.error( "Failed to update TransformationRuns table with SelectedSite", res['Message'] )
    elif not res['Value']:
      res = self.__insertTransformationRun( transID, runID, selectedSite = selectedSite, connection = connection )
    return res

  def __insertTransformationRun( self, transID, runID, selectedSite = '', connection = False ):
    req = "INSERT INTO TransformationRuns (TransformationID,RunNumber,Status,LastUpdate) \
    VALUES (%d,%d,'Active',UTC_TIMESTAMP())" % ( transID, runID )
    if selectedSite:
      req = "INSERT INTO TransformationRuns (TransformationID,RunNumber,SelectedSite,Status,LastUpdate) \
      VALUES (%d,%d,'%s','Active',UTC_TIMESTAMP())" % ( transID, runID, selectedSite )
    res = self._update( req, connection )
    if not res['OK']:
      gLogger.error( "Failed to insert to TransformationRuns table", res['Message'] )
    return res

  #############################################################################
  #
  # Managing the RunsMetadata table
  #

  def setRunsMetadata( self, runID, metadataDict, connection = False ):
    """ Add the metadataDict to runID (if already present, does nothing)
    """
    connection = self.__getConnection( connection )
    for name, value in metadataDict.items():
      res = self.__insertRunMetadata( runID, name, value, connection )
      if not res['OK']:
        return res
    return S_OK()

  def __insertRunMetadata( self, runID, name, value, connection ):
    if type( runID ) in StringTypes:
      runID = int( runID )
    req = "INSERT INTO RunsMetadata (RunNumber, Name, Value) VALUES(%d, '%s', '%s')" % ( runID, name, value )
    res = self._update( req, connection )
    if not res['OK']:
      if '1062: Duplicate entry' in res['Message']:
        gLogger.debug( "Failed to insert to RunsMetadata table: %s" % res['Message'] )
        return S_OK()
      else:
        gLogger.error( "Failed to insert to RunsMetadata table", res['Message'] )
        return res
    gLogger.info( "Inserted %s %s of run %d to RunsMetadata table" % ( name, value, runID ) )
    return S_OK()

  def getRunsMetadata( self, runIDs, connection = False ):
    """ get meta of a run. RunIDs can be a list.  
    """
    connection = self.__getConnection( connection )
    if type( runIDs ) in ( StringTypes, IntType ):
      runIDs = [runIDs]
    runIDs = [str( x ) for x in runIDs]
    runIDs = ', '.join( runIDs )
    req = "SELECT * FROM RunsMetadata WHERE RunNumber IN (%s)" % runIDs
    res = self._query( req, connection )
    if not res['OK']:
      gLogger.error( "Failure executing %s" % str( req ) )
      return res
    else:
      res = res['Value']
      dictOfNameValue = {}
      for t in res:
        if t[0] in dictOfNameValue.keys():
          dictOfNameValue[t[0]].update( {t[1]:t[2]} )
        else:
          dictOfNameValue[t[0]] = {t[1]:t[2]}
      return S_OK( dictOfNameValue )

  def deleteRunsMetadata( self, runIDs, connection = False ):
    """ delete meta of a run. RunIDs can be a list.  
    """
    connection = self.__getConnection( connection )
    if type( runIDs ) in ( StringTypes, IntType ):
      runIDs = [runIDs]
    runIDs = [str( x ) for x in runIDs]
    runIDs = ', '.join( runIDs )
    req = "DELETE FROM RunsMetadata WHERE RunNumber IN (%s)" % runIDs
    res = self._update( req, connection )
    if not res['OK']:
      gLogger.error( "Failure executing %s" % str( req ) )
      return res
    return S_OK()

  def getRunsInCache( self, connection = False ):
    """ get which runNumnber are cached
    """
    connection = self.__getConnection( connection )
    req = "SELECT DISTINCT RunNumber FROM RunsMetadata"
    res = self._query( req, connection )
    if not res['OK']:
      gLogger.error( "Failure executing %s" % str( req ) )
      return res
    else:
      return S_OK( [run[0] for run in res['Value']] )

  #############################################################################
