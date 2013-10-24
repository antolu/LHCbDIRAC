""" DIRAC Transformation DB

    Transformation database is used to collect and serve the necessary information
    in order to automate the task of job preparation for high level transformations.
    This class is typically used as a base class for more specific data processing
    databases
"""

import re

from DIRAC                                              import gLogger, S_OK, S_ERROR
from DIRAC.TransformationSystem.DB.TransformationDB     import TransformationDB as DIRACTransformationDB
from DIRAC.Core.Utilities.List                          import intListToString, breakListIntoChunks
from LHCbDIRAC.Workflow.Utilities.Utils                 import makeRunList
import threading, copy
from types import IntType, LongType, FloatType, ListType, TupleType, StringTypes

MAX_ERROR_COUNT = 3

class TransformationDB( DIRACTransformationDB ):
  """ Extension of the DIRAC Transformation DB
  """

  def __init__( self, dbname = None, dbconfig = None, maxQueueSize = 10, dbIn = None ):
    """ The standard constructor takes the database name (dbname) and the name of the
        configuration section (dbconfig)
    """
    DIRACTransformationDB.__init__( self, dbname, dbconfig, maxQueueSize, dbIn )
    self.lock = threading.Lock()
    self.dbname = dbname
    self.queryFields = ['SimulationConditions', 'DataTakingConditions', 'ProcessingPass', 'FileType',
                        'EventType', 'ConfigName', 'ConfigVersion', 'ProductionID', 'DataQualityFlag',
                         'StartRun', 'EndRun', 'Visible', 'RunNumbers', 'TCK']
    self.intFields = ['EventType', 'ProductionID', 'StartRun', 'EndRun']
    self.transRunParams = ['TransformationID', 'RunNumber', 'SelectedSite', 'Status', 'LastUpdate']
    self.allowedStatusForTasks = ( 'Unused', 'ProbInFC' )
    self.TRANSFILEPARAMS.append( "RunNumber" )
    self.TASKSPARAMS.append( "RunNumber" )

  def _generateTables( self ):
    """ _generateTables

    Extension of TransformationDB ( DIRAC ) adding tables BkQueries, TransformationRuns
    and RunsMetadata. It is also modifying TransformationFiles and TransformationTasks.
    """

    tables = DIRACTransformationDB._generateTables( self )
    if not tables[ 'OK' ]:
      return tables
    tables = tables[ 'Value' ]

    # Add a column to TransformationFiles table
    if 'TransformationFiles' in tables:
      tables['TransformationFiles' ][ 'Fields' ][ 'RunNumber' ] = 'INT(11) DEFAULT 0'

    # Add a column to TransformationTasks table
    if 'TransformationTasks' in tables:
      tables['TransformationTasks' ][ 'Fields' ][ 'RunNumber' ] = 'INT(11) DEFAULT 0'

    # Creates new table BkQueries
    if 'BkQueries' not in tables:
      _bkQueries = {
                       'Fields'     : {
                                       'BkQueryID'            : 'INT(11) NOT NULL AUTO_INCREMENT',
                                       'SimulationConditions' : 'VARCHAR(512) NOT NULL DEFAULT "All"',
                                       'DataTakingConditions' : 'VARCHAR(512) NOT NULL DEFAULT "All"',
                                       'ProcessingPass'       : 'VARCHAR(512) NOT NULL DEFAULT "All"',
                                       'FileType'             : 'VARCHAR(512) NOT NULL DEFAULT "All"',
                                       'EventType'            : 'VARCHAR(512) NOT NULL DEFAULT "All"',
                                       'ConfigName'           : 'VARCHAR(512) NOT NULL DEFAULT "All"',
                                       'ConfigVersion'        : 'VARCHAR(512) NOT NULL DEFAULT "All"',
                                       'ProductionID'         : 'VARCHAR(512) NOT NULL DEFAULT "All"',
                                       'DataQualityFlag'      : 'VARCHAR(512) NOT NULL DEFAULT "All"',
                                       'StartRun'             : 'INT(11) NOT NULL DEFAULT 0',
                                       'EndRun'               : 'INT(11) NOT NULL DEFAULT 0',
                                       'RunNumbers'           : 'BLOB NOT NULL DEFAULT "All"',
                                       'TCK'                  : 'VARCHAR(512) NOT NULL DEFAULT "All"',
                                       'Visible'              : 'VARCHAR(8) NOT NULL DEFAULT "All"'
                                      },
                       'Indexes'    : {
                                       'SimulationConditions' : [ 'SimulationConditions' ],
                                       'DataTakingConditions' : [ 'DataTakingConditions' ],
                                       'ProcessingPass'       : [ 'ProcessingPass' ],
                                       'FileType'             : [ 'FileType' ],
                                       'EventType'            : [ 'EventType' ],
                                       'ConfigName'           : [ 'ConfigName' ],
                                       'ConfigVersion'        : [ 'ConfigVersion' ],
                                       'ProductionID'         : [ 'ProductionID' ],
                                       'DataQualityFlag'      : [ 'DataQualityFlag' ],
                                       'StartRun'             : [ 'StartRun' ],
                                       'EndRun'               : [ 'EndRun' ],
                                       'RunNumbers'           : [ 'RunNumbers' ],
                                       'TCK'                  : [ 'TCK' ]
                                      },
                       'PrimaryKey' : [ 'BkQueryID' ],
                       'Engine'     : 'MyISAM'
                      }
      tables[ 'BkQueries' ] = _bkQueries

    # Creates new table TransformationRuns
    if 'TransformationRuns' not in tables:
      _transformationRuns = {
                             'Fields'     : {
                                             'TransformationID' : 'INTEGER NOT NULL',
                                             'RunNumber'        : 'INT(11) NOT NULL',
                                             'SelectedSite'     : 'VARCHAR(256) DEFAULT ""',
                                             'Status'           : 'CHAR(32) DEFAULT "Active"',
                                             'LastUpdate'       : 'DATETIME'
                                             },
                             'Indexes'    : {
                                             'TransformationID': [ 'TransformationID' ],
                                             'RunNumber'       : [ 'RunNumber' ]
                                            },
                             'PrimaryKey' : [ 'TransformationID', 'RunNumber' ],
                             'Engine'     : 'MyISAM'
                            }
      tables[ 'TransformationRuns' ] = _transformationRuns

    # Creates new table RunsMetadata
    if 'RunsMetadata' not in tables:
      _runsMetadata = {
                       'Fields'     : {
                                       'RunNumber' : 'INT(11) NOT NULL',
                                       'Name'      : 'VARCHAR(256) NOT NULL',
                                       'Value'     : 'VARCHAR(256) NOT NULL'
                                      },
                       'Indexes'    : {
                                       'RunNumber' : [ 'RunNumber' ]
                                      },
                       'PrimaryKey' : [ 'RunNumber', 'Name' ],
                       'Engine'     : 'MyISAM'
                      }
      tables[ 'RunsMetadata' ] = _runsMetadata

    return S_OK( tables )

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

    runIDsToBeDeleted = list( set( runsMaybeToDelete ) - set( runsToKeep ) )

    if runIDsToBeDeleted:
      res = self.deleteRunsMetadata( {'RunNumber':runIDsToBeDeleted, 'Name':['TCK', 'CondDb', 'DDDB']}, connection )
      if not res['OK']:
        return res

    res = DIRACTransformationDB.cleanTransformation( self, transName, connection = connection )
    if not res['OK']:
      return res

    res = self.__cleanTransformationRuns( transID, connection = connection )
    if not res['OK']:
      return res
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
      run = taskForSumbission.get( 'RunNumber' )
      if run and run not in runNumbers:
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
      print "not res ", res  # canc
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


  def setBookkeepingQueryEndRunForTransformation( self, transName, runNumber, connection = False ):
    """ Set the EndRun for the supplied transformation """
    res = self._getConnectionTransID( connection, transName )
    if not res['OK']:
      return res
    connection = res['Value']['Connection']
    transID = res['Value']['TransformationID']
    res = self.__getTransformationBkQueryID( transID, connection = connection )
    if not res['OK']:
      return res
    bkQueryID = res['Value']
    res = self.__getBookkeepingQuery( bkQueryID, connection = connection )
    if not res['OK']:
      return res
    startRun = res['Value'].get( 'StartRun' )
    endRun = res['Value'].get( 'EndRun' )
    if endRun and runNumber < endRun:
      return S_ERROR( "EndRun can not be reduced!" )
    if startRun and startRun > runNumber:
      return S_ERROR( "EndRun is before StartRun!" )
    req = "UPDATE BkQueries SET EndRun = %d WHERE BkQueryID = %d" % ( runNumber, bkQueryID )
    return self._update( req, connection )

  def setBookkeepingQueryStartRunForTransformation( self, transName, runNumber, connection = False ):
    """ Set the StartRun for the supplied transformation """
    res = self._getConnectionTransID( connection, transName )
    if not res['OK']:
      return res
    connection = res['Value']['Connection']
    transID = res['Value']['TransformationID']
    res = self.__getTransformationBkQueryID( transID, connection = connection )
    if not res['OK']:
      return res
    bkQueryID = res['Value']
    res = self.__getBookkeepingQuery( bkQueryID, connection = connection )
    if not res['OK']:
      return res
    endRun = res['Value'].get( 'EndRun' )
    startRun = res['Value'].get( 'StartRun' )
    if startRun and runNumber > startRun:
      return S_ERROR( "StartRun can not be increased!" )
    if endRun and runNumber > endRun:
      return S_ERROR( "StartRun cannot be after EndRun!" )
    req = "UPDATE BkQueries SET StartRun = %d WHERE BkQueryID = %d" % ( runNumber, bkQueryID )
    return self._update( req, connection )

  def addBookkeepingQueryRunListTransformation( self, transName, runList, connection = False ):
    """ Adds the list of runs
    """
    res = self._getConnectionTransID( connection, transName )
    if not res['OK']:
      return S_ERROR( "Failed to get Connection to TransformationDB" )
    connection = res['Value']['Connection']
    transID = res['Value']['TransformationID']
    res = self.__getTransformationBkQueryID( transID, connection = connection )
    if not res['OK']:
      return S_ERROR( "Cannot retrieve BkQueryID" )
    bkQueryID = res['Value']
    res = self.__getBookkeepingQuery( bkQueryID, connection = connection )
    if not res['OK']:
      return S_ERROR( "Cannot retrieve BkQuery" )
    if isinstance( res['Value']['RunNumbers'], str ):
      runInQuery = [res['Value']['RunNumbers']]
    else:
      runInQuery = res['Value']['RunNumbers']
    res = makeRunList( str( runList ) )
    runs = res['Value']
    for run in runs:
      if run not in runInQuery:
        runInQuery.append( run )
    runInQuery.sort()
    if len( runInQuery ) > 999:
      return S_ERROR( "RunList bigger the 1000 not allowed because of Oracle limitations!!!" )
    value = ';;;'.join( runInQuery )
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
    """ Set the bookkeeping query ID of the transformation specified by transID
    """
    return self.setTransformationParameter( transName, 'BkQueryID', bkQueryID, author = author,
                                            connection = connection )


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

  def __insertExistingTransformationFiles( self, transID, fileTuplesList, connection = False ):
    """ extends DIRAC.__insertExistingTransformationFiles
        Does not add userSE and adds runNumber
    """

    gLogger.info( "Inserting %d files in TransformationFiles" % len( fileTuplesList ) )
    # splitting in various chunks, in case it is too big
    for fileTuples in breakListIntoChunks( fileTuplesList, 10000 ):
      gLogger.verbose( "Adding first %d files in TransformationFiles (out of %d)" % ( len( fileTuples ),
                                                                                      len( fileTuplesList ) ) )
      req = "INSERT INTO TransformationFiles (TransformationID,Status,TaskID,FileID,TargetSE,LastUpdate,RunNumber) VALUES"
      candidates = False

      for ft in fileTuples:
        _lfn, originalID, fileID, status, taskID, targetSE, _usedSE, _errorCount, _lastUpdate, _insertTime, runNumber = ft[:11]
        if status not in ( 'Unused', 'Removed' ):
          candidates = True
          if not re.search( '-', status ):
            status = "%s-inherited" % status
            if taskID:
              taskID = str( int( originalID ) ).zfill( 8 ) + '_' + str( int( taskID ) ).zfill( 8 )
          req = "%s (%d,'%s','%s',%d,'%s',UTC_TIMESTAMP(),%d)," % ( req, transID, status, taskID, fileID,
                                                                    targetSE, runNumber )
      if not candidates:
        continue
      req = req.rstrip( "," )
      res = self._update( req, connection )
      if not res['OK']:
        return res

    return S_OK()

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
      foundLfns = set()
      for fileDict in res['Value']:
        fileIDs.append( fileDict['FileID'] )
        runID = fileDict.get( 'RunNumber' )
        if not runID:
          runID = 0
        lfn = fileDict['LFN']
        if fileDict['Status'] in self.allowedStatusForTasks:
          foundLfns.add( lfn )
        else:
          gLogger.error( "Supplied file not in %s status but %s" % ( self.allowedStatusForTasks, fileDict['Status'] ), lfn )
      unavailableLfns = set( lfns ) - foundLfns
      if unavailableLfns:
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

  def getTransformationRuns( self, condDict = None, older = None, newer = None, timeStamp = 'LastUpdate',
                             orderAttribute = None, limit = None, connection = False ):
    """ Gets the transformation runs registered (usual query)
    """

    connection = self.__getConnection( connection )
    selectDict = {}
    if condDict:
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
    """ Gets counters of runs (taken from the TransformationFiles table)
    """
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
      transRunStatusDict[transID][runID]['Total'] = transRunStatusDict.setdefault( transID, {} ).setdefault( runID, {} ).setdefault( 'Total', 0 ) + count
      transRunStatusDict[transID][runID][status] = count
    return S_OK( transRunStatusDict )

  def addTransformationRunFiles( self, transName, runID, lfns, connection = False ):
    """ Adds the RunID to the TransformationFiles table
    """
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
    fileIDs, _lfnFilesIDs = res['Value']
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
    """ Sets a status in the TransformationRuns table
    """
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
    """ Sets the site for Transformation Runs
    """
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
    """ Inserts a new Run
    """
    req = "INSERT INTO TransformationRuns (TransformationID,RunNumber,Status,LastUpdate) \
    VALUES (%d,%d,'Active',UTC_TIMESTAMP())" % ( transID, runID )
    if selectedSite:
      req = "INSERT INTO TransformationRuns (TransformationID,RunNumber,SelectedSite,Status,LastUpdate) \
      VALUES (%d,%d,'%s','Active',UTC_TIMESTAMP())" % ( transID, runID, selectedSite )
    res = self._update( req, connection )
    if not res['OK']:
      gLogger.error( "Failed to insert to TransformationRuns table", res['Message'] )
    return res

  def __cleanTransformationRuns( self, transID, connection = False ):
    req = "DELETE  FROM TransformationRuns WHERE TransformationID = %s" % transID
    res = self._update( req , connection )
    if not res['OK']:
      gLogger.error( "Failure executing %s" % str( req ) )
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

  def updateRunsMetadata( self, runID, metadataDict, connection = False ):
    """ Add the metadataDict to runID (if already present, does nothing)
    """
    connection = self.__getConnection( connection )
    for name, value in metadataDict.items():
      res = self.__updateRunMetadata( runID, name, value, connection )
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

  def __updateRunMetadata( self, runID, name, value, connection ):
    if type( runID ) in StringTypes:
      runID = int( runID )
    req = "UPDATE RunsMetadata SET Value = %s WHERE RunNumber = %d AND Name = '%s'" % ( value , runID, name )
    res = self._update( req, connection )
    if not res['OK']:
      gLogger.error( "Failed to update RunsMetadata table", res['Message'] )
      return res
    gLogger.info( "Updated %s %s of run %d in RunsMetadata table" % ( name, value, runID ) )
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
        dictOfNameValue.setdefault( t[0], {} ).update( {t[1]:t[2]} )
      return S_OK( dictOfNameValue )

  def deleteRunsMetadata( self, condDict = None, connection = False ):
    """ delete meta of a run. RunIDs can be a list.
    """
    connection = self.__getConnection( connection )
    req = "DELETE FROM RunsMetadata %s" % self.buildCondition( condDict )
    res = self._update( req, connection )
    if not res['OK']:
      gLogger.error( "Failure executing %s" % str( req ) )
      return res
    return S_OK()

  def getRunsInCache( self, condDict = None, connection = False ):
    """ get which runNumnber are cached
    """
    connection = self.__getConnection( connection )
    req = "SELECT DISTINCT RunNumber FROM RunsMetadata %s" % self.buildCondition( condDict )
    res = self._query( req, connection )
    if not res['OK']:
      gLogger.error( "Failure executing %s" % str( req ) )
      return res
    else:
      return S_OK( [run[0] for run in res['Value']] )

  #############################################################################
