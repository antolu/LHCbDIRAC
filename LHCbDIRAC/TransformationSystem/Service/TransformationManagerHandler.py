""" DISET request handler for the LHCbDIRAC/TransformationDB. """

__RCSID__ = "$Id$"

from types import LongType, IntType, StringType, DictType, ListType, StringTypes, BooleanType

from DIRAC import S_OK, S_ERROR
from DIRAC.TransformationSystem.Service.TransformationManagerHandler import TransformationManagerHandler as TManagerBase
from LHCbDIRAC.TransformationSystem.DB.TransformationDB import TransformationDB

database = False
def initializeTransformationManagerHandler( serviceInfo ):
  global database
  database = TransformationDB( 'TransformationDB', 'Transformation/TransformationDB' )
  return S_OK()

class TransformationManagerHandler( TManagerBase ):

  def __init__( self, *args, **kargs ):
    """ c'tor
    """
    self.setDatabase( database )
    TManagerBase.__init__( self, *args, **kargs )

  types_deleteTransformation = [[LongType, IntType]]
  def export_deleteTransformation( self, transID ):
    return database.deleteTransformation( transID, author = self.getRemoteCredentials()[ 'DN' ] )

  types_setHotFlag = [[LongType, IntType], BooleanType]
  def export_setHotFlag( self, transID, hotFlag ):
    return database.setHotFlag( transID, hotFlag )

  #############################################################################
  #
  # Managing the BkQueries table
  #


  types_addBookkeepingQuery = [ [LongType, IntType], DictType ]
  def export_addBookkeepingQuery( self, transID, queryDict ):
    return database.addBookkeepingQuery( transID, queryDict )

  types_deleteBookkeepingQuery = [ [LongType, IntType] ]
  def export_deleteBookkeepingQuery( self, transID ):
    return database.deleteBookkeepingQuery( transID )

  types_getBookkeepingQuery = [ [LongType, IntType] ]
  def export_getBookkeepingQuery( self, transID ):
    return database.getBookkeepingQuery( transID )
  
  types_getTransformationsWithBkQueries = [ ListType ]
  def export_getTransformationsWithBkQueries(self, transIDs):
    return database.getTransformationsWithBkQueries( transIDs )
  
  types_setBookkeepingQueryEndRun = [ [LongType, IntType] , [LongType, IntType]]
  def export_setBookkeepingQueryEndRun( self, transID, runNumber ):
    return database.setBookkeepingQueryEndRun( transID, runNumber )
  
  types_setBookkeepingQueryStartRun = [ [LongType, IntType] , [LongType, IntType]]
  def export_setBookkeepingQueryStartRun( self, transID, runNumber ):
    return database.setBookkeepingQueryStartRun( transID, runNumber )
  
  types_addBookkeepingQueryRunList = [ [LongType, IntType] , [ListType]]
  def export_addBookkeepingQueryRunList( self, transID, runList ):
    return  database.addBookkeepingQueryRunList( transID, runList )

  #############################################################################
  #
  # Managing the TransformationRuns table
  #

  types_getTransformationRuns = []
  def export_getTransformationRuns( self, condDict = {}, orderAttribute = None, limit = None ):
    return database.getTransformationRuns( condDict, orderAttribute = orderAttribute, limit = limit )

  types_getTransformationRunStats = [[IntType, LongType, ListType]]
  def export_getTransformationRunStats( self, transIDs ):
    if type( transIDs ) in ( int, long ):
      transIDs = [transIDs]
    return database.getTransformationRunStats( transIDs )

  types_addTransformationRunFiles = [[LongType, IntType], [LongType, IntType], ListType]
  def export_addTransformationRunFiles( self, transID, runID, lfns ):
    return database.addTransformationRunFiles( transID, runID, lfns )

  types_setTransformationRunStatus = [[LongType, IntType], [LongType, IntType, ListType], StringTypes]
  def export_setTransformationRunStatus( self, transID, runID, status ):
    return database.setTransformationRunStatus( transID, runID, status )

  types_setTransformationRunsSite = [[LongType, IntType], [LongType, IntType], StringTypes]
  def export_setTransformationRunsSite( self, transID, runID, assignedSE ):
    return database.setTransformationRunsSite( transID, runID, assignedSE )

  types_getTransformationRunsSummaryWeb = [DictType, ListType, IntType, IntType]
  def export_getTransformationRunsSummaryWeb( self, selectDict, sortList, startItem, maxItems ):
    """ Get the summary of the transformation run information for a given page in the generic format
    """

    # Obtain the timing information from the selectDict
    last_update = selectDict.get( 'LastUpdate', None )
    if last_update:
      del selectDict['LastUpdate']
    fromDate = selectDict.get( 'FromDate', None )
    if fromDate:
      del selectDict['FromDate']
    if not fromDate:
      fromDate = last_update
    toDate = selectDict.get( 'ToDate', None )
    if toDate:
      del selectDict['ToDate']
    # Sorting instructions. Only one for the moment.
    if sortList:
      orderAttribute = sortList[0][0] + ":" + sortList[0][1]
    else:
      orderAttribute = None

    # Get the transformations that match the selection
    res = database.getTransformationRuns( condDict = selectDict, older = toDate,
                                          newer = fromDate, orderAttribute = orderAttribute )
    self._parseRes( res )

    # Prepare the standard structure now within the resultDict dictionary
    resultDict = {}
    trList = res['Records']
    # Create the total records entry
    nTrans = len( trList )
    resultDict['TotalRecords'] = nTrans
    # Create the ParameterNames entry
    paramNames = res['ParameterNames']
    resultDict['ParameterNames'] = paramNames

    # Add the job states to the ParameterNames entry
    #taskStateNames   = ['Created','Running','Submitted','Failed','Waiting','Done','Stalled']
    #resultDict['ParameterNames'] += ['Jobs_'+x for x in taskStateNames]
    # Add the file states to the ParameterNames entry
    fileStateNames = ['PercentProcessed', 'Processed', 'Unused', 'Assigned',
                      'Total', 'Problematic', 'ApplicationCrash', 'MaxReset']
    resultDict['ParameterNames'] += ['Files_' + x for x in fileStateNames]

    # Get the transformations which are within the selected window
    if nTrans == 0:
      return S_OK( resultDict )
    ini = startItem
    last = ini + maxItems
    if ini >= nTrans:
      return S_ERROR( 'Item number out of range' )
    if last > nTrans:
      last = nTrans
    transList = trList[ini:last]
    if not transList:
      return S_OK( resultDict )

    # Obtain the run statistics for the requested transformations
    transIDs = []
    for transRun in transList:
      transRunDict = dict( zip( paramNames, transRun ) )
      transID = int( transRunDict['TransformationID'] )
      if not transID in transIDs:
        transIDs.append( transID )
    res = database.getTransformationRunStats( transIDs )
    if not res['OK']:
      return res
    transRunStatusDict = res['Value']

    statusDict = {}
    # Add specific information for each selected transformation/run
    for transRun in transList:
      transRunDict = dict( zip( paramNames, transRun ) )
      transID = transRunDict['TransformationID']
      runID = transRunDict['RunNumber']
      if transID not in transRunStatusDict or runID not in transRunStatusDict[transID]:
        for state in fileStateNames:
          transRun.append( 0 )
        continue
      # Update the status counters
      status = transRunDict['Status']
      if not statusDict.has_key( status ):
        statusDict[status] = 0
      statusDict[status] += 1

      # Populate the run file statistics
      fileDict = transRunStatusDict[transID][runID]
      if fileDict['Total'] == 0:
        fileDict['PercentProcessed'] = 0
      else:
        processed = fileDict.get( 'Processed', 0 )
        fileDict['PercentProcessed'] = "%.1f" % ( int( processed * 1000. / fileDict['Total'] ) / 10. )
      for state in fileStateNames:
        if fileDict and state in fileDict:
          transRun.append( fileDict[state] )
        else:
          transRun.append( 0 )

      # Get the statistics on the number of jobs for the transformation
      #res = database.getTransformationTaskRunStats(transID)
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
    return S_OK( resultDict )


  #############################################################################
  #
  # Managing the RunsMetadata table
  #

  types_addRunsMetadata = [[LongType, IntType], DictType]
  def export_addRunsMetadata( self, runID, metadataDict ):
    """ insert run metadata
    """
    return database.setRunsMetadata( runID, metadataDict )

  types_updateRunsMetadata = [[LongType, IntType], DictType]
  def export_updateRunsMetadata( self, runID, metadataDict ):
    """ insert run metadata
    """
    return database.updateRunsMetadata( runID, metadataDict )

  types_getRunsMetadata = [[LongType, IntType]]
  def export_getRunsMetadata( self, runID ):
    """ retrieve run metadata
    """
    return database.getRunsMetadata( runID )

  types_deleteRunsMetadata = [[LongType, IntType]]
  def export_deleteRunsMetadata( self, runID ):
    """ delete run metadata
    """
    return database.deleteRunsMetadata( runID )

  types_getRunsInCache = [DictType]
  def export_getRunsInCache( self, condDict ):
    """ gets what's in
    """
    return database.getRunsInCache( condDict )
