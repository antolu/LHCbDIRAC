""" DISET request handler for the LHCbDIRAC/TransformationDB. """

__RCSID__ = "$Id$"

from types import LongType, IntType, StringType, DictType, ListType, StringTypes

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

  #############################################################################
  #
  # Managing the BkQueries table
  #

  types_createTransformationQuery = [ [LongType, IntType, StringType], DictType ]
  def export_createTransformationQuery( self, transName, queryDict ):
    credDict = self.getRemoteCredentials()
    authorDN = credDict[ 'DN' ]
    res = database.createTransformationQuery( transName, queryDict, author = authorDN )
    return self._parseRes( res )

  types_deleteTransformationBookkeepingQuery = [ [LongType, IntType, StringType] ]
  def export_deleteTransformationBookkeepingQuery( self, transName ):
    credDict = self.getRemoteCredentials()
    authorDN = credDict[ 'DN' ]
    res = database.deleteTransformationBookkeepingQuery( transName, author = authorDN )
    return self._parseRes( res )

  types_deleteBookkeepingQuery = [ [LongType, IntType] ]
  def export_deleteBookkeepingQuery( self, queryID ):
    res = database.deleteBookkeepingQuery( queryID )
    return self._parseRes( res )

  types_getBookkeepingQueryForTransformation = [ [LongType, IntType, StringType] ]
  def export_getBookkeepingQueryForTransformation( self, transName ):
    res = database.getBookkeepingQueryForTransformation( transName )
    return self._parseRes( res )

  types_setBookkeepingQueryEndRunForTransformation = [ [LongType, IntType, StringType] , [LongType, IntType]]
  def export_setBookkeepingQueryEndRunForTransformation( self, transName, runNumber ):
    res = database.setBookkeepingQueryEndRunForTransformation( transName, runNumber )
    return self._parseRes( res )

  types_setBookkeepingQueryStartRunForTransformation = [ [LongType, IntType, StringType] , [LongType, IntType]]
  def export_setBookkeepingQueryStartRunForTransformation( self, transName, runNumber ):
    res = database.setBookkeepingQueryStartRunForTransformation( transName, runNumber )
    return self._parseRes( res )

  types_addBookkeepingQueryRunListTransformation = [ [LongType, IntType, StringType] , [StringType]]
  def export_addBookkeepingQueryRunListTransformation( self, transName, runList ):
    res = database.addBookkeepingQueryRunListTransformation( transName, runList )
    return self._parseRes( res )

  #############################################################################
  #
  # Managing the TransformationRuns table
  #

  types_getTransformationRuns = []
  def export_getTransformationRuns( self, condDict = {}, orderAttribute = None, limit = None ):
    res = database.getTransformationRuns( condDict = condDict, orderAttribute = orderAttribute, limit = limit )
    return self._parseRes( res )

  types_getTransformationRunStats = [ListType]
  def export_getTransformationRunStats( self, transIDs ):
    res = database.getTransformationRunStats( transIDs )
    return self._parseRes( res )

#  types_getTransformationRunsSummaryWeb = [DictType, ListType, IntType, IntType]
#  def export_getTransformationRunsSummaryWeb(self,selectDict,sortList,startItem,maxItems):
#    return self.__getTableSummaryWeb('TransformationRuns',selectDict,sortList,startItem,maxItems,
#                                     selectColumns=['TransformationID','RunNumber','SelectedSite','Status'],
#                                     timeStamp='LastUpdate',statusColumn='Status')

  types_addTransformationRunFiles = [[LongType, IntType, StringType], [LongType, IntType], ListType]
  def export_addTransformationRunFiles( self, transName, runID, lfns ):
    return database.addTransformationRunFiles( transName, runID, lfns )

  types_setTransformationRunStatus = [[LongType, IntType, StringType], [LongType, IntType, ListType], StringTypes]
  def export_setTransformationRunStatus( self, transID, runID, status ):
    return database.setTransformationRunStatus( transID, runID, status )

  types_setTransformationRunsSite = [[LongType, IntType, StringType], [LongType, IntType], StringTypes]
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
    if not res['OK']:
      return self._parseRes( res )

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
