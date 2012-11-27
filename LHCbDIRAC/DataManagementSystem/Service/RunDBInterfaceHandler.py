########################################################################
# File: RunDBInterfaceHandler.py
########################################################################
""" :mod: RunDBInterfaceHandler
    ===========================
 
    .. module: RunDBInterfaceHandler
    :synopsis: DISET request handler base class for the DatasetDB 
"""
# imports
import os
import time
import sys
from types import DictType, ListType, IntType, StringTypes
## from DIRAC
from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Core.DISET.RequestHandler import RequestHandler

__RCSID__ = "$Id$"

allRunFields = [ 'runID', 'fillID', 'state', 'runType', 'partitionName', 'partitionID', 
                 'startTime', 'endTime', 'destination', 'startLumi', 'endLumi', 'beamEnergy']
allFileFields = ['fileID', 'runID', 'name', 'state', 'bytes', 
                 'events', 'stream', 'creationTime', 'timeStamp', 'refCount']

server = False
runStates = {}
runStateRev = {}
fileStates = {}
fileStateRev = {}

def initializeRunDBInterfaceHandler( serviceInfo ):
  global server
  #sys.path.insert(0, '/home/rainer/projects/RunDatabase/python')
  #sys.path.append( '/group/online/rundb/RunDatabase/python' )
  sys.path.append( '/admin/RunDatabase/python' )
  from path import SQL_ALCHEMY_PATH
  sys.path.append( SQL_ALCHEMY_PATH )
  try:
    ORACLE_HOME = os.environ['ORACLE_HOME']
  except:
    print 'ERROR: ORACLE_HOME environment variable should be set'
  sys.path.append( "%s" % str( ORACLE_HOME ) )
  import RunDatabase_Defines
  #print dir(RunDatabase_Defines)
  #print RunDatabase_Defines.FileFields

  from RunDatabase_Defines import RUN_STATE_TRANSLATION, FILE_STATE_TRANSLATION

  global fileStates
  global fileStateRev
  fileStates = FILE_STATE_TRANSLATION
  fileStateRev = {}
  for key, value in fileStates.items():
    fileStateRev[value] = key
  global runStates
  global runStateRev
  runStates = RUN_STATE_TRANSLATION
  runStateRev = {}
  for key, value in runStates.items():
    runStateRev[value] = key

  im = __import__( 'RunDatabase', globals(), locals(), ['*'] )
  import RunDatabase
  print im
  from DbModel import createEngine_Oracle
  try:
    server = RunDatabase.RunDbServer( engine = createEngine_Oracle() )
  except:
    print 'Failed to make an instance of runDB server'
  return S_OK()

class RunDBInterfaceHandler( RequestHandler ):
  """
  .. class:: RunDBInterfaceHandler
  """
  types_getFilesSummaryWeb = [ DictType, ListType, IntType, IntType ]
  def export_getFilesSummaryWeb( self, selectDict, sortList, startItem, maxItems ):
    """ export of getFilesSummaryWeb """
    paramString = ''
    for selectParam in allFileFields:
      if selectDict.has_key( selectParam ):
        selectValue = selectDict[selectParam]
        if selectParam == 'state':
          intStates = []
          for strState in selectValue:
            intStates.append( fileStateRev[strState] )
          selectValue = intStates
        if type( selectValue ) in StringTypes:
          paramString = "%s,%s='%s'" % ( paramString, selectParam, selectValue )
        else:
          paramString = "%s,%s=%s" % ( paramString, selectParam, selectValue )
    decending = False
    if sortList:
      paramString = "%s,orderBy='%s'" % ( paramString, sortList[0][0] )
      if sortList[0][1] == 'DESC':
        decending = True
    paramString = "%s,no=%s" % ( paramString, sys.maxint )
    if paramString:
      filesQueryString = "success,result = server.getFilesDirac(fields=allFileFields%s)" % paramString
    else:
      filesQueryString = "success,result = server.getFilesDirac(fields=allFileFields)"
    print filesQueryString
    exec( filesQueryString )
    if not success:
      return S_ERROR( result )
    resultDict = {}
    nFiles = len( result )
    resultDict['TotalRecords'] = nFiles
    if nFiles == 0:
      return S_OK( resultDict )
    if decending:
      result.reverse()

    statusCountDict = {}
    for res in result:
      state = res[3]
      if state in fileStates:
        state = fileStates[state]
      else:
        state = 'UNKNOWN'
      if state not in statusCountDict:
        statusCountDict[state] = 0
      statusCountDict[state] += 1
    resultDict['Extras'] = statusCountDict

    iniFile = startItem
    lastFile = iniFile + maxItems
    if iniFile >= nFiles:
      return S_ERROR( 'Item number out of range' )
    if lastFile > nFiles:
      lastFile = nFiles
    fileList = result[iniFile:lastFile]

    # prepare the standard structure now
    resultDict['ParameterNames'] = allFileFields
    records = []
    for fileTuple in fileList:
      ['fileID', 'runID', 'name', 'state', 'bytes', 'events', 'stream', 'creationTime', 'timeStamp', 'refCount']
      fileID, runID, name, state, bytes, events, stream, creationTime, timeStamp, refCount = fileTuple
      timeStamp = str( timeStamp )
      creationTime = str( creationTime )
      if fileStates.has_key( state ):
        state = fileStates[state]
      else:
        state = 'UNKNOWN'
      records.append( ( fileID, runID, name, state, bytes, events, stream, creationTime, timeStamp, refCount ) )

    resultDict['Records'] = records
    print resultDict
    return S_OK( resultDict )

  """
  getFiles(self, fields        = ['name'],
                     fileID        = None,
                     runID         = None,
                     name          = None,
                     stream        = None,
                     state         = None,
                     timeout       = None,
                     refCount      = None,
                     runType       = None,
                     runPartName   = None,
                     runPartID     = None,
                     runStartTime  = None,
                     runEndTime    = None,
                     runDest       = None,
                     runState      = None,
                     orderBy       = None,
                     no            = 100
              ):
  """

  types_getFileSelections = []
  def export_getFileSelections( self ):
    pass

  types_getRunsSummaryWeb = [DictType, ListType, IntType, IntType]
  def export_getRunsSummaryWeb( self, selectDict, sortList, startItem, maxItems ):
    """ export of getRunsSummaryWeb """
    paramString = ''
    for selectParam in allRunFields:
      if selectDict.has_key( selectParam ):
        selectValue = selectDict[selectParam]
        if selectParam == 'state':
          intStates = []
          for strState in selectValue:
            intStates.append( runStateRev[strState] )
          selectValue = intStates
        if type( selectValue ) in StringTypes:
          paramString = "%s,%s='%s'" % ( paramString, selectParam, selectValue )
        else:
          paramString = "%s,%s=%s" % ( paramString, selectParam, selectValue )
    decending = False
    if sortList:
      paramString = "%s,orderBy='%s'" % ( paramString, sortList[0][0] )
      if sortList[0][1] == 'DESC':
        decending = True
    paramString = "%s,no=%s" % ( paramString, sys.maxint )
    if paramString:
      jobsQueryString = "success,result = server.getRunsDirac(fields=allRunFields,runExtraParams=['magnetCurrent','magnetState']%s)" % paramString
    else:
      jobsQueryString = "success,result = server.getRunsDirac(fields=allRunFields,runExtraParams=['magnetCurrent','magnetState'])"
    print jobsQueryString
    exec( jobsQueryString )
    if not success:
      return S_ERROR( result )
    resultDict = {}
    nRuns = len( result )
    resultDict['TotalRecords'] = nRuns
    if nRuns == 0:
      return S_OK( resultDict )

    if decending:
      result.reverse()

    statusCountDict = {}
    for res in result:
      state = res[2]
      if state in runStates:
        state = runStates[state]
      else:
        state = 'UNKNOWN'
      if state not in statusCountDict:
        statusCountDict[state] = 0
      statusCountDict[state] += 1
    resultDict['Extras'] = statusCountDict

    iniRun = startItem
    lastRun = iniRun + maxItems
    if iniRun >= nRuns:
      return S_ERROR( 'Item number out of range' )
    if lastRun > nRuns:
      lastRun = nRuns
    runList = result[iniRun:lastRun]

    # prepare the standard structure now
    runCounters = {}
    for runTuple in runList:
      runID, fillID, state, runType, partitionName, partitionID, startTime, endTime, destination, startLumi, endLumi, beamEnergy, magnetCurrent, magnetState = runTuple
      runCounters[runID] = {'Size':0, 'Events':0, 'Files':0}

    # Now sum the number of events and files in the run
    if runCounters:
      success, result = server.getFilesDirac( fields = allFileFields, runID = runCounters.keys(), orderBy = 'runID', no = sys.maxint )
      if not success:
        return S_ERROR( result )
      for item in result:
        runID = item[1]
        size = item[4]
        events = item[5]
        if size and events:
          runCounters[runID]['Size'] += size
          runCounters[runID]['Events'] += events
          runCounters[runID]['Files'] += 1

    totalFiles = 0
    totalEvents = 0
    totalSize = 0
    for runID in runCounters.keys():
      totalFiles += runCounters[runID]['Files']
      totalEvents += runCounters[runID]['Events']
      totalSize += runCounters[runID]['Size']
      size = "%.2f" % ( runCounters[runID]['Size'] / ( 1000 * 1000 * 1000.0 ) )
      runCounters[runID]['Size'] = size
    resultDict['Counters'] = {'Files':totalFiles, 'Events':totalEvents, 'Size':"%.2f" % ( totalSize / ( 1000 * 1000 * 1000.0 ) )}

    records = []
    for runTuple in runList:
      runID, fillID, state, runType, partitionName, partitionID, startTime, endTime, destination, startLumi, endLumi, beamEnergy, magnetCurrent, magnetState = runTuple
      startTime = str( startTime )
      endTime = str( endTime )
      if state in runStates:
        state = runStates[state]
      else:
        state = 'UNKNOWN'
      try:
        integratedLumi = endLumi - startLumi
      except:
        integratedLumi = 'na'
      records.append( ( runID, fillID, state, runType, partitionName, partitionID, startTime, endTime, destination, startLumi, endLumi, beamEnergy, runCounters[runID]['Files'], runCounters[runID]['Events'], runCounters[runID]['Size'], magnetCurrent, magnetState, integratedLumi ) )
    resultDict['Records'] = records
    resultDict['ParameterNames'] = allRunFields + ['files', 'events', 'size', 'magnetCurrent', 'magnetState', 'integratedLumi']
    print 'parameter names: ' , resultDict['ParameterNames']
    print resultDict
    return S_OK( resultDict )

  types_getRunSelections = []
  def export_getRunSelections( self ):
    """ export of getRunSelections """
    try:
      paramDict = {}

      queries = [( 'PartitionName', 'getPartitionNames' ),
                 ( 'RunType', 'getRunTypes' ),
                 ( 'FillID', 'getFillIDs' ),
                 ( 'Destination', 'getDestinations' ),
                 ( 'StartLumi', 'getStartLumis' ),
                 ( 'EndLumi', 'getEndLumis' ),
                 ( 'BeamEnergy', 'getBeamEnergies' )]

      for key, query in queries:
        startTime = time.time()
        execString = "success,result = server.%s()" % query
        print execString
        exec( execString )
        gLogger.debug( "RunDBInterfaceHandler.getSelections: server.%s() took %.2f seconds." % ( query, time.time() - startTime ) )
        if not success:
          errStr = "RunDBInterfaceHandler.getSelections: Failed to get distinct %s." % key
          gLogger.error( errStr, result )
          return S_ERROR( errStr )
        paramDict[key] = [ res for res in results if res ]

      startTime = time.time()
      success, result = server.getRunStates()
      gLogger.debug( "RunDBInterfaceHandler.getSelections: server.getRunStates() took %.2f seconds." % ( time.time() - startTime ) )
      if not success:
        errStr = "RunDBInterfaceHandler.getSelections: Failed to get distinct run States."
        gLogger.error( errStr, result )
        return S_ERROR( errStr )
      states = []
      for runStat in result:
        if runStates.has_key( runStat ):
          states.append( runStates[runStat] )
      paramDict['State'] = states
      return S_OK( paramDict )
    except Exception, x:
      errStr = "RunDBInterfaceHandler.getSelections: Exception while obtaining possible run configurations."
      gLogger.exception( errStr, '', x )
      return S_ERROR( "%s %s" % ( errStr, x ) )

  types_getRunParams = [IntType]
  def export_getRunParams( self, runID ):
    """ export of getRunParams """
    success, result = server.getRunParams( runID )
    if not success:
      return S_ERROR( result )
    return S_OK( result )
