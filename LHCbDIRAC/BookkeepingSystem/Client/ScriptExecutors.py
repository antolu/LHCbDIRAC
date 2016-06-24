"""
Set of functions used by the DMS scripts
"""
import os

from DIRAC  import gLogger, gConfig, S_OK, exit as diracExit
from DIRAC.Core.Utilities.List import breakListIntoChunks
from DIRAC.Core.Base import Script
from DIRAC.DataManagementSystem.Client.DataManager import DataManager
from DIRAC.Resources.Catalog.FileCatalog import FileCatalog
from DIRAC.Resources.Storage.StorageElement  import StorageElement
from DIRAC.DataManagementSystem.Utilities.DMSHelpers import DMSHelpers, resolveSEGroup
from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
from LHCbDIRAC.TransformationSystem.Client.TransformationClient import TransformationClient
from LHCbDIRAC.DataManagementSystem.Client.DMScript import printDMResult, ProgressBar
from LHCbDIRAC.BookkeepingSystem.Client.BKQuery import BKQuery, parseRuns, BadRunRange

bkClient = BookkeepingClient()

__RCSID__ = "$Id$"

#==================================================================================

def executeFileMetadata( dmScript ):
  """
  Get a list of LFNs, their BK metadata and print it out
  """
  full = False
  switches = Script.getUnprocessedSwitches()
  for switch in switches:
    if switch[0] == 'Full':
      full = True

  args = Script.getPositionalArgs()
  for lfn in args:
    dmScript.setLFNsFromFile( lfn )
  lfnList = sorted( dmScript.getOption( 'LFNs', [] ) )

  if not lfnList:
    Script.showHelp()
    diracExit( 0 )

  res = bkClient.getFileMetadata( lfnList )
  if not res['OK']:
    gLogger.error( 'ERROR: Failed to get file metadata:', res['Message'] )
    diracExit( 2 )

  exitCode = 0
  lenName = max( len( lfn ) for lfn in lfnList ) + 2
  lenGUID = 38
  lenItem = 15
  sep = ''

  if not full:
    # Header: justification must match those in the loop!
    gLogger.notice( '%s %s %s %s %s %s %s' % ( 'FileName'.ljust( lenName ),
                                               'Size'.ljust( 10 ),
                                               'GUID'.ljust( lenGUID ),
                                               'Replica'.ljust( 8 ),
                                               'DataQuality'.ljust( 12 ),
                                               'RunNumber'.ljust( 10 ),
                                               '#events'.ljust( 10 ) ) )
  lfnMetadata = res['Value']['Successful']
  for lfn in lfnMetadata:
    lfnMetaDict = lfnMetadata[lfn]
    if full:
      gLogger.notice( '%s%s %s' % ( sep, 'FileName'.ljust( lenItem ), lfn ) )
      sep = '\n'
      for item in sorted( lfnMetaDict ):
        gLogger.notice( '%s %s' % ( item.ljust( lenItem ), lfnMetaDict[item] ) )
    else:
      size = lfnMetaDict['FileSize']
      guid = lfnMetaDict['GUID']
      gotReplica = lfnMetaDict['GotReplica']
      dq = lfnMetaDict.get( 'DataqualityFlag' )
      run = lfnMetaDict['RunNumber']
      evtStat = lfnMetaDict['EventStat']
      if not gotReplica:
        gotReplica = 'No'
      gLogger.notice( '%s %s %s %s %s %s %s' % ( lfn.ljust( lenName ),
                                                 str( size ).ljust( 10 ),
                                                 guid.ljust( lenGUID ),
                                                 gotReplica.ljust( 8 ),
                                                 dq.ljust( 12 ),
                                                 str( run ).ljust( 10 ),
                                                 str( evtStat ).ljust( 10 ) ) )
  failed = res['Value'].get( 'Failed', [] )
  if failed:
    gLogger.notice( '\n' )
    for lfn in failed:
      gLogger.error( 'File does not exist in the Bookkeeping.', lfn )
      exitCode = 2

  diracExit( exitCode )

#==================================================================================

def __buildPath( bkDict ):
  """
  Build a BK path from the BK dictionary
  """
  return os.path.join( '/' + bkDict['ConfigName'], bkDict['ConfigVersion'], bkDict['ConditionDescription'],
                       bkDict['ProcessingPass'][1:].replace( 'Real Data', 'RealData' ), str( bkDict['EventType'] ),
                       bkDict['FileType'] ) + ( ' (Invisible)' if bkDict['VisibilityFlag'] == 'N' else '' )

def executeFilePath( dmScript ):
  """
  Gets a list of LFNs and extracts their BK paths or other metadata
  Then print the result or group LFNs by path or metadata
  """
  full = False
  groupBy = False
  summary = False
  printList = False
  ignoreFileType = False
  switches = Script.getUnprocessedSwitches()
  for switch in switches:
    if switch[0] == 'Full':
      full = True
    elif switch[0] == 'GroupByPath':
      groupBy = 'Path'
    elif switch[0] == 'GroupByProduction':
      groupBy = 'Production'
    elif switch[0] == 'GroupBy':
      groupBy = switch[1]
    elif switch[0] == 'Summary':
      summary = True
    elif switch[0] == 'List':
      printList = True
    elif switch[0] == 'IgnoreFileType':
      ignoreFileType = True
  if summary and not groupBy:
    groupBy = 'Path'

  args = Script.getPositionalArgs()
  for lfn in args:
    dmScript.setLFNsFromFile( lfn )
  lfnList = sorted( dmScript.getOption( 'LFNs', [] ) )

  if len( lfnList ) == 0:
    Script.showHelp()
    diracExit( 0 )

  dirMetadata = ( 'Production', 'ConfigName', 'ConditionDescription', 'EventType',
                 'FileType', 'ConfigVersion', 'ProcessingPass', 'Path' )
  fileMetadata = ( 'EventType', 'FileType', 'RunNumber', 'JobId', 'DataqualityFlag', 'GotReplica' )
  if groupBy and groupBy not in dirMetadata:
    if groupBy not in fileMetadata:
      gLogger.notice( 'Invalid metata item', groupBy )
      gLogger.notice( 'Directory metadata:', ', '.join( dirMetadata ) )
      gLogger.notice( 'File metadata:', ', '.join( fileMetadata ) )
      diracExit( 1 )
    res = bkClient.getFileMetadata( lfnList )
    if res['OK']:
      paths = {'Successful':{}, 'Failed':[]}
      for lfn, metadata in res['Value']['Successful'].iteritems():
        group = metadata.get( groupBy )
        paths['Successful'].setdefault( '%s %s' % ( groupBy, group ), set() ).add( lfn )
        lfnList.remove( lfn )
      paths['Failed'].extend( lfnList )
      if summary:
        pathSummary = {'Successful':{}}
        for groupStr in paths['Successful']:
          pathSummary['Successful'][groupStr] = '%d files' % len( paths['Successful'][groupStr] )
        if paths['Failed']:
          pathSummary[ 'Failed' ] = len( paths['Failed'] )
        res = S_OK( pathSummary )
      else:
        res = S_OK( paths )
  else:
    directories = {}
    for lfn in lfnList:
      directories.setdefault( os.path.dirname( lfn ), [] ).append( lfn )

    chunkSize = 10
    progressBar = ProgressBar( len( directories ), title = 'Getting metadata for %d directories' % len( directories ),
                               chunk = chunkSize )
    success = {}
    failed = set()
    for dirChunk in breakListIntoChunks( sorted( directories ), chunkSize ):
      progressBar.loop()
      res = bkClient.getDirectoryMetadata( dirChunk )
      if not res['OK']:
        printDMResult( res )
        diracExit( 1 )
      success.update( res['Value']['Successful'] )
      failed.update( res['Value']['Failed'] )
    progressBar.endLoop()

    res = S_OK( {'Successful':success, 'Failed':failed} )
    paths = {'Successful':{}, 'Failed':{}}
    for dirName in success:
      if full:
        success[dirName] = success[dirName][0]
      else:
        bkDict = success[dirName][0]
        if ignoreFileType:
          bkDict['FileType'] = ''
        bkDict['Path'] = __buildPath( bkDict )
        if groupBy in bkDict:
          if groupBy != 'Path':
            prStr = '%s %s' % ( groupBy, bkDict[groupBy] )
          else:
            prStr = bkDict[groupBy]
          paths['Successful'].setdefault( prStr, set() ).update( directories[dirName] )
        elif groupBy:
          gLogger.notice( 'Invalid metadata item: %s' % groupBy )
          gLogger.notice( 'Available are: %s' % str( bkDict.keys() ) )
          diracExit( 1 )
        else:
          success[dirName] = bkDict['Path']

    if groupBy:
      if summary:
        pathSummary = {'Successful': {}, 'Failed' : {}}
        for path in paths['Successful']:
          nfiles = len( paths['Successful'][path] )
          if 'Invisible' in path:
            path = path.split()[0]
            inv = ' (Invisible)'
          else:
            inv = ''
          pathSummary['Successful'][path] = '%d files%s' % ( nfiles, inv )
        if failed:
          pathSummary['Failed'] = dict( ( path, 'Directory not in BK (%d files)' % len( directories[path] ) ) for path in failed )
        else:
          pathSummary.pop( 'Failed' )
        res = S_OK( pathSummary )
      else:
        for dirName in failed:
          paths['Failed'].update( dict.fromkeys( directories[dirName], 'Directory not in BK' ) )
        res = S_OK( paths )

  printDMResult( res, empty = 'None', script = 'dirac-bookkeeping-file-path' )
  if printList:
    gLogger.notice( '\nList of %s values' % groupBy )
    gLogger.notice( ','.join( sorted( [item.replace( '%s ' % groupBy, '' ) for item in res['Value']['Successful']] ) ) )

#==================================================================================

def _updateFileLumi( fileDict, retries = 5 ):
  """
  Update the luminosity of a list of files in the BK

  :param dict fileDict: {lfn:luminosity}

  :return: bool reporting error
  """
  error = False
  progressBar = ProgressBar( len( fileDict ), title = 'Updating luminosity', step = 10 )
  for lfn in fileDict:
    progressBar.loop()
    # retry 5 times
    for i in range( retries - 1, -1, -1 ):
      res = bkClient.updateFileMetaData( lfn, {'Luminosity':fileDict[lfn]} )
      if res['OK']:
        break
      elif i == 0:
        error = True
        gLogger.error( 'Error setting Luminosity', res['Message'] )
  progressBar.endLoop()
  return error

def _updateDescendantsLumi( parentLumi, doIt = False, force = False ):
  """
  Get file descendants and update their luminosity if necessary (if doIt == True)
  This function does it recursively to all descendants

  :param dict parentLumi: {lfn:lumi} for parent files
  :param bool doit: execute the operation if True, else only print out information
  :param bool force: update lumi even if OK (useful if further descendants may not be OK)

  :return: bool indicating error
  """
  if not parentLumi:
    return None
  # Get descendants:
  error = False
  res = bkClient.getFileDescendants( parentLumi.keys(), depth = 1, checkreplica = False )
  if not res['OK']:
    gLogger.error( 'Error getting descendants', res['Message'] )
    return True
  success = res['Value']['WithMetadata']
  descLumi = {}
  fileTypes = {}
  fileLumi = {}
  for lfn in success:
    for desc in success[lfn]:
      fileType = success[lfn][desc]['FileType']
      if fileType not in ( 'LOG', ) and 'HIST' not in fileType:
        descLumi.setdefault( desc, 0. )
        descLumi[desc] += parentLumi[lfn]
        fileTypes[desc] = fileType
        fileLumi[desc] = success[lfn][desc]['Luminosity']
  if not descLumi:
    return None

  prStr = 'Updating' if doIt else 'Would update'
  nDesc = len( descLumi )
  saveLumi = descLumi.copy()
  for lfn in fileLumi:
    if abs( fileLumi[lfn] - descLumi[lfn] ) < 1:
      descLumi.pop( lfn, None )
  if descLumi:
    gLogger.notice( '%s lumi of %d descendants out of %d (file types: %s) of %d files' % ( prStr, len( descLumi ), nDesc, ','.join( sorted( set( fileTypes.values() ) ) ), len( parentLumi ) ) )
  else:
    gLogger.notice( 'All %d descendants (file types: %s) of %d files are OK' % ( nDesc, ','.join( sorted( set( fileTypes.values() ) ) ), len( parentLumi ) ) )
    if not force:
      return None
  if doIt:
    error = _updateFileLumi( descLumi )
  result = _updateDescendantsLumi( descLumi if not force else saveLumi, doIt = doIt, force = force )
  return error or bool( result )

def _updateRunLumi( run, evtType, fileInfo, doIt = False, force = False ):
  """
  Updates the files luminosity from the run nformation and the files statistics
  run : run number (int)
  evtType: event type (int)
  fileInfo: list of tuples containing run files information for that event type [(lfn, nbevts, lumi), ...]
  """
  res = bkClient.getRunInformations( run )
  if not res['OK']:
    gLogger.error( 'Error from BK getting run information', res['Message'] )
    return
  info = res['Value']
  runLumi = info['TotalLuminosity']
  runEvts = dict( zip( info['Stream'], info['Number of events'] ) )[evtType]
  filesLumi = sum( [lumi for _lfn, _evts, lumi in fileInfo] )
  # Check luminosity
  error = False
  if not runEvts:
    gLogger.notice( "Event number is zero for run %d, cannot update" % run )
  elif abs( runLumi - filesLumi ) > 1:
    prStr = 'Updating' if doIt else 'Would update'
    gLogger.notice( "%s %d files as run %d and files lumi don't match: runLumi %.1f, filesLumi %.1f" % ( prStr, len( fileInfo ), run, runLumi, filesLumi ) )
    fileDict = {}
    for info in fileInfo:
      # Split the luminosity according to nb of events
      info[2] = float( runLumi ) * info[1] / runEvts
      fileDict[info[0]] = info[2]
    if doIt:
      error = _updateFileLumi( fileDict )
  else:
    gLogger.notice( 'Run %d: %d RAW files are OK' % ( run, len( fileInfo ) ) )

  # Now update descendants
  fileLumi = dict( [( lfn, lumi ) for lfn, _evts, lumi in fileInfo] )
  result = _updateDescendantsLumi( fileLumi, doIt = doIt, force = force )
  return error or result

def executeFixLuminosity( dmScript ):
  """
  Checks the luminosity of files in a BK query against the run luminosity
  If requested, it fixes recursively the luminosity of the files and all descendants
  """
  doIt = False
  force = False
  for switch in Script.getUnprocessedSwitches():
    if switch[0] == 'DoIt':
      doIt = True
    elif switch[0] == 'Force':
      force = True
    pass

  bkQuery = dmScript.getBKQuery()
  if not bkQuery:
    gLogger.error( "No BK query given..." )
    diracExit( 1 )

  fileType = bkQuery.getFileTypeList()
  if fileType != ['RAW']:
    gLogger.notice( 'This script only works from RAW files' )
    diracExit( 2 )

  bkQueryDict = bkQuery.getQueryDict()
  evtTypes = bkQuery.getEventTypeList()

  runChecked = False
  for evtType in evtTypes:
    gLogger.notice( '**** Event type %s' % evtType )
    bkQueryDict['EventType'] = evtType
    res = bkClient.getFilesWithMetadata( bkQueryDict )
    if not res['OK']:
      gLogger.fatal( 'Error getting BK files', res['Message'] )
    parameterNames = res['Value']['ParameterNames']
    info = res['Value']['Records']
    runFiles = {}
    for item in info:
      metadata = dict( zip( parameterNames, item ) )
      run = metadata['RunNumber']
      lfn = metadata['FileName']
      lumi = metadata['Luminosity']
      evts = metadata['EventStat']
      runFiles.setdefault( run, [] ).append( [lfn, evts, lumi] )
    if not runChecked:
      res = bkClient.getRunStatus( runFiles.keys() )
      if not res['OK']:
        gLogger.fatal( 'Error getting run status', res['Message'] )
        diracExit( 3 )
      runNotFinished = sorted( str( run ) for run in res['Value']['Successful'] if res['Value']['Successful'][run]['Finished'] != 'Y' )
      if runNotFinished:
        gLogger.notice( 'Found %d runs that are not Finished: %s' % ( len( runNotFinished ), ','.join( runNotFinished ) ) )
      runFinished = sorted( run for run in res['Value']['Successful'] if res['Value']['Successful'][run]['Finished'] == 'Y' )
      if runFinished:
        gLogger.notice( 'Found %d runs that are Finished' % len( runFinished ) )
      else:
        gLogger.notice( 'No Finished run found' )
        diracExit( 0 )
    for run in runFinished:
      result = _updateRunLumi( run, int( evtType ), runFiles[run], doIt = doIt, force = force )
      if doIt:
        if result is not None:
          gLogger.notice( 'Update done %s' % ( 'with errors' if result else 'successfully' ) )

#==================================================================================

def executeFileAncestors( dmScript, level = 1 ):
  """
  Gets a list of LFNs and obtains from BK the list of ancestors at a certain depth and/or for a certain Production
  """
  full = False
  checkreplica = True
  for switch in Script.getUnprocessedSwitches():
    if switch[0] == 'Full':
      full = True
    elif switch[0] == 'All':
      checkreplica = False
    elif switch[0] == 'Depth':
      try:
        level = int( switch[1] )
      except ValueError as e:
        gLogger.exception( "Invalid value for --Depth:", switch[1], lException = e )

  args = Script.getPositionalArgs()

  try:
    level = int( args[-1] )
    args.pop()
  except ( ValueError, IndexError ):
    pass

  for lfn in args:
    dmScript.setLFNsFromFile( lfn )
  lfnList = dmScript.getOption( 'LFNs', [] )

  chunkSize = 50
  progressBar = ProgressBar( len( lfnList ), chunk = chunkSize, title = 'Getting ancestors for %d files (depth %d)' % ( len( lfnList ), level ) )
  fullResult = S_OK( {} )
  for lfnChunk in breakListIntoChunks( lfnList, 50 ):
    progressBar.loop()
    result = bkClient.getFileAncestors( lfnChunk, level, replica = checkreplica )

    if result['OK']:
      if full:
        fullResult['Value'].setdefault( 'WithMetadata', {} ).update( result['Value']['WithMetadata'] )
      else:
        okResult = result['Value']['WithMetadata']
        for lfn in okResult:
          fullResult['Value'].setdefault( 'Successful', {} )[lfn] = \
            dict( ( desc, 'Replica-%s' % meta['GotReplica'] ) for desc, meta in okResult[lfn].iteritems() )
      fullResult['Value'].setdefault( 'Failed', {} ).update( result['Value']['Failed'] )
    else:
      fullResult = result
      break
  progressBar.endLoop()

  diracExit( printDMResult( fullResult, empty = "None", script = "dirac-bookkeeping-get-file-ancestors" ) )

#==================================================================================

def executeFileDescendants( dmScript, level = 1 ):
  """
  Gets a list of LFNs and obtains from BK the list of descendants at a certain depth and/or for a certain Production
  """
  checkreplica = True
  prod = 0
  full = False
  for switch in Script.getUnprocessedSwitches():
    if switch[0] == 'All':
      checkreplica = False
    elif switch[0] == 'Depth':
      try:
        level = int( switch[1] )
      except ValueError as e:
        gLogger.exception( "Invalid value for --Depth: %s", switch[1], lException = e )
    elif switch[0] == 'Production':
      try:
        prod = int( switch[1] )
      except ValueError as e:
        gLogger.exception( "Invalid production", switch[1], lException = e )
        prod = 0
    elif switch[0] == 'Full':
      full = True

  args = Script.getPositionalArgs()

  try:
    level = int( args[-1] )
    args.pop()
  except ( ValueError, IndexError ):
    pass

  for lfn in args:
    dmScript.setLFNsFromFile( lfn )
  lfnList = dmScript.getOption( 'LFNs', [] )

  chunkSize = int( 20 / level )
  progressBar = ProgressBar( len( lfnList ), chunk = chunkSize, title = 'Getting descendants for %d files (depth %d)' % ( len( lfnList ), level ) + ( ' for production %d' % prod if prod else '' ) )
  fullResult = S_OK( {} )
  for lfnChunk in breakListIntoChunks( lfnList, 50 ):
    progressBar.loop()
    result = bkClient.getFileDescendants( lfnChunk, depth = level, production = prod, checkreplica = checkreplica )
    if result['OK']:
      noDescendants = set( lfnChunk ) - set( result['Value']['Successful'] ) - set( result['Value']['Failed'] ) - \
                      set( result['Value']['NotProcessed'] )
      if noDescendants:
        fullResult['Value'].setdefault( 'NoDescendants', [] ).extend( sorted( noDescendants ) )
      if full:
        fullResult['Value'].setdefault( 'WithMetadata', {} ).update( result['Value']['WithMetadata'] )
      else:
        okResult = result['Value']['WithMetadata']
        for lfn in okResult:
          fullResult['Value'].setdefault( 'Successful', {} )[lfn] = \
            dict( ( desc, 'Replica-%s' % meta['GotReplica'] ) for desc, meta in okResult[lfn].iteritems() )
      fullResult['Value'].setdefault( 'Failed', {} ).update( result['Value']['Failed'] )
      fullResult['Value'].setdefault( 'NotProcessed', [] ).extend( result['Value']['NotProcessed'] )
    else:
      fullResult = result
      break
  progressBar.endLoop()

  diracExit( printDMResult( fullResult, empty = "None", script = "dirac-bookkeeping-get-file-descendants" ) )

#==================================================================================

def executeGetFiles( dmScript, maxFiles = 20 ):
  """
  Get files given a BK query
  """
  output = None
  nMax = None
  bkFile = None
  for switch, val in Script.getUnprocessedSwitches():
    if switch == 'Output':
      output = val
    elif switch == 'File':
      bkFile = val
    elif switch == 'Term':
      bkFile = '/dev/stdin'
    elif switch == 'MaxFiles':
      try:
        nMax = int( val )
      except ValueError:
        gLogger.error( 'Invalid integer', val )
        diracExit( 2 )

  bkQuery = dmScript.getBKQuery()
  bkQueries = [bkQuery] if bkQuery else []
  if bkFile and os.path.exists( bkFile ):
    with open( bkFile, 'r' ) as f:
      bkQueries += [BKQuery( ll.strip().split()[0] ) for ll in f.readlines()]

  if not bkQueries:
    gLogger.notice( "No BK query given, use --BK <bkPath> or --BKFile <localFile>" )
    diracExit( 1 )

  fileDict = {}
  for bkQuery in bkQueries:
    gLogger.notice( "Using BKQuery:", bkQuery )

    useFilesWithMetadata = False
    if useFilesWithMetadata:
      res = bkClient.getFilesWithMetadata( bkQuery.getQueryDict() )
      if not res['OK']:
        gLogger.error( 'ERROR getting the files', res['Message'] )
        diracExit( 1 )
      parameters = res['Value']['ParameterNames']
      for record in res['Value']['Records']:
        dd = dict( zip( parameters, record ) )
        lfn = dd.pop( 'FileName' )
        fileDict[lfn] = dd

    else:
      lfns = bkQuery.getLFNs( printSEUsage = False, printOutput = False )

      for lfnChunk in breakListIntoChunks( lfns, 1000 ):
        res = bkClient.getFileMetadata( lfnChunk )
        if not res['OK']:
          gLogger.error( 'ERROR: failed to get metadata:', res['Message'] )
          diracExit( 1 )
        fileDict.update( res['Value']['Successful'] )

  if not fileDict:
    gLogger.notice( 'No files found for BK query' )
    diracExit( 0 )

  # Now print out
  nFiles = len( fileDict )
  if output:
    f = open( output, 'w' )
    if not nMax:
      nMax = maxFiles
  else:
    if not nMax:
      nMax = nFiles
  gLogger.notice( '%d files found' % nFiles, '(showing only first %d files):' % nMax if nFiles > nMax else ':' )
  outputStr = '%s %s %s %s %s' % ( 'FileName'.ljust( 100 ),
                                   'Size'.ljust( 10 ),
                                   'GUID'.ljust( 40 ),
                                   'Replica'.ljust( 8 ),
                                   'Visibility'.ljust( 8 ) )
  gLogger.notice( outputStr )
  nFiles = 0
  for lfn in sorted( fileDict ):
    metadata = fileDict[lfn]
    size = metadata['FileSize']
    guid = metadata['GUID']
    hasReplica = metadata['GotReplica']
    visible = metadata.get( 'VisibilityFlag', '?' )
    outputStr = '%s %s %s %s %s' % ( lfn.ljust( 100 ),
                                     str( size ).ljust( 10 ),
                                     guid.ljust( 40 ),
                                     str( hasReplica ).ljust( 8 ),
                                     str( visible ).ljust( 8 ) )
    if output:
      f.write( outputStr + '\n' )
    if nFiles < nMax:
      nFiles += 1
      gLogger.notice( outputStr )

  if output:
    f.close()
    gLogger.notice( '\nList of %d files saved in' % len( fileDict ), output )

#==================================================================================

def executeFileSisters( dmScript, level = 1 ):
  """
  Gets a list of files and extract from BK the sisters (i.e. files with same parent in the same production
  """
  checkreplica = True
  prod = 0
  full = False
  sameType = True
  for switch in Script.getUnprocessedSwitches():
    if switch[0] == 'All':
      checkreplica = False
    elif switch[0] == 'Depth':
      try:
        level = int( switch[1] )
      except ValueError as e:
        gLogger.exception( "Invalid value for --Depth:", switch[1], lException = e )
    elif switch[0] == 'Production':
      try:
        prod = int( switch[1] )
      except ValueError as e:
        gLogger.exception( "Invalid value for --Production:", switch[1], lException = e )
        prod = 0
    elif switch[0] == 'Full':
      full = True
    elif switch[0] == 'AllFileTypes':
      sameType = False

  args = Script.getPositionalArgs()

  try:
    level = int( args[-1] )
    args.pop()
  except ( ValueError, IndexError ):
    pass
  if level == 1:
    relation = 'NoSister'
  else:
    relation = 'NoCousin'

  for lfn in args:
    dmScript.setLFNsFromFile( lfn )
  lfnList = dmScript.getOption( 'LFNs', [] )
  if not lfnList:
    gLogger.error( "No LFNs given" )
    diracExit( 1 )

  prodLfns = {}
  if not prod:
    # Get the productions for the input files
    directories = {}
    for lfn in lfnList:
      directories.setdefault( os.path.dirname( lfn ), [] ).append( lfn )
    res = bkClient.getDirectoryMetadata( directories.keys() )
    if not res['OK']:
      gLogger.error( "Error getting directories metadata", res['Message'] )
      diracExit( 1 )
    for dirName in directories:
      prod = res['Value']['Successful'].get( dirName, [{}] )[0].get( 'Production' )
      if not prod:
        gLogger.error( "Error: could not get production number for %s" % dirName )
      else:
        prodLfns.setdefault( prod, [] ).extend( directories[dirName] )
  else:
    prodLfns[prod] = lfnList

  if full:
    resItem = 'WithMetadata'
  else:
    resItem = 'Successful'
  fullResult = { 'OK': True, 'Value': {resItem:{}, relation:set()}}
  resValue = fullResult['Value']

  for prod, lfnList in prodLfns.iteritems():
    if sameType:
      res = bkClient.getFileMetadata( lfnList )
      if not res['OK']:
        gLogger.error( "Error getting file metadata", res['Message'] )
        diracExit( 1 )
      lfnTypes = {}
      for lfn in res['Value']['Successful']:
        metadata = res['Value']['Successful'][lfn]
        lfnTypes[lfn] = metadata['FileType']
    else:
      lfnTypes = dict.fromkeys( lfnList, None )

    # First get ancestors
    result = bkClient.getFileAncestors( lfnTypes.keys(), level, replica = False )
    if not result['OK']:
      gLogger.error( "Error getting ancestors:", res['Message'] )
      diracExit( 1 )

    ancestors = dict( ( anc['FileName'], lfn ) for lfn, ancList in result['Value']['Successful'].iteritems() for anc in ancList )

    res = bkClient.getFileDescendants( ancestors.keys(), depth = 999999, production = prod, checkreplica = checkreplica )

    fullResult['OK'] = res['OK']
    if res['OK']:
      for anc, sisters in res['Value']['WithMetadata'].iteritems():
        lfn = ancestors[anc]
        found = False
        for sister in sisters:
          metadata = sisters[sister]
          if sister != lfn and ( not sameType or metadata['FileType'] == lfnTypes[lfn] ):
            if full:
              resValue[resItem].setdefault( lfn, {} ).update( metadata )
            else:
              resValue[resItem].setdefault( lfn, set() ).add( sister )
            found = True
        if not found:
          resValue[relation].add( lfn )
        if lfn in lfnList:
          lfnList.remove( lfn )
      for lfn in lfnList:
        resValue[relation].add( lfn )
      for lfn in set( resValue[resItem] ).intersection( resValue[relation] ):
        resValue[relation].remove( lfn )
    else:
      break

  diracExit( printDMResult( fullResult, empty = "None", script = "dirac-bookkeeping-get-file-sisters" ) )

#==================================================================================

def _intWithQuotes( val, quote = "'" ):
  """
  Print numbers with a character separating each thousand

  :param int,long val: integer value to printout
  :param character quote: character to use as a separator

  :return: string to print out
  """
  chunks = []
  if not val:
    return 'None'
  while val:
    if val >= 1000:
      chunks.append( "%03d" % ( val % 1000 ) )
    else:
      chunks.append( "%d" % ( val % 1000 ) )
    val /= 1000
  chunks.reverse()
  return quote.join( chunks )

def _scaleValue( val, units ):
  """
  Scale a value by thousands, return value and unit

  :param float val: value to scale
  :param iterable units: list of unit names (increasing order)

  :return: tuple (scaledValue, unitName)
  """
  if val:
    for unit in units:
      if val < 1000.:
        break
      val /= 1000.
  else:
    val = 0
    unit = ''
  return val, unit

def _scaleLumi( lumi ):
  """
  Return lumi in the appropriate unit
  """
  return _scaleValue( lumi, ( '/microBarn', '/nb', '/pb', '/fb', '/ab' ) )

def _scaleSize( size ):
  """
  Return size in appropriate unit
  """
  return _scaleValue( size, ( 'Bytes', 'kB', 'MB', 'GB', 'TB', 'PB' ) )

def _getCollidingBunches( fills ):
  """
  Get the number of colliding bunches for all fills and average

  :param iterable fills: list of fill numbers

  :return: dictionary {fill:nbCollisingBunches}
  """
  import urllib2
  import json
  result = {}
  for fill in fills:
    try:
      runDbUrl = 'https://lbrundb.cern.ch/api/fill/%d/' % fill
      fillInfo = json.load( urllib2.urlopen( runDbUrl ) )
      result[fill] = int( fillInfo['nCollidingBunches'] )
    except ( urllib2.HTTPError, KeyError, ValueError ) as e:
      gLogger.exception( "Exception getting info for fill", str( fill ), lException = e )
      pass
  return result

def executeGetStats( dmScript ):
  """
  Extract statistics for a BK query
  """
  triggerRate = False
  listRuns = False
  listFills = False
  for switch in Script.getUnprocessedSwitches():
    if switch[0] == 'TriggerRate':
      triggerRate = True
    elif switch[0] == 'ListRuns':
      listRuns = True
    elif switch[0] == 'ListFills':
      listFills = True

  if listRuns:
    listFills = False
  lfns = dmScript.getOption( 'LFNs' )
  bkQuery = dmScript.getBKQuery()
  if not bkQuery and not lfns:
    gLogger.notice( "No BK query given..." )
    diracExit( 1 )

  if bkQuery:
    prodList = bkQuery.getQueryDict().get( 'Production', [None] )
    if not isinstance( prodList, list ):
      prodList = [prodList]
    bkQuery.setOption( 'ProductionID', None )
    fileType = bkQuery.getFileTypeList()
  else:
    prodList = [None]
    fileType = None
  if fileType != ['RAW'] and triggerRate:
    gLogger.notice( 'TriggerRate option ignored as not looking for RAW files' )
    triggerRate = False

  transClient = TransformationClient()

  for prod in prodList:
    if bkQuery:
      queryDict = bkQuery.getQueryDict()
      if queryDict.get( 'Visible', 'All' ).lower() in ( 'no', 'all' ):
        bkQuery.setOption( 'ReplicaFlag', 'ALL' )
    if prod:
      res = transClient.getTransformation( prod, extraParams = False )
      if not res['OK']:
        continue
      prodName = res['Value']['TransformationName']
      bkQuery.setOption( 'Production', prod )
      gLogger.notice( "For production %d, %s (query %s)" % ( prod, prodName, bkQuery ) )
    elif lfns:
      gLogger.notice( "For %d LFNs:" % len( lfns ) )
    else:
      gLogger.notice( "For BK query:", bkQuery )
      if bkQuery:
        queryDict = bkQuery.getQueryDict()

    # Get information from BK
    if not triggerRate and not lfns and 'ReplicaFlag' not in queryDict and 'DataQuality' not in queryDict:
      gLogger.notice( "Getting info from filesSummary..." )
      query = queryDict.copy()
      # Horrible! At some point the BK service was requiring a dictionary with at least 3 elements, to check if still needed
      if len( query ) <= 3:
        query.update( {'1':1, '2':2, '3':3 } )
      fileTypes = query.get( 'FileType' )
      if not isinstance( fileTypes, list ):
        fileTypes = [fileTypes]
      if 'FileType' in query:
        query.pop( 'FileType' )
      records = []
      nDatasets = 1
      if isinstance( fileTypes, list ):
        nDatasets = len( fileTypes )
      eventTypes = query.get( 'EventType' )
      if isinstance( eventTypes, list ):
        nDatasets *= len( eventTypes )
      for fileType in fileTypes:
        if fileType:
          query['FileType'] = fileType
        res = bkClient.getFilesSummary( query )
        if not res['OK']:
          gLogger.error( "Error getting statistics from BK", res['Message'] )
          diracExit( 0 )
        paramNames = res['Value']['ParameterNames']
        record = []
        for paramValues in res['Value']['Records']:
          if not record:
            record = len( paramValues ) * [0]
          record = [( rec + val ) if val else rec for rec, val in zip( record, paramValues )]
        # print fileType, record
        for name, value in zip( paramNames, record ):
          if name == 'NumberOfEvents':
            nevts = value
          elif name == 'FileSize':
            size = value
          elif name == 'Luminosity':
            lumi = value
        record.append( nevts / float( lumi ) if lumi else 0. )
        record.append( size / float( lumi )  if lumi else 0. )
        if not records:
          records = len( record ) * [0]
        records = [rec1 + rec2 for rec1, rec2 in zip( record, records )]
        # print fileType, records, 'Total'
      paramNames += ['EvtsPerLumi', 'SizePerLumi' ]
    else:
      gLogger.notice( "Getting info from files..." )
      # To be replaced with getFilesWithMetadata when it allows invisible files
      paramNames = ['NbofFiles', 'NumberOfEvents', 'FileSize', 'Luminosity', 'EvtsPerLumi', 'SizePerLumi']
      nbFiles = 0
      nbEvents = 0
      fileSize = 0
      lumi = 0
      datasets = set()
      runList = {}
      if not lfns:
        fileTypes = queryDict.get( 'FileType' )
        res = bkClient.getFilesWithMetadata( queryDict )
        if 'ParameterNames' in res.get( 'Value', {} ):
          parameterNames = res['Value']['ParameterNames']
          info = res['Value']['Records']
        else:
          if 'Value' in res:
            gLogger.error( 'ParameterNames not present:', str( res['Value'].keys() ) if isinstance( res['Value'], dict ) else str( res['Value'] ) )
          info = []
          res = bkClient.getFiles( queryDict )
          if res['OK']:
            lfns = res['Value']
        for item in info:
          metadata = dict( zip( parameterNames, item ) )
          datasets.add( ( metadata.get( 'EventType', metadata['FileName'].split( '/' )[5] ), metadata.get( 'FileType', metadata.get( 'Name' ) ) ) )
          try:
            fileSize += metadata['FileSize']
            lumi += metadata['Luminosity']
            run = metadata['RunNumber']
            runList.setdefault( run, [ 0., 0., 0. ] )
            runList[run][0] += metadata['Luminosity']
            if metadata['EventStat']:
              nbEvents += metadata['EventStat']
              runList[run][1] += metadata['EventStat']
            runList[run][2] += metadata['FileSize']
            nbFiles += 1
          except Exception as e:
            gLogger.exception( 'Exception for %s' % str( metadata.keys() ), lException = e )
      if lfns:
        lfnChunks = breakListIntoChunks( lfns, 1000 )
        progressBar = ProgressBar( len( lfns ), title = "Get metadata from BK", chunk = 1000 )
        for lfnChunk in lfnChunks:
          progressBar.loop()
          res = bkClient.getFileMetadata( lfnChunk )
          if res['OK']:
            for lfn, metadata in res['Value']['Successful'].iteritems():
              datasets.add( ( metadata['EventType'], metadata['FileType'] ) )
              try:
                fileSize += metadata['FileSize']
                lumi += metadata['Luminosity']
                run = metadata['RunNumber']
                runList.setdefault( run, [ 0, 0, 0 ] )
                runList[run][0] += metadata['Luminosity']
                if metadata['EventStat']:
                  nbEvents += metadata['EventStat']
                  runList[run][1] += metadata['EventStat']
                runList[run][2] += metadata['FileSize']
                nbFiles += 1
              except Exception as e:
                gLogger.exception( 'Exception for %s' % lfn, str( metadata.keys() ), lException = e )
          else:
            gLogger.error( "Error getting files metadata:", res['Message'] )
            continue
        progressBar.endLoop()
      records = [nbFiles, nbEvents, fileSize, lumi,
                 nbEvents / float( lumi ) if lumi else 0.,
                 fileSize / float( lumi ) if lumi else 0.]
      nDatasets = max( 1, len( datasets ) )

    # Now printout the results
    tab = 17
    nfiles = nevts = lumi = 0
    for name, value in zip( paramNames, records ):
      if name == 'NbofFiles':
        nfiles = value
        gLogger.notice( '%s: %s' % ( 'Nb of Files'.ljust( tab ), _intWithQuotes( value ) ) )
      elif name == 'NumberOfEvents':
        nevts = value
        gLogger.notice( '%s: %s' % ( 'Nb of Events'.ljust( tab ), _intWithQuotes( value ) ) )
      elif name == 'FileSize':
        size = value
        sizePerEvt = '(%.1f kB per evt)' % ( size / nevts / 1000. ) if nevts and nDatasets == 1 else ''
        size, sizeUnit = _scaleSize( size )
        gLogger.notice( '%s: %.3f %s %s' % ( 'Total size'.ljust( tab ), size, sizeUnit, sizePerEvt ) )
      elif name == 'Luminosity':
        lumi = value / nDatasets
        lumi, lumiUnit = _scaleLumi( lumi )
        lumiString = 'Luminosity' if nDatasets == 1 else 'Avg luminosity'
        gLogger.notice( '%s: %.3f %s' % ( lumiString.ljust( tab ), lumi, lumiUnit ) )
      elif name == 'SizePerLumi':
        value *= nDatasets
        gLogger.notice( "%s: %.1f GB" % ( ( 'Size  per %s' % '/pb' ).ljust( tab ), value * 1000000. / 1000000000. ) )
    if lumi:
      filesPerLumi = nfiles / lumi
      gLogger.notice( "%s: %.1f" % ( ( 'Files per %s' % lumiUnit ).ljust( tab ), filesPerLumi ) )
    if triggerRate:
      # Get information from the runs, but first get those that are Finished
      res = bkClient.getRunStatus( list( runList ) )
      if not res['OK']:
        gLogger.error( 'Error getting run status', res['Message'] )
        runs = []
      else:
        success = res['Value']['Successful']
        runs = [run for run in success if success[run].get( 'Finished' ) == 'Y']
      notFinished = set( runList ) - set( runs )
      if notFinished:
        gLogger.notice( '%d runs not Finished (ignored), %s runs Finished (used for trigger rate)' % ( len( notFinished ), str( len( runs ) if len( runs ) else 'no' ) ) )
        gLogger.notice( 'These runs are not Finished: %s' % ','.join( str( run ) for run in sorted( notFinished ) ) )
      if runs:
        nevts = 0
        size = 0
        fullDuration = 0.
        totalLumi = 0.
        fills = {}
        fillDuration = {}
        for run in sorted( runs ):
          res = bkClient.getRunInformations( run )
          if not res['OK']:
            gLogger.error( 'Error from BK getting run information', res['Message'] )
          else:
            nevts += runList[run][1]
            size += runList[run][2]
            info = res['Value']
            fill = info['FillNumber']
            fills.setdefault( fill, [] ).append( str( run ) )
            runDuration = ( info['RunEnd'] - info['RunStart'] ).total_seconds() / 3600.
            fillDuration[fill] = fillDuration.setdefault( fill, 0 ) + runDuration
            fullDuration += runDuration
            lumi = info['TotalLuminosity']
            if abs( lumi - runList[run][0] / nDatasets ) > 1:
              gLogger.notice( 'Run and files luminosity mismatch (ignored): run %d, runLumi %d, filesLumi %d' % ( run, lumi, int( runList[run][0] / nDatasets ) ) )
            else:
              totalLumi += lumi
        if fullDuration:
          triggerRate = nevts / fullDuration / 3600
          rate = ( '%.1f events/second' % triggerRate )
        else:
          triggerRate = 0.
          rate = 'Run duration not available'
        totalLumi, lumiUnit = _scaleLumi( totalLumi )
        gLogger.notice( '%s: %.3f %s' % ( 'Total Luminosity'.ljust( tab ), totalLumi, lumiUnit ) )
        gLogger.notice( '%s: %.1f hours (%d runs)' % ( 'Run duration'.ljust( tab ), fullDuration, len( runs ) ) )
        gLogger.notice( '%s: %s' % ( 'Trigger rate'.ljust( tab ), rate ) )
        rate = ( '%.1f MB/second' % ( size / 1000000. / fullDuration / 3600. ) ) if fullDuration else 'Run duration not available'
        gLogger.notice( '%s: %s' % ( 'Throughput'.ljust( tab ), rate ) )
        result = _getCollidingBunches( fillDuration )
        collBunches = 0.
        for fill in fillDuration:
          if fill not in result:
            gLogger.notice( "Error: no number of colliding bunches for fill %d" % fillDuration )
          else:
            collBunches += result[fill] * fillDuration[fill]
        if fullDuration:
          collBunches /= fullDuration
          gLogger.notice( '%s: %.1f on average' % ( 'Colliding bunches'.ljust( tab ), collBunches ) )
          if collBunches:
            gLogger.notice( '%s: %.2f events/s/bunch' % ( 'Trigger per bunch'.ljust( tab ), triggerRate / collBunches ) )
        if listFills:
          gLogger.notice( 'List of fills: ', ','.join( "%d (%d runs, %.1f hours)" % ( fill, len( fills[fill] ), fillDuration[fill] ) for fill in sorted( fills ) ) )
        if listRuns:
          for fill in sorted( fills ):
            gLogger.notice( 'Fill %d (%4d bunches, %.1f hours):' % ( fill, result[fill], fillDuration[fill] ), ','.join( fills[fill] ) )
    gLogger.notice( "" )

def executeRunTCK():
  """
  Get the TCK for a range of runs, and then prints each TCK with run ranges
  """
  runsDict = {}
  force = False
  for switch in Script.getUnprocessedSwitches():
    if switch[0] == 'Force':
      force = True
    if switch[0] == 'Runs':
      # Add a fake run number to force parseRuns to return a list
      try:
        runRange = switch[1]
        runsDict = parseRuns( {}, '0,' + runRange )
      except BadRunRange:
        gLogger.fatal( "Bad run range" )
        diracExit( 1 )

  runsList = runsDict['RunNumber']
  # Remove the fake run number
  runsList.remove( 0 )
  runDict = {}
  progressBar = ProgressBar( len( runsList ), title = "Getting TCK for run range %s " % runRange, step = 20 )
  for run in sorted( runsList ):
    progressBar.loop()
    res = bkClient.getRunInformations( run )
    if res['OK']:
      tck = res['Value'].get( 'Tck' )
      streams = res['Value'].get( 'Stream', [] )
      if ( 90000000 in streams or force ) and tck:
        runDict[run] = tck
  progressBar.endLoop()
  tckList = []
  for run in sorted( runDict ):
    if runDict[run] not in tckList:
      tckList.append( runDict[run] )
  for tck in tckList:
    prStr = 'TCK %s: ' % tck
    firstRun = None
    # Add a fake run (None) in order to print out the last range
    for run in sorted( runDict ) + [None]:
      runTck = runDict.get( run )
      if runTck == tck and not firstRun:
        firstRun = run
        lastRun = run
      elif runTck != tck and firstRun:
        prStr += '%d:%d, ' % ( firstRun, lastRun )
        firstRun = None
      else:
        lastRun = run
    gLogger.notice( prStr[:-2] )
