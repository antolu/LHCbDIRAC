#!/usr/bin/env python
"""
  Script for scanning the popularity table
  """
import DIRAC
import os, sys, time
from DIRAC import gLogger
# Code

def getTimeBin( time ):
  year, week, day = time.isocalendar()
  week += 52 * year
  if byDay:
    day += 7 * week
    return day
  else:
    return week

def cacheDirectories( directories ):
  from DIRAC.Core.Utilities.List import breakListIntoChunks

  # Ignore cached directories
  dirSet = directories - set( bkPathForLfn )
  if not dirSet:
    return
  startTime = time.time()
  gLogger.always( 'Caching %d directories, %d not cached yet' % ( len( directories ), len( dirSet ) ) )

  # # keep only the first directory when numbered
  dirLong2Short = {}
  dirShort2Long = {}
  # List must be sorted in order to get it from the cache!
  for lfn in sorted( dirSet ):
    splitDir = lfn.split( '/' )
    if len( splitDir[-2] ) == 4 and splitDir[-2].isdigit():
      short = '/'.join( splitDir[:-2] )
    else:
      short = lfn
    dirLong2Short[lfn] = short
    dirShort2Long.setdefault( short, lfn )

  # # Cache BK path for directories
  chunkSize = 100
  gLogger.always( 'Getting cache metadata for %d directories' % len( dirShort2Long ) )
  # First try cached information in Data Usage DB
  invisible = set()
  res = duClient.getDirMetadata( dirShort2Long.values() )
  if not res['OK']:
    gLogger.fatal( "\nError getting metadata from DataUsage", res['Message'] )
    lfnsFromBK = set( dirShort2Long.values() )
  else:
    lfnsFromBK = set()
    for lfn in dirSet:
      longDir = dirShort2Long[dirLong2Short[lfn]]
      metadata = res['Value'].get( longDir )
      if metadata:
        if len( metadata ) < 9 or metadata[8] != "N":
          bkPathForLfn[lfn] = os.path.join( '/', metadata[1], metadata[2], metadata[3], metadata[4][1:], metadata[5], metadata[6] )
          processingPass[bkPathForLfn[lfn]] = metadata[4]
          prodForBKPath.setdefault( bkPathForLfn[lfn], set() ).add( metadata[7] )
        else:
          invisible.add( lfn )
      else:
        lfnsFromBK.add( longDir )
  # For those not available ask the BK
  if lfnsFromBK:
    gLogger.always( 'Getting BK metadata for %d directories' % len( lfnsFromBK ) )
    success = {}
    for lfns in breakListIntoChunks( list( lfnsFromBK ), chunkSize ):
      while True:
        res = bkClient.getDirectoryMetadata( lfns )
        if not res['OK']:
          gLogger.fatal( "\nError getting BK metadata", res['Message'] )
        else:
          break
      success.update( res['Value'].get( 'Successful', {} ) )
    gLogger.always( '' )
    for lfn in dirSet - set( bkPathForLfn ):
      longDir = dirShort2Long[dirLong2Short[lfn]]
      metadata = success.get( longDir, [{}] )[0]
      if metadata:
        if metadata.get( 'VisibilityFlag', 'Y' ) == 'Y':
          bkPathForLfn[lfn] = BKQuery( metadata ).makePath()
          processingPass[bkPathForLfn[lfn]] = metadata['ProcessingPass']
          prodForBKPath.setdefault( bkPathForLfn[lfn], set() ).add( metadata['Production'] )
        else:
          invisible.add( lfn )
      else:
        bkPathForLfn[lfn] = "Unknown-" + lfn
        prodForBKPath.setdefault( bkPathForLfn[lfn], set() )
  gLogger.always( 'Obtained BK path' )
  if invisible:
    gLogger.always( 'The following paths are ignored as invisible:\n', '\n'.join( sorted( invisible ) ) )
  dirSet -= invisible

  # # Get the creation date
  # for lfns in breakListIntoChunks( dirLong2Short.values(), chunkSize ):
  #  while True:
  #    res = fcClient.getDirectoryMetadata( lfns )
  #    if not res['OK']:
  #      gLogger.fatal( 'Error getting directory metadata', res['Message'] )
  #    else:
  #      break
  #  success = res['Value']['Successful']
  #  for lfn in dirSet:
  #    shortDir = dirLong2Short[lfn]
  #    if shortDir in success:
  #      bkPath = bkPathForLfn[lfn]
  #      ct = creationTime.get( bkPath, datetime.now() )
  #      creationTime[bkPath] = min( success[shortDir]['CreationDate'], ct )

  missingSU = set( [dirLong2Short[lfn] for lfn in dirSet] )
  gLogger.always( 'Get LFN Storage Usage for %d directories' % len( missingSU ) )
  for lfn in missingSU:
    # LFN usage
    while True:
      res = suClient.getSummary( lfn )
      if not res['OK']:
        gLogger.fatal( 'Error getting LFN storage usage %s' % lfn, res['Message'] )
      else:
        break
    bkPath = bkPathForLfn[dirShort2Long[lfn]]
    type = 'LFN'
    bkPathUsage.setdefault( bkPath, {} ).setdefault( type, [0, 0] )
    bkPathUsage[bkPath][type][0] += sum( [val.get( 'Files', 0 ) for val in res['Value'].values()] )
    bkPathUsage[bkPath][type][1] += sum( [val.get( 'Size', 0 ) for val in res['Value'].values()] )

  # # get the PFN usage per storage type
  gLogger.always( 'Check storage type and PFN usage for %d directories' % len( dirSet ) )
  for lfn in dirSet:
    while True:
      res = suClient.getDirectorySummaryPerSE( lfn )
      if not res['OK']:
        gLogger.fatal( 'Error getting storage usage per SE %s' % lfn, res['Message'] )
      else:
        break
    info = physicalDataUsage.setdefault( lfn, {} )
    for type in storageTypes:
      # Active type will be recorded in Disk, just a special flag
      if type != 'LFN' and type not in info:
        nf = sum( [val['Files'] for se, val in res['Value'].items() if isType( se, type )] )
        size = sum( [val['Size'] for se, val in res['Value'].items() if isType( se, type )] )
        info[type] = {'Files':nf, 'Size':size}
    bkPath = bkPathForLfn[lfn]
    for type in info:
      bkPathUsage.setdefault( bkPath, {} ).setdefault( type, [ 0, 0 ] )
      bkPathUsage[bkPath][type][0] += info[type].get( 'Files', 0 )
      bkPathUsage[bkPath][type][1] += info[type].get( 'Size', 0 )
    datasetStorage[storageType( res['Value'] )].add( bkPath )

  gLogger.always( 'Obtained BK path and storage usage of %d directories in %.1f seconds' % ( len( dirSet ), time.time() - startTime ) )

def isType( se, type ):
  if type == 'All':
    return True
  if type == 'LFN':
    return False
  return storageType( [se] ) == type

def prBinNumber( bin ):
  if byDay:
    week = bin / 7
    day = bin % 7
  else:
    week = bin

  year = week / 52
  week = week % 52
  if byDay:
    return 'y%4d/w%02d/d%d' % ( year, week, day )
  else:
    return 'y%4d/w%02d' % ( year, week )

def prSize( size ):
  units = ( 'Bytes', 'kB', 'MB', 'GB', 'TB', 'PB' )
  for unit in units:
    if size < 1000.:
      return '%.1f %s' % ( size, unit )
    size /= 1000.
  return '???'

def getPhysicalUsage( baseDir ):
  while True:
    res = suClient.getStorageDirectoryData( baseDir, None, None, None, timeout = 3600 )
    if not res['OK']:
      gLogger.fatal( "Error getting list of directories for %s" % baseDir, res['Message'] )
    else:
      break
  # The returned value is a dictionary of all directory leaves
  info = res['Value']
  dirSet = set()
  nf = 0
  size = 0
  gLogger.info( "Storage usage OK for %d directories in %s" % ( len( info ), baseDir ) )
  for directory in info:
    nodes = directory.split( '/' )
    try:
      prod = int( nodes[5] )
      # FIXME
      # Don't count the certification productions
      if prod < 2000 and nodes[2] == 'validation':
        continue
    except:
      pass
    if len( nodes ) < 5 or nodes[4] not in ( 'LOG', 'HIST', 'BRUNELHIST', 'DAVINCIHIST', 'GAUSSHIST' ):
      physicalDataUsage.setdefault( directory, {} ).setdefault( 'All', info[directory] )
      dirSet.add( directory )
      nf += info[directory]['Files']
      size += info[directory]['Size']
  # In case the base directory was not a leave, cache its physical storage data
  physicalDataUsage.setdefault( baseDir, {} ).setdefault( 'All', {'Files':nf, 'Size':size} )
  return dirSet

def storageType( seList ):
  if not [se for se in seList if not se.endswith( "-ARCHIVE" ) and \
          se not in ( 'CERN-SW-TEST', 'CERN-FREEZER-EOS', 'CERN-FREEZER' )] and \
         [se for se in seList if se.endswith( "-ARCHIVE" )]:
    # Only -ARCHIVE
    return 'Archived'
  if [se for se in seList if se.endswith( '-RAW' ) or se.endswith( '-RDST' )]:
    # Any file on Tape
    return 'Tape'
  # Others
  return 'Disk'

if __name__ == '__main__':

  # Script initialization
  from DIRAC.Core.Base import Script

  since = 30
  getAllDatasets = False
  Script.registerSwitch( '', 'Since=', '   Number of days to look for (default: %d)' % since )
  Script.registerSwitch( '', 'All', '   If used, gets all existing datasets, not only the used ones' )
  Script.setUsageMessage( '\n'.join( [ __doc__,
                                       'Usage:',
                                       '  %s [option|cfgfile]' % Script.scriptName, ] ) )
  Script.parseCommandLine( ignoreErrors = True )
  for switch in Script.getUnprocessedSwitches():
    if switch[0] == 'Since':
      try:
        since = int( switch[1] )
      except:
        pass
    elif switch[0] == 'All':
      getAllDatasets = True

  bkPathForLfn = {}
  prodForBKPath = {}
  bkPathUsage = {}
  processingPass = {}

  from LHCbDIRAC.DataManagementSystem.Client.StorageUsageClient import StorageUsageClient
  from LHCbDIRAC.DataManagementSystem.Client.DataUsageClient import DataUsageClient
  from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient   import BookkeepingClient
  from LHCbDIRAC.BookkeepingSystem.Client.BKQuery   import BKQuery
  from LHCbDIRAC.TransformationSystem.Client.TransformationClient import TransformationClient
  from DIRAC.Resources.Catalog.FileCatalog import FileCatalog
  from datetime import datetime, timedelta
  duClient = DataUsageClient()
  suClient = StorageUsageClient()
  bkClient = BookkeepingClient()
  fcClient = FileCatalog()
  transClient = TransformationClient()

  # Dictionary with weekly/dayly usage for each BK path
  timeUsage = {}
  # PFN #files and size for each BK path
  physicalDataUsage = {}
  # set of used directories
  usedDirectories = set()
  storageTypes = ( 'Disk', 'Tape', 'Archived', 'All', 'LFN' )
  datasetStorage = {}
  for type in storageTypes:
    datasetStorage[type] = set()
  usedSEs = {}
  byDay = ( since <= 30 )
  if byDay:
    binSize = 'day'
    nbBins = since
  else:
    binSize = 'week'
    nbBins = int( ( since + 6 ) / 7 )
    since = 7 * nbBins

  ignoreDirectories = ( 'user', 'test', 'debug', 'dataquality', 'software', 'database', 'swtest', 'data', 'certification', 'validation' )
  nowBin = getTimeBin( datetime.now() - timedelta( days = 1 ) )
  notCached = set()

  if getAllDatasets:
    # Get list of directories
    from DIRAC.Resources.Catalog.FileCatalog import FileCatalog
    startTime = time.time()
    res = FileCatalog().listDirectory( '/lhcb' )
    if not res['OK']:
      gLogger.fatal( "Cannot get list of directories", res['Message'] )
      DIRAC.exit( 1 )
    directories = set( [ subDir for subDir in res['Value']['Successful']['/lhcb']['SubDirs'] if subDir.split( '/' )[2] not in ignoreDirectories] )
    allDirectoriesSet = set()
    for baseDir in directories:
      allDirectoriesSet.update( getPhysicalUsage( baseDir ) )
    gLogger.always( "Obtained %d directories storage usage information in %.1f seconds)" % ( len( allDirectoriesSet ), time.time() - startTime ) )
    cacheDirectories( allDirectoriesSet )

  # Get the popularity raw information for the specified number of days
  if since:
    entries = 0
    now = datetime.now()
    endTime = datetime( now.year, now.month, now.day, 0, 0, 0 )
    startTime = endTime
    gLogger.always( 'Get popularity day-by-day' )
    stTime = time.time()
    for _i in range( since ):
      endTime = startTime
      startTime = endTime - timedelta( days = 1 )
      endTimeQuery = endTime.isoformat()
      startTimeQuery = startTime.isoformat()
      status = 'Used'
      for _i in range( 10 ):
        res = duClient.getDataUsageSummary( startTimeQuery, endTimeQuery, status, timeout = 7200 )
        if res['OK']:
          break
        gLogger.error( "Error getting popularity entries, retrying...", res['Message'] )
      if not res['OK']:
        gLogger.fatal( "Error getting popularity entries", res['Message'] )
        DIRAC.exit( 1 )
      val = res[ 'Value' ]
      entries += len( val )

      # Get information on useful directories
      directories = set( [row[1] for row in val if row[1].split( '/' )[2] not in ignoreDirectories] )
      usedDirectories.update( directories )
      cacheDirectories( directories )

      # Get information in bins (day or week)
      for rowId, dirLfn, se, count, insertTime in val:
        if dirLfn not in directories:
          # print rowId, dirLfn, count, insertTime, 'ignored'
          continue
        # get the bin (day or week)
        bin = getTimeBin( insertTime )
        bkPath = bkPathForLfn.get( dirLfn )
        if not bkPath:
          if dirLfn not in notCached:
            notCached.add( dirLfn )
            gLogger.error( 'Directory %s was not cached' % dirLfn )
          bkPath = 'Unknown-' + dirLfn
        timeUsage[bkPath][bin] = timeUsage.setdefault( bkPath, {} ).setdefault( bin, 0 ) + count
        usedSEs.setdefault( bkPath, set() ).add( se )
        # if bkPath == '/LHCb/Collision12/Beam4000GeV-VeloClosed-MagUp/Real Data/Reco14/Stripping20/90000000/DIMUON.DST':
          # print rowId, dirLfn, count, insertTime, bin

    gLogger.always( "Retrieved %d entries from Popularity table in %.1f seconds" % ( entries, time.time() - stTime ) )
    gLogger.always( 'Found %d datasets used since %d days' % ( len( timeUsage ), since ) )
    counters = {}
    strangeBKPaths = set( [bkPath for bkPath in timeUsage if not bkPathUsage[bkPath].get( 'LFN', ( 0, 0 ) )[0]] )
    if strangeBKPaths:
      gLogger.always( 'Some datasets do not have an LFN count:' )
      gLogger.always( '\n'.join( ["%s : %s" % ( bkPath, str( bkPathUsage[bkPath] ) ) for bkPath in strangeBKPaths] ) )
    for type in ( 'All', 'LFN' ):
      for i in range( 2 ):
        counters.setdefault( type, [] ).append( sum( [bkPathUsage[bkPath].get( type, ( 0, 0 ) )[i] for bkPath in timeUsage] ) )
    for bkPath in sorted( timeUsage ):
      if bkPath not in ( datasetStorage['Disk'] | datasetStorage['Archived'] | datasetStorage['Tape'] ):
        datasetStorage[storageType( usedSEs[bkPath] )].add( bkPath )
      nLfns, lfnSize = bkPathUsage[bkPath].get( 'LFN', ( 0, 0 ) )
      nPfns, pfnSize = bkPathUsage[bkPath].get( 'All', ( 0, 0 ) )
      gLogger.always( '%s (%d LFNs, %s), (%d PFNs, %s, %.1f replicas)' % ( bkPath, nLfns, prSize( lfnSize ), nPfns, prSize( pfnSize ), float( nPfns ) / float( nLfns ) if nLfns else 0. ) )
      bins = sorted( timeUsage[bkPath] )
      lastBin = bins[-1]
      accesses = sum( [timeUsage[bkPath][bin] for bin in bins] )
      gLogger.always( '\tUsed first in %s, %d accesses (%.1f%%), %d accesses during last %s %s' %
                      ( prBinNumber( bins[0] ), accesses,
                        accesses * 100. / nLfns if nLfns else 0.,
                        timeUsage[bkPath][lastBin], binSize, prBinNumber( lastBin ) ) )
    gLogger.always( "\nA total of %d LFNs (%s), %d PFNs (%s) have been used" % ( counters['LFN'][0], prSize( counters['LFN'][1] ), counters['All'][0], prSize( counters['All'][1] ) ) )

  if getAllDatasets:
    # Consider only unused directories
    unusedDirectories = allDirectoriesSet - usedDirectories
    if unusedDirectories:
      cacheDirectories( unusedDirectories )
      # Remove the used datasets (from other directories)
      unusedBKPaths = set( [bkPathForLfn[lfn] for lfn in unusedDirectories if lfn in bkPathForLfn] ) - set( timeUsage )
      # Remove empty datasets
      strangeBKPaths = set( [bkPath for bkPath in unusedBKPaths if not bkPathUsage[bkPath].get( 'LFN', ( 0, 0 ) )[0]] )
      if strangeBKPaths:
        gLogger.always( 'Some datasets do not have an LFN count:' )
        gLogger.always( '\n'.join( ["%s : %s" % ( bkPath, str( bkPathUsage[bkPath] ) ) for bkPath in strangeBKPaths] ) )
      unusedBKPaths = set( [bkPath for bkPath in unusedBKPaths if bkPathUsage[bkPath].get( 'LFN', ( 0, 0 ) )[0]] )
      # In case there are datasets both on tape and disk, priviledge tape
      datasetStorage['Disk'] -= datasetStorage['Tape']
      gLogger.always( "\nThe following %d BK paths were not used since %d days" % ( len( unusedBKPaths ), since ) )
      for type in storageTypes[0:3]:
        gLogger.always( "\n=========== %s datasets ===========" % type )
        unusedPaths = unusedBKPaths & datasetStorage[type]
        counters = {}
        for t in ( 'All', 'LFN' ):
          for i in range( 2 ):
            counters.setdefault( t, [] ).append( sum( [bkPathUsage[bkPath].get( t, ( 0, 0 ) )[i] for bkPath in unusedPaths] ) )
        for bkPath in sorted( unusedPaths ):
          nLfns, lfnSize = bkPathUsage[bkPath].get( 'LFN', ( 0, 0 ) )
          nPfns, pfnSize = bkPathUsage[bkPath].get( 'All', ( 0, 0 ) )
          gLogger.always( '\t%s (%d LFNs, %s), (%d PFNs, %s, %.1f replicas)' % ( bkPath, nLfns, prSize( lfnSize ), nPfns, prSize( pfnSize ), float( nPfns ) / float( nLfns ) ) )
        gLogger.always( "\nA total of %d %s LFNs (%s), %d PFNs (%s) were not used" % ( counters['LFN'][0], type, prSize( counters['LFN'][1] ), counters['All'][0], prSize( counters['All'][1] ) ) )
      noBKDirectories = sorted( unusedDirectories - set( bkPathForLfn ) )
  else:
    unusedBKPaths = set()
    datasetStorage['Disk'] -= datasetStorage['Tape']

  # Now create a CSV file with all dataset information
  # Name, ProcessingPass, #files, size, SE type, each week's usage (before now)
  csvFile = 'popularity-%ddays.csv' % since
  f = open( csvFile, 'w' )
  title = "Name,Configuration,ProcessingPass,FileType,Type,Creation-%s,NbLFN,LFNSize,NbDisk,DiskSize,NbTape,TapeSize,NbArchived,ArchivedSize,Nb Replicas,Nb ArchReps,Storage,FirstUsage,LastUsage,Now" % binSize
  for bin in range( nbBins ):
    title += ',%d' % ( 1 + bin )
  f.write( title + '\n' )
  TB = 1000. * 1000. * 1000. * 1000.
  for bkPath in sorted( timeUsage ) + sorted( unusedBKPaths ):
    if bkPath.startswith( 'Unknown-' ):
      continue
    info = bkPathUsage[bkPath]
    # If there are fewer Archive replicas than LFNs, check if the production is still active
    lfns = info.get( 'LFN', ( 0, 0 ) )[0]
    archives = info.get( 'Archived', ( 0, 0 ) )[0]
    prods = prodForBKPath[bkPath]
    res = transClient.getTransformations( {'TransformationID':list( prods )} )
    creationTime = datetime.now()
    active = []
    for prodDict in res.get( 'Value', [] ):
      creationTime = min( creationTime, prodDict['CreationDate'] )
      if prodDict['Status'] in ( 'Active', 'Idle', 'Completed' ):
        active.append( str( prodDict['TransformationID'] ) )
    if active:
      gLogger.always( "Active productions %s found in %s" % ( ','.join( sorted( active ) ), bkPath ) )
    if info['LFN'][0] == 0:
      continue
    for type in info:
      info[type][1] /= TB
    # Some BK paths contain a , to be replaces by a . for the CSV file!!
    config = '/'.join( bkPath.split( '/' )[0:3] )
    fileType = bkPath.split( '/' )[-1]
    if ',' in bkPath:
      gLogger.always( "BK path found with ',':", bkPath )
    row = '%s,%s,%s,%s' % ( bkPath.replace( ',', '.' ).replace( 'Real Data', 'RealData' ), config, processingPass.get( bkPath, 'Unknown' ).replace( 'Real Data', 'RealData' ), fileType )
    row += ',0' if bkPath.startswith( '/MC' ) else ',1'
    row += ',%d' % ( getTimeBin( creationTime ) )
    for type in ( 'LFN', 'Disk', 'Tape', 'Archived' ):
      row += ',%d,%f' % tuple( info[type] )
    row += ',%f,%f' % ( float( info['Disk'][0] ) / float( info['LFN'][0] ), float( info['Archived'][0] ) / float( info['LFN'][0] ) )
    if active:
      dsType = 'Active'
    else:
      dsType = 'Unknown'
      for type in storageTypes[0:3]:
        if bkPath in datasetStorage[type]:
          dsType = type
          break
    row += ',%s' % dsType
    bins = sorted( timeUsage.get( bkPath, {} ) )
    if not bins:
      bins = [0]
    row += ',%d,%d,%d' % ( bins[0], bins[-1], nowBin )
    usage = 0
    for bin in range( nbBins ):
      usage += timeUsage.get( bkPath, {} ).get( nowBin - bin, 0 )
      row += ',%d' % usage
    f.write( row + '\n' )
  f.close()
  gLogger.always( '\nSuccessfully wrote CSV file %s' % csvFile )



