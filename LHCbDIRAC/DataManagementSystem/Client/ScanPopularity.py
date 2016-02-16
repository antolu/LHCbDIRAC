"""
  Methods for scanning the popularity table
  """

__RCSID__ = "$Id: ScanPopularity.py 76721 2014-07-22 08:05:22Z phicharp $"

import DIRAC
import os, time
from DIRAC import gLogger
from LHCbDIRAC.Interfaces.API.DiracLHCb import getSiteForSE

from LHCbDIRAC.DataManagementSystem.Client.StorageUsageClient import StorageUsageClient
from LHCbDIRAC.DataManagementSystem.Client.DataUsageClient import DataUsageClient
from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient   import BookkeepingClient
from LHCbDIRAC.BookkeepingSystem.Client.BKQuery   import BKQuery, makeBKPath
from LHCbDIRAC.TransformationSystem.Client.TransformationClient import TransformationClient
from DIRAC.Resources.Catalog.FileCatalog import FileCatalog
from datetime import datetime, timedelta

# Code


def getTimeBin( date ):
  year, week, day = date.isocalendar()
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
  gLogger.always( '\nCaching %d directories, %d not cached yet' % ( len( directories ), len( dirSet ) ) )

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
          ft = lfn.split( '/' )[4]
          if ft in ( 'DST', 'MDST' ) and ft != metadata[6]:
            metadata[6] = 'ALL.DST,ALL.MDST'
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
    gLogger.always( 'Successfully obtained BK metadata for %d directories' % len( success ) )
    for lfn in dirSet - set( bkPathForLfn ):
      longDir = dirShort2Long[dirLong2Short[lfn]]
      metadata = success.get( longDir, [{}] )[0]
      if metadata:
        if metadata.get( 'VisibilityFlag', metadata.get( 'Visibility', 'Y' ) ) == 'Y':
          bkDict = BKQuery( metadata ).getQueryDict()
          bkPath = makeBKPath( bkDict )
          processingPass[bkPath] = metadata['ProcessingPass']
          prodForBKPath.setdefault( bkPath, set() ).add( metadata['Production'] )
        else:
          invisible.add( lfn )
      else:
        bkPath = "Unknown-" + lfn
        prodForBKPath.setdefault( bkPath, set() )
      if lfn not in invisible:
        bkPathForLfn[lfn] = bkPath
        bkPathUsage.setdefault( bkPath, {} ).setdefault( 'LFN', [0, 0] )
  if invisible:
    gLogger.always( 'The following %d paths are ignored as invisible:\n' % len( invisible ), '\n'.join( sorted( invisible ) ) )
  dirSet -= invisible

  if dirSet:
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
      infoType = 'LFN'
      bkPathUsage.setdefault( bkPath, {} ).setdefault( infoType, [0, 0] )
      bkPathUsage[bkPath][infoType][0] += sum( [val.get( 'Files', 0 ) for val in res['Value'].values()] )
      bkPathUsage[bkPath][infoType][1] += sum( [val.get( 'Size', 0 ) for val in res['Value'].values()] )

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
      for infoType in storageTypes:
        # Active type will be recorded in Disk, just a special flag
        if infoType != 'LFN' and infoType not in info:
          nf = sum( [val['Files'] for se, val in res['Value'].items() if isType( se, infoType )] )
          size = sum( [val['Size'] for se, val in res['Value'].items() if isType( se, infoType )] )
          info[infoType] = {'Files':nf, 'Size':size}
      for site in storageSites:
        if site not in info:
          nf = sum( [val['Files'] for se, val in res['Value'].items() if isAtSite( se, site ) and isType( se, 'Disk' )] )
          size = sum( [val['Size'] for se, val in res['Value'].items() if isAtSite( se, site ) and isType( se, 'Disk' )] )
          info[site] = {'Files':nf, 'Size':size}
      bkPath = bkPathForLfn[lfn]
      for infoType in info:
        bkPathUsage.setdefault( bkPath, {} ).setdefault( infoType, [ 0, 0 ] )
        bkPathUsage[bkPath][infoType][0] += info[infoType].get( 'Files', 0 )
        bkPathUsage[bkPath][infoType][1] += info[infoType].get( 'Size', 0 )
      datasetStorage[storageType( res['Value'] )].add( bkPath )

  gLogger.always( 'Obtained BK path and storage usage of %d directories in %.1f seconds' % ( len( dirSet ), time.time() - startTime ) )

def isType( se, infoType ):
  if infoType == 'All':
    return True
  if infoType == 'LFN':
    return False
  return storageType( [se] ) == infoType

def isAtSite( se, site ):
  seSite = cachedSESites.get( se )
  if not seSite:
    seSite = getSiteForSE( se )
    if seSite['OK']:
      cachedSESites[se] = seSite['Value']
  return site == seSite

def prBinNumber( binNumber ):
  if byDay:
    week = binNumber / 7
    day = binNumber % 7
  else:
    week = binNumber

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
          se not in ( 'CERN-SWTEST', 'CERN-FREEZER-EOS', 'CERN-FREEZER' )] and \
         [se for se in seList if se.endswith( "-ARCHIVE" )]:
    # Only -ARCHIVE
    return 'Archived'
  if [se for se in seList if se.endswith( '-RAW' ) or se.endswith( '-RDST' )]:
    # Any file on Tape
    return 'Tape'
  # Others
  return 'Disk'


def scanPopularity( since, getAllDatasets ):

  global bkPathForLfn, prodForBKPath, bkPathUsage, processingPass
  bkPathForLfn = {}
  prodForBKPath = {}
  bkPathUsage = {}
  processingPass = {}

  global duClient, suClient, bkClient, fcClient, transClient
  duClient = DataUsageClient()
  suClient = StorageUsageClient()
  bkClient = BookkeepingClient()
  fcClient = FileCatalog()
  transClient = TransformationClient()

  global timeUsage, physicalDataUsage, storageTypes, storageSites, cachedSESites, datasetStorage, byDay
  # Dictionary with weekly/dayly usage for each BK path
  timeUsage = {}
  # PFN #files and size for each BK path
  physicalDataUsage = {}
  # set of used directories
  usedDirectories = set()
  storageTypes = ( 'Disk', 'Tape', 'Archived', 'All', 'LFN' )
  storageSites = ( 'LCG.CERN.ch', 'LCG.CNAF.it', 'LCG.GRIDKA.de', 'LCG.IN2P3.fr', 'LCG.PIC.es', 'LCG.RAL.uk', 'LCG.SARA.nl' )
  cachedSESites = {}
  datasetStorage = {}
  for infoType in storageTypes:
    datasetStorage[infoType] = set()
  usedSEs = {}
  byDay = ( since <= 30 )
  if byDay:
    binSize = 'day'
    nbBins = since
  else:
    binSize = 'week'
    nbBins = int( ( since + 6 ) / 7 )
    since = 7 * nbBins

  from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
  ignoreDirectories = Operations().getValue( 'DataManagement/PopularityIgnoreDirectories', ['user', 'test', 'debug', 'dataquality', 'software', 'database', 'swtest', 'certification', 'validation'] )
  nowBin = getTimeBin( datetime.now() - timedelta( days = 1 ) )
  notCached = set()

  if getAllDatasets:
    # Get list of directories
    startTime = time.time()
    res = FileCatalog().listDirectory( '/lhcb' )
    if not res['OK']:
      gLogger.fatal( "Cannot get list of directories", res['Message'] )
      DIRAC.exit( 1 )
    directories = set( [ subDir for subDir in res['Value']['Successful']['/lhcb']['SubDirs']
                        if subDir.split( '/' )[2] not in ignoreDirectories and
                        'RAW' not in subDir and 'RDST' not in subDir and 'SDST' not in subDir] )
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
      for _rowId, dirLfn, se, count, insertTime in val:
        if dirLfn not in directories:
          # print rowId, dirLfn, count, insertTime, 'ignored'
          continue
        # get the binNumber (day or week)
        binNumber = getTimeBin( insertTime )
        bkPath = bkPathForLfn.get( dirLfn )
        if not bkPath:
          if dirLfn not in notCached:
            notCached.add( dirLfn )
            gLogger.error( 'Directory %s was not cached' % dirLfn )
          bkPath = 'Unknown-' + dirLfn
        timeUsage[bkPath][binNumber] = timeUsage.setdefault( bkPath, {} ).setdefault( binNumber, 0 ) + count
        usedSEs.setdefault( bkPath, set() ).add( se )
        # if bkPath == '/LHCb/Collision12/Beam4000GeV-VeloClosed-MagUp/Real Data/Reco14/Stripping20/90000000/DIMUON.DST':
          # print rowId, dirLfn, count, insertTime, binNumber

    gLogger.always( "\n=============================================================" )
    gLogger.always( "Retrieved %d entries from Popularity table in %.1f seconds" % ( entries, time.time() - stTime ) )
    gLogger.always( 'Found %d datasets used since %d days' % ( len( timeUsage ), since ) )
    counters = {}
    strangeBKPaths = set( [bkPath for bkPath in timeUsage if not bkPathUsage.get( bkPath, {} ).get( 'LFN', ( 0, 0 ) )[0]] )
    if strangeBKPaths:
      gLogger.always( '%d used datasets do not have an LFN count:' % len( strangeBKPaths ) )
      gLogger.always( '\n'.join( ["%s : %s" % ( bkPath, str( bkPathUsage.get( bkPath, {} ) ) ) for bkPath in strangeBKPaths] ) )
    gLogger.always( '\nDataset usage for %d datasets' % len( timeUsage ) )
    for infoType in ( 'All', 'LFN' ):
      for i in range( 2 ):
        counters.setdefault( infoType, [] ).append( sum( [bkPathUsage.get( bkPath, {} ).get( infoType, ( 0, 0 ) )[i] for bkPath in timeUsage] ) )
    for bkPath in sorted( timeUsage ):
      if bkPath not in datasetStorage['Disk'] | datasetStorage['Archived'] | datasetStorage['Tape']:
        datasetStorage[storageType( usedSEs[bkPath] )].add( bkPath )
      nLfns, lfnSize = bkPathUsage.get( bkPath, {} ).get( 'LFN', ( 0, 0 ) )
      nPfns, pfnSize = bkPathUsage.get( bkPath, {} ).get( 'All', ( 0, 0 ) )
      gLogger.always( '%s (%d LFNs, %s), (%d PFNs, %s, %.1f replicas)' % ( bkPath, nLfns, prSize( lfnSize ), nPfns, prSize( pfnSize ), float( nPfns ) / float( nLfns ) if nLfns else 0. ) )
      bins = sorted( timeUsage[bkPath] )
      lastBin = bins[-1]
      accesses = sum( [timeUsage[bkPath][binNumber] for binNumber in bins] )
      gLogger.always( '\tUsed first in %s, %d accesses (%.1f%%), %d accesses during last %s %s' %
                      ( prBinNumber( bins[0] ), accesses,
                        accesses * 100. / nLfns if nLfns else 0.,
                        timeUsage[bkPath][lastBin], binSize, prBinNumber( lastBin ) ) )
    gLogger.always( "\nA total of %d LFNs (%s), %d PFNs (%s) have been used" % ( counters['LFN'][0], prSize( counters['LFN'][1] ), counters['All'][0], prSize( counters['All'][1] ) ) )

  if getAllDatasets:
    # Consider only unused directories
    unusedDirectories = allDirectoriesSet - usedDirectories
    if unusedDirectories:
      gLogger.always( "\n=============================================================" )
      gLogger.always( '%d directories have not been used' % len( unusedDirectories ) )
      # Remove the used datasets (from other directories)
      unusedBKPaths = set( [bkPathForLfn[lfn] for lfn in unusedDirectories if lfn in bkPathForLfn] ) - set( timeUsage )
      # Remove empty datasets
      strangeBKPaths = set( [bkPath for bkPath in unusedBKPaths if not bkPathUsage.get( bkPath, {} ).get( 'LFN', ( 0, 0 ) )[0]] )
      if strangeBKPaths:
        gLogger.always( '%d unused datasets do not have an LFN count:' % len( strangeBKPaths ) )
        gLogger.always( '\n'.join( ["%s : %s" % ( bkPath, str( bkPathUsage.get( bkPath, {} ) ) ) for bkPath in strangeBKPaths] ) )
      unusedBKPaths = set( [bkPath for bkPath in unusedBKPaths if bkPathUsage.get( bkPath, {} ).get( 'LFN', ( 0, 0 ) )[0]] )

      # In case there are datasets both on tape and disk, priviledge tape
      datasetStorage['Disk'] -= datasetStorage['Tape']
      gLogger.always( "\nThe following %d BK paths were not used since %d days" % ( len( unusedBKPaths ), since ) )
      for infoType in storageTypes[0:3]:
        gLogger.always( "\n=========== %s datasets ===========" % infoType )
        unusedPaths = unusedBKPaths & datasetStorage[infoType]
        counters = {}
        for t in ( 'All', 'LFN' ):
          for i in range( 2 ):
            counters.setdefault( t, [] ).append( sum( [bkPathUsage.get( bkPath, {} ).get( t, ( 0, 0 ) )[i] for bkPath in unusedPaths] ) )
        for bkPath in sorted( unusedPaths ):
          nLfns, lfnSize = bkPathUsage.get( bkPath, {} ).get( 'LFN', ( 0, 0 ) )
          nPfns, pfnSize = bkPathUsage.get( bkPath, {} ).get( 'All', ( 0, 0 ) )
          gLogger.always( '\t%s (%d LFNs, %s), (%d PFNs, %s, %.1f replicas)' % ( bkPath, nLfns, prSize( lfnSize ), nPfns, prSize( pfnSize ), float( nPfns ) / float( nLfns ) ) )
        gLogger.always( "\nA total of %d %s LFNs (%s), %d PFNs (%s) were not used" % ( counters['LFN'][0], infoType, prSize( counters['LFN'][1] ), counters['All'][0], prSize( counters['All'][1] ) ) )
      # noBKDirectories = sorted( unusedDirectories - set( bkPathForLfn ) )
  else:
    unusedBKPaths = set()
    datasetStorage['Disk'] -= datasetStorage['Tape']

  # Now create a CSV file with all dataset information
  # Name, ProcessingPass, #files, size, SE type, each week's usage (before now)
  csvFile = 'popularity-%ddays.csv' % since
  gLogger.always( "\n=============================================================" )
  gLogger.always( 'Creating %s file with %d datasets' % ( csvFile, len( timeUsage ) + len( unusedBKPaths ) ) )
  f = open( csvFile, 'w' )
  title = "Name,Configuration,ProcessingPass,FileType,Type,Creation-%s," % binSize + \
          "NbLFN,LFNSize,NbDisk,DiskSize,NbTape,TapeSize,NbArchived,ArchivedSize," + \
          ','.join( [site.split( '.' )[1] for site in storageSites] ) + \
          ",Nb Replicas,Nb ArchReps,Storage,FirstUsage,LastUsage,Now"
  for binNumber in range( nbBins ):
    title += ',%d' % ( 1 + binNumber )
  f.write( title.replace( ',', ';' ) + '\n' )
  TB = 1000. * 1000. * 1000. * 1000.
  for bkPath in sorted( timeUsage ) + sorted( unusedBKPaths ):
    if bkPath.startswith( 'Unknown-' ):
      continue
    info = bkPathUsage.get( bkPath, {} )
    # check if the production is still active
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
    for infoType in info:
      info[infoType][1] /= TB
    # Some BK paths contain a , to be replaces by a . for the CSV file!!
    config = '/'.join( bkPath.split( '/' )[0:3] )
    fileType = bkPath.split( '/' )[-1]
    if ',' in bkPath:
      gLogger.always( "BK path found with ',':", bkPath )
    # Name,Configuration,ProcessingPass, FileType
    row = '%s,%s,%s,%s' % ( bkPath.replace( ',', '.' ).replace( 'Real Data', 'RealData' ), config, processingPass.get( bkPath, 'Unknown' ).replace( 'Real Data', 'RealData' ), fileType )
    # Type
    row += ',0' if bkPath.startswith( '/MC' ) else ',1'
    # CreationTime
    row += ',%d' % ( getTimeBin( creationTime ) )
    # NbLFN,LFNSize,NbDisk,DiskSize,NbTape,TapeSize, NbArchived,ArchivedSize
    for infoType in ( 'LFN', 'Disk', 'Tape', 'Archived' ):
      row += ',%d,%f' % tuple( info[infoType] )
    for site in storageSites:
      row += ',%f' % info[site][1]
    row += ',%f,%f' % ( float( info['Disk'][0] ) / float( info['LFN'][0] ), float( info['Archived'][0] ) / float( info['LFN'][0] ) )
    if active:
      dsType = 'Active'
    else:
      dsType = 'Unknown'
      for infoType in storageTypes[0:3]:
        if bkPath in datasetStorage[infoType]:
          dsType = infoType
          break
    row += ',%s' % dsType
    bins = sorted( timeUsage.get( bkPath, {} ) )
    if not bins:
      bins = [0]
    row += ',%d,%d,%d' % ( bins[0], bins[-1], nowBin )
    usage = 0
    for binNumber in range( nbBins ):
      usage += timeUsage.get( bkPath, {} ).get( nowBin - binNumber, 0 )
      row += ',%d' % usage
    f.write( row.replace( ',', ';' ) + '\n' )
  f.close()
  gLogger.always( '\nSuccessfully wrote CSV file %s' % csvFile )

