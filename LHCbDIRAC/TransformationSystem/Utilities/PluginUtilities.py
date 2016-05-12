"""
  Utilities for scripts dealing with transformations
"""
__RCSID__ = "$Id$"

import os
import datetime
import random
import time

from DIRAC import gConfig, gLogger, S_OK, S_ERROR, exit as DIRACExit
from DIRAC.Core.Utilities.List import breakListIntoChunks
from DIRAC.Core.Utilities.Time import timeThis
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.DataManagementSystem.Utilities.DMSHelpers import DMSHelpers

from DIRAC.TransformationSystem.Client.Utilities import PluginUtilities as DIRACPluginUtilities
from DIRAC.TransformationSystem.Client.Utilities import isArchive, isFailover, getActiveSEs

from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient, BKClientWithRetry
from LHCbDIRAC.BookkeepingSystem.Client.BKQuery import BKQuery
from LHCbDIRAC.TransformationSystem.Client.TransformationClient import TransformationClient
from LHCbDIRAC.ResourceStatusSystem.Client.ResourceManagementClient import ResourceManagementClient

class PluginUtilities( DIRACPluginUtilities ):
  """
  Utility class used by plugins
  """

  def __init__( self, plugin = 'LHCbStandard', transClient = None, dataManager = None, fc = None,
                bkClient = None, rmClient = None,
                debug = False, transInThread = None, transID = None ):
    """
    c'tor
    """
    # clients
    if transClient is None:
      self.transClient = TransformationClient()
    else:
      self.transClient = transClient
    if bkClient is None:
      bkClient = BookkeepingClient()
    self.bkClient = BKClientWithRetry( bkClient )
    if rmClient is None:
      self.rmClient = ResourceManagementClient()
    else:
      self.rmClient = rmClient

    super( PluginUtilities, self ).__init__( plugin = plugin, transClient = self.transClient,
                                             dataManager = dataManager, fc = fc,
                                             debug = debug, transInThread = transInThread, transID = transID )

    self.freeSpace = {}
    self.cachedLFNAncestors = {}
    self.cachedNbRAWFiles = {}
    self.cachedRunLfns = {}
    self.cachedProductions = {}
    self.cachedLastRun = 0
    self.cachedLFNProcessedPath = {}
    self.cacheFile = ''
    self.filesParam = {}
    self.transRunFiles = {}
    self.notProcessed = {}
    self.cacheHitFrequency = max( 0., 1 - self.getPluginParam( 'RunCacheUpdateFrequency', 0.05 ) )
    self.__runExpired = {}
    self.__recoType = ''
    self.dmsHelper = DMSHelpers()
    self.runDestinations = {}
    self.cachedDirMetadata = {}
    self.runsUsedForShares = set()
    self.shareMetrics = None


  def setDebug( self, val ):
    self.debug = val

  def updateSharesUsage( self, counters, se, count, runID ):
    """ Update the usage counters if share is by files, and the run duration otherwise """
    inc = 0
    if self.shareMetrics == 'Files':
      inc = count
    else:
      self.logVerbose( "Updating SE %s for run %d which is %salready used" %
                       ( se, runID, 'not ' if runID not in self.runsUsedForShares else '' ) )
      if runID not in self.runsUsedForShares:
        # Update with run duration
        res = self.__getRunDuration( runID )
        if not res['OK']:
          self.logError( "Error getting run time", res['Message'] )
        else:
          inc = res['Value']
          self.runsUsedForShares.add( runID )
    if inc:
      counters[se] = counters.setdefault( se, 0 ) + inc
      self.logVerbose( 'New %s counter for %s: %d (+%d)' % ( self.shareMetrics, se, counters[se], inc ) )
      self.printShares( "New counters and used fraction (%)", counters, log = self.logVerbose )

  def printShares( self, title, shares, counters = None, log = None ):
    if log is None:
      log = self.logInfo
    if counters is None:
      counters = shares
    normCounters = normaliseShares( counters ) if counters else {}

    log( title + " (metrics is %s)" % self.shareMetrics )
    for se in sorted( shares ):
      infoStr = "%s: %4.1f |" % ( se.ljust( 15 ), shares[se] )
      if se in counters:
        infoStr += " %4.1f" % normCounters[se]
      log( infoStr )

  def getPluginShares( self, section = None, backupSE = None ):
    """
    Get shares from CS:
    * If backupSE is not present: just return the CS shares as they are
    * If backupSE is specified, the shares represent a percentage of the RAW at each site and the rest is for backupSE

    If the ShareMetrics is Files:
    * Use the number of files as metrics for SE usage
    Else:
    * Use the duration (in seconds) of runs as metrics for SE usage
    """
    if self.shareMetrics is None:
      self.shareMetrics = self.getPluginParam( 'ShareMetrics', '' )
    if section is None:
      # This is for reconstruction shares (CU)
      sharesSections = { 'DataReconstruction': 'CPUforRAW', 'DataReprocessing' : 'CPUforReprocessing'}
      res = self.transClient.getTransformation( self.transID )
      if not res['OK']:
        self.logError( "Cannot get information on transformation" )
        return res
      else:
        transType = res['Value']['Type']
      section = sharesSections.get( transType, 'CPUforRAW' )
    res = getShares( section )
    if not res['OK']:
      self.logError( "There is no CS section %s" % section, res['Message'] )
      return res
    rawFraction = None
    if backupSE:
      # Apply these processing fractions to the RAW distribution shares
      rawFraction = res['Value']
      result = getShares( 'RAW', normalise = True )
      if result['OK']:
        rawShares = result['Value']
        shares = dict( ( se, rawShares[se] * rawFraction[se] ) for se in set( rawShares ) & set( rawFraction ) )
        tier1Fraction = sum( shares.values() )
        shares[backupSE] = 100. - tier1Fraction
      else:
        return res
      self.logInfo( "Fraction of RAW (%s) to be processed at each SE (%%):" % section )
      for se in sorted( rawFraction ):
        self.logInfo( "%s: %.1f" % ( se.ljust( 15 ), 100. * rawFraction[se] ) )
    else:
      shares = normaliseShares( res['Value'] )
      self.logInfo( "Obtained the following target distribution shares (%):" )
      for se in sorted( shares ):
        self.logInfo( "%s: %.1f" % ( se.ljust( 15 ), shares[se] ) )

    # Get the existing destinations from the transformationDB, just for printing
    if self.shareMetrics == 'Files':
      res = self.getExistingCounters( requestedSEs = sorted( shares ) )
    else:
      res = self.getSitesRunsDuration( requestedSEs = sorted( shares ) )
    if not res['OK']:
      self.logError( "Failed to get used share", res['Message'] )
      return res
    else:
      existingCount = res['Value']
      self.printShares( "Target shares and usage for production (%):", shares, existingCount )
    if rawFraction:
      return S_OK( ( rawFraction, shares ) )
    else:
      return S_OK( ( existingCount, shares ) )

  def __getRunDuration( self, runID ):
    res = self.getTransformationRuns( runs = runID )
    if not res['OK']:
      return res
    runDictList = res['Value']
    # Get run metadata
    runMetadata = dict( ( runDict['RunNumber'], self.transClient.getRunsMetadata( runDict['RunNumber'] ).get( 'Value', {} ) ) for runDict in runDictList )
    return S_OK( self.__extractRunDuration( runMetadata, runID ) )

  def __extractRunDuration( self, runMetadata, runID ):
    duration = runMetadata.get( runID, {} ).get( 'Duration' )
    if duration is None:
      res = self.bkClient.getRunInformation( { 'RunNumber':[runID], 'Fields':['JobStart', 'JobEnd']} )
      if not res['OK']:
        self.logError( "Error getting run start/end information", res['Message'] )
        duration = 0
      if runID in res['Value']:
        start = res['Value'][runID]['JobStart']
        end = res['Value'][runID]['JobEnd']
        duration = ( end - start ).seconds
      else:
        duration = 0
    return duration

  def getSitesRunsDuration( self, transID = None, normalise = False, requestedSEs = None ):
    """
    Get per site how much time of run was assigned
    """
    res = self.getTransformationRuns( transID = transID )
    if not res['OK']:
      return res
    runDictList = res['Value']
    # Get run metadata
    runMetadata = dict( ( runDict['RunNumber'], self.transClient.getRunsMetadata( runDict['RunNumber'] ).get( 'Value', {} ) ) for runDict in runDictList )

    seUsage = {}
    for runDict in runDictList:
      runID = runDict['RunNumber']
      selectedSEs = runDict['SelectedSite']
      selectedSEs = set( selectedSEs.split( ',' ) ) if selectedSEs is not None else set()
      if requestedSEs:
        selectedSEs &= set( requestedSEs )
        if not selectedSEs:
          continue
      duration = self.__extractRunDuration( runMetadata, runID )
      self.runsUsedForShares.add( runID )
      for se in selectedSEs:
        seUsage[se] = seUsage.setdefault( se, 0 ) + duration

    if normalise:
      seUsage = normaliseShares( seUsage )
    return S_OK( seUsage )


  def getExistingCounters( self, transID = None, normalise = False, requestedSEs = None ):
    """
    Used by RAWShares and AtomicRun, gets what has been done up to now while distributing runs
    """
    if transID is None:
      transID = self.transID
    res = self.transClient.getCounters( 'TransformationFiles', ['UsedSE'],
                                        {'TransformationID':transID} )
    if not res['OK']:
      return res
    usageDict = {}
    total = sum( count for _uDict, count in res['Value'] )
    for usedDict, count in res['Value']:
      usedSE = usedDict['UsedSE']
      if usedSE != 'Unknown':
        usageDict[usedSE] = count
    if requestedSEs:
      requestedSEs = set( requestedSEs )
      seDict = {}
      for se, count in usageDict.iteritems():
        overlap = set( se.split( ',' ) ) & requestedSEs
        if overlap:
          for ov in overlap:
            seDict[ov] = seDict.setdefault( ov, 0 ) + count
        else:
          self.logWarn( "%s is in counters but not in required list" % se )
      usageDict = seDict
    if normalise:
      usageDict = normaliseShares( usageDict )
    return S_OK( usageDict )

  def getBookkeepingMetadata( self, lfns, param ):
    filesParam = {}
    for chunk in breakListIntoChunks( lfns, 1000 ):
      res = self.bkClient.getFileMetadata( chunk )
      if res['OK']:
        success = res['Value']['Successful']
        filesParam.update( dict( ( lfn, success[lfn].get( param ) ) for lfn in success ) )
        # Always cache the size, will be useful
        self.cachedLFNSize.update( dict( ( lfn, success[lfn].get( 'FileSize' ) ) for lfn in success ) )
      else:
        return res
    return S_OK( filesParam )

  def getProductions( self, bkPathList, eventType, transStatus ):
    now = datetime.datetime.utcnow()
    period = self.getPluginParam( 'Period', 6 )
    cacheLifeTime = self.getPluginParam( 'CacheLifeTime', 24 )
    productions = self.getCachedProductions()
    if 'CacheTime' in productions and ( now - productions['CacheTime'] ) < datetime.timedelta( hours = cacheLifeTime ):
      # If we haven't found productions for one of the processing passes, retry
      cacheOK = len( [bkPath for bkPath in bkPathList if bkPath not in productions.get( 'List', {} )] ) == 0
    else:
      cacheOK = False
    if cacheOK:
      if transStatus != 'Flush' and ( now - productions['LastCall_%s' % self.transID] ) < datetime.timedelta( hours = period ):
        self.logInfo( "Skip this loop (less than %s hours since last call)" % period )
        return None
    else:
      productions.setdefault( 'List', {} )
      self.logVerbose( "Cache is being refreshed (lifetime %d hours)" % cacheLifeTime )
      for bkPath in bkPathList:
        bkQuery = BKQuery( bkPath, visible = 'All' )
        bkQuery.setOption( 'EventType', eventType )
        prods = bkQuery.getBKProductions()
        if not prods:
          self.logVerbose( "For bkPath %s, found no productions, wait next time" % ( bkPath ) )
          return None
        productions['List'][bkPath] = []
        self.logVerbose( "For bkPath %s, found productions %s" % \
                              ( bkPath, ','.join( ['%s' % prod for prod in prods] ) ) )
        for prod in prods:
          res = self.transClient.getTransformation( prod )
          if not res['OK']:
            self.logError( "Error getting transformation %s" % prod, res['Message'] )
          else:
            if res['Value']['Status'] != "Cleaned":
              productions['List'][bkPath].append( int( prod ) )
        self.logInfo( "For bkPath %s, selected productions: %s" % \
                           ( bkPath, ','.join( ['%s' % prod for prod in productions['List'][bkPath] ] ) ) )
      productions['CacheTime'] = now

    productions['LastCall_%s' % self.transID] = now
    self.setCachedProductions( productions )
    return productions

  @timeThis
  def getFilesParam( self, lfns, param ):
    if not self.filesParam:
      nCached = 0
      for run in self.cachedRunLfns:
        for paramValue in self.cachedRunLfns[run]:
          for lfn in self.cachedRunLfns[run][paramValue]:
            self.filesParam[lfn] = paramValue
            nCached += 1
      self.logVerbose( 'Found %d files cached for param %s' % ( nCached, param ) )
    filesParam = dict( ( lfn, self.filesParam.get( lfn ) ) for lfn in lfns )
    newLFNs = [lfn for lfn in lfns if not filesParam[lfn]]
    if newLFNs:
      res = self.getBookkeepingMetadata( newLFNs, param )
      if not res['OK']:
        return res
      filesParam.update( res['Value'] )
      self.filesParam.update( res['Value'] )
      self.logVerbose( "Obtained BK %s of %d files" % ( param, len( newLFNs ) ) )
    return S_OK( filesParam )

  def getStorageFreeSpace( self, candSEs ):
    """ Get free space in a list of SEs from the RSS
    """
    weight = {}
    for se in candSEs:
      weight[se] = self.getRMFreeSpace( se )
    self.logVerbose( "Free space from RSS: %s" % weight )
    return weight

  def getRMFreeSpace( self, se ):
    """ Get free space in an SE from the RSS
    """

    cacheLimit = datetime.datetime.utcnow() - datetime.timedelta( hours = 12 )

    if not ( se in self.freeSpace ) or self.freeSpace[ se ][ 'LastCheckTime' ] < cacheLimit:
      res = self.rmClient.getSEStorageSpace( se )
      if not res[ 'OK' ]:
        self.logError( 'Error when getting space for SE %s' % ( se, ), res[ 'Message' ] )
        return 0

      self.freeSpace[ se ] = res[ 'Value' ]

    free = self.freeSpace[ se ][ 'Free' ]
    token = self.freeSpace[ se ][ 'Token' ]

    # ubeda: I do not fully understand this 'hack'
    if token == 'LHCb-Tape':
      self.logDebug( 'Hardcoded LHCb-Tape space to 1000.' )
      free = 1000.

    self.logDebug( 'Free space for SE %s, token %s: %d' % ( se, token, free ) )
    return free

  def rankSEs( self, candSEs ):
    """ Ranks the SEs according to their free space
    """
    if len( candSEs ) <= 1:
      return candSEs
    # Weights should be obtained from the RSS or CS
    weightForSEs = self.getStorageFreeSpace( candSEs )
    rankedSEs = []
    while weightForSEs:
      if len( weightForSEs ) == 1:
        se = weightForSEs.keys()[0]
      else:
        weights = weightForSEs.copy()
        total = 0.
        orderedSEs = []
        for se, w in weights.iteritems():
          # Minimum space 1 GB in case all are 0
          total += max( w, 0.001 )
          weights[se] = total
          orderedSEs.append( se )
        rand = random.uniform( 0., total )
        self.logDebug( 'List of ordered SEs (random number is %.1f out of %.1f)' % ( rand, total ) )
        for se in orderedSEs:
          self.logDebug( '%s: %.1f' % ( se, weights[se] ) )
        for se in orderedSEs:
          if rand <= weights[se]:
            break
      self.logDebug( "Selected SE is %s" % se )
      rankedSEs.append( se )
      weightForSEs.pop( se )
    return rankedSEs

  def setTargetSEs( self, numberOfCopies, archive1SEs, archive2SEs, mandatorySEs, secondarySEs, existingSEs, exclusiveSEs = False ):
    """ Decide on which SEs to target from lists and current status of replication
        Policy is max one archive1, one archive 2, all mandatory SEs and required number of copies elsewhere
    """
    # Select active SEs
    nbArchive1 = min( 1, len( archive1SEs ) )
    nbArchive2 = min( 1, len( archive2SEs ) )
    archive1ActiveSEs = getActiveSEs( archive1SEs )
    if not archive1ActiveSEs:
      archive1ActiveSEs = archive1SEs
    archive2ActiveSEs = getActiveSEs( archive2SEs )
    if not archive2ActiveSEs:
      archive2ActiveSEs = archive2SEs
    secondaryActiveSEs = getActiveSEs( secondarySEs )

    targetSEs = []
    self.logVerbose( "Selecting SEs from %s, %s, %s, %s (%d copies) for files in %s" % ( archive1ActiveSEs,
                                                                                         archive2ActiveSEs,
                                                                                         mandatorySEs,
                                                                                         secondaryActiveSEs,
                                                                                         numberOfCopies,
                                                                                         existingSEs ) )
    # Ensure that we have a archive1 copy
    archive1Existing = [se for se in archive1SEs if se in existingSEs and se not in archive1ActiveSEs]
    ses = self.selectSEs( archive1Existing + self.rankSEs( archive1ActiveSEs ), nbArchive1, existingSEs )
    self.logVerbose( "Archive1SEs: %s" % ses )
    if len( ses ) < nbArchive1 :
      self.logError( 'Cannot select archive1SE in active SEs' )
      return None
    targetSEs += ses

    # ... and an Archive2 copy
    archive2Existing = [se for se in archive2SEs if se in existingSEs and se not in archive2ActiveSEs]
    ses = self.selectSEs( archive2Existing + self.rankSEs( archive2ActiveSEs ), nbArchive2, existingSEs )
    self.logVerbose( "Archive2SEs: %s" % ses )
    if len( ses ) < nbArchive2 :
      self.logError( 'Cannot select archive2SE in active SEs' )
      return None
    targetSEs += ses

    # Now select the secondary copies
    # Missing secondary copies, make a list of candidates
    candidateSEs = [se for se in mandatorySEs if not self.isSameSEInList( se, existingSEs )]
    candidateSEs += [se for se in existingSEs if not self.isSameSEInList( se, targetSEs + candidateSEs ) and not isArchive( se )]
    candidateSEs += [se for se in self.rankSEs( secondaryActiveSEs ) if not self.isSameSEInList( se, candidateSEs )]
    ses = self.selectSEs( candidateSEs, numberOfCopies, existingSEs )
    self.logVerbose( "SecondarySEs: %s" % ses )
    if len( ses ) < numberOfCopies:
      self.logError( "Can not select enough Active SEs as SecondarySE" )
      return None
    targetSEs += ses

    if exclusiveSEs:
      targetSEs = [se for se in targetSEs if se not in existingSEs]
    self.logVerbose( "Selected target SEs: %s" % targetSEs )
    return ','.join( sorted( targetSEs ) )

  def selectSEs( self, candSEs, needToCopy, existingSEs ):
    """ Select SEs from a list, preferably from existing SEs
        in order to obtain the required number of replicas
    """
    targetSEs = []
    for se in [se for se in candSEs if se in existingSEs]:
      if needToCopy <= 0:
        break
      targetSEs.append( se )
      needToCopy -= 1
    if needToCopy > 0:
      for se in [s for s in candSEs if s not in existingSEs]:
        if needToCopy <= 0:
          break
        if not self.isSameSEInList( se, existingSEs ):
          targetSEs.append( se )
          needToCopy -= 1
    return targetSEs

  def assignTargetToLfns( self, lfns, replicas, stringTargetSEs ):
    """ Assign target SEs for each LFN, excluding the existing ones
        Returns a dictionary for files to be transferred and a list of files already in place
    """
    # Suppress duplicate SEs from list
    targetSEs = self.uniqueSEs( [se for se in stringTargetSEs.split( ',' ) if se] )
    alreadyCompleted = []
    fileTargetSEs = {}
    for lfn in lfns:
      existingSEs = [se for se in replicas.get( lfn, [] ) if not isFailover( se )]
      if not existingSEs:
        self.logWarn( 'File found without replicas', lfn )
        continue
      # Discard existing SEs or SEs with similar description
      neededSEs = [se for se in targetSEs if not self.isSameSEInList( se, existingSEs ) ]
      if not neededSEs:
        alreadyCompleted.append( lfn )
      else:
        fileTargetSEs[lfn] = ','.join( sorted( neededSEs ) )
    return ( fileTargetSEs, alreadyCompleted )

  @timeThis
  def getProcessedFiles( self, lfns ):
    """
    Check which files have been processed by a given production, i.e. have a meaningful descendant
    """
    from LHCbDIRAC.DataManagementSystem.Client.ConsistencyChecks import getFileDescendants
    return getFileDescendants( self.transID, lfns, transClient = self.transClient, dm = self.dm, bkClient = self.bkClient, verbose = self.debug )

  @timeThis
  def getRAWAncestorsForRun( self, runID, param = None, paramValue = None, getFiles = False ):
    """
    Determine from BK how many ancestors files from a given run we have.
    This is used for deciding when to flush a run (when all RAW files have been processed)
    """
    ancestors = 0
    # The transformation files cannot be cached globally as they evolve at each cycle
    lfns = self.transRunFiles.get( runID, [] )
    if not lfns:
      res = self.getTransformationFiles( runID = runID )
      if not res['OK']:
        self.logError( "Cannot get transformation files for run %s: %s" % ( str( runID ), res['Message'] ) )
        return 0
      excludedStatuses = self.getPluginParam( 'IgnoreStatusForFlush', [ 'Removed', 'MissingInFC', 'Problematic' ] )
      lfns = [fileDict['LFN'] for fileDict in res['Value'] if fileDict['Status'] not in excludedStatuses]
      self.transRunFiles[runID] = lfns
      self.logVerbose( 'Obtained %d input files for run %d' % ( len( lfns ), runID ) )

    if not lfns:
      return 0

    # Restrict to files with the required parameter
    if param:
#       paramStr = ' (%s:%s)' % ( param, paramValue if paramValue else '' )
      res = self.getFilesParam( lfns, param )
      if not res['OK']:
        self.logError( "Error getting BK param %s:" % param, res['Message'] )
        return 0
      paramValues = res['Value']
      lfns = [f for f, v in paramValues.iteritems() if v == paramValue]
#     else:
#       paramStr = ''
    if lfns:
      lfnToCheck = lfns[0]
    else:
      lfnToCheck = None
    # Get number of ancestors for known files
    cachedLfns = self.cachedLFNAncestors.get( runID, {} )
    setLfns = set( lfns )
    hitLfns = setLfns & set( cachedLfns )
    if hitLfns and not getFiles:
      self.logVerbose( "Ancestors cache hit for run %d: %d files cached" % \
                       ( runID, len( hitLfns ) ) )
      ancestors += sum( [cachedLfns[lfn] for lfn in hitLfns] )
      lfns = list( setLfns - hitLfns )

    # If some files are unknown, get the ancestors from BK
    ancestorFiles = set()
    if lfns:
      res = self.getFileAncestors( lfns )
      if res['OK']:
        ancestorDict = res['Value']['Successful']
      else:
        self.logError( "Error getting ancestors: %s" % res['Message'] )
        ancestorDict = {}
      for lfn in ancestorDict:
        ancFiles = [f['FileName'] for f in ancestorDict[lfn] if f['FileType'] == 'RAW']
        ancestorFiles.update( ancFiles )
        n = len( ancFiles )
        self.cachedLFNAncestors.setdefault( runID, {} )[lfn] = n
        ancestors += n

    if getFiles:
      return ancestorFiles
    notProcessed = self.__getNotProcessedAncestors( runID, lfnToCheck )
    if notProcessed:
      self.logVerbose( "Found %d files not processed for run %d" % ( notProcessed, runID ) )
      ancestors += notProcessed
    self.logVerbose( "found %d for run %d" % ( ancestors, runID ) )
    return ancestors

  def __getNotProcessedAncestors( self, runID, lfnToCheck ):
    """
    returns the number of RAW ancestor files that were not processed by the reconstruction production
    This is necessary only if that produciton is not processing all files, but in the doubt we check it
    """
    if runID in self.notProcessed or not lfnToCheck:
      return self.notProcessed.get( runID, 0 )
    ancestorFullDST = None
    recoProduction = None
    notProcessed = 0
    if not self.__recoType:
      self.__recoType = self.getPluginParam( 'RecoFileType', ['FULL.DST'] )
    # Check if the file itself is a FULL.DST
    res = self.bkClient.getFileMetadata( lfnToCheck )
    if res['OK']:
      if res['Value']['Successful'].get( lfnToCheck, {} ).get( 'FileType' ) in self.__recoType:
        ancestorFullDST = lfnToCheck
      else:
        # If not, get its ancestors
        res = self.getFileAncestors( lfnToCheck, replica = False )
        if res['OK']:
          fullDst = [f['FileName'] for f in res['Value']['Successful'].get( lfnToCheck, [{}] ) if f.get( 'FileType' ) in self.__recoType]
          if fullDst:
            ancestorFullDST = fullDst[0]
        else:
          self.logError( "Error getting ancestors of %s" % lfnToCheck, res['Message'] )
    if ancestorFullDST:
      self.logDebug( "Ancestor reco file found: %s" % ancestorFullDST )
      res = self.bkClient.getJobInfo( ancestorFullDST )
      if res['OK']:
        try:
          recoProduction = res['Value'][0][18]
          self.logVerbose( 'Reconstruction production is %d' % recoProduction )
        except Exception as _e:
          self.logException( "Exception extracting reco production from %s" % str( res['Value'] ) )
          recoProduction = None
      else:
        self.logError( "Error getting job information", res['Message'] )
    else:
      self.logVerbose( "No ancestor reco file found" )
    if recoProduction:
      res = self.transClient.getTransformationFiles( { 'TransformationID':recoProduction, 'RunNumber':runID} )
      if res['OK']:
        notProcessed = len( [fileDict for fileDict in res['Value'] if fileDict['Status'] == 'NotProcessed'] )
    self.notProcessed[runID] = notProcessed
    return notProcessed

  @timeThis
  def getTransformationFiles( self, runID ):
    return self.transClient.getTransformationFiles( { 'TransformationID' : self.transID, 'RunNumber': runID } )

  @timeThis
  def getFileAncestors( self, lfns, depth = 10, replica = True ):
    return self.bkClient.getFileAncestors( lfns, depth = depth, replica = replica )

  @timeThis
  def getTransformationRuns( self, runs = None, transID = None ):
    """ get the run table for a list of runs, if missing, add them """
    if transID is None:
      transID = self.transID
    condDict = {'TransformationID':transID}
    if isinstance( runs, ( dict, list, tuple, set ) ):
      condDict['RunNumber'] = list( runs )
    elif isinstance( runs, int ):
      runs = [runs]
      condDict['RunNumber'] = runs
    result = self.transClient.getTransformationRuns( condDict )
    if not result['OK']:
      return result
    # Check that all runs are there, if not add them
    runsFound = set( run['RunNumber'] for run in result['Value'] )
    if runs:
      missingRuns = set( runs ) - runsFound
      if missingRuns:
        self.logInfo( 'Add missing runs in transformation runs table: %s' % ','.join( [str( run ) for run in sorted( missingRuns )] ) )
        for runID in missingRuns:
          res = self.transClient.insertTransformationRun( transID, runID, '' )
          if not res['OK']:
            return res
        result = self.transClient.getTransformationRuns( condDict )
    return result

  @timeThis
  def groupByRunAndParam( self, lfns, files, param = '' ):
    """ Group files by run and another BK parameter (e.g. file type or event type)
    """
    runDict = {}
    # no need to query the BK as we have the answer from files
    lfns = [ fileDict for fileDict in files if fileDict['LFN'] in lfns]
    self.logVerbose( "Starting groupByRunAndParam for %d files, %s" % ( len( lfns ), 'by %s' % param if param else 'no param' ) )
    res = groupByRun( lfns )
    runGroups = res['Value']
    for runNumber in runGroups:
      runLFNs = runGroups[runNumber]
      if not param:
        runDict[runNumber] = {None:runLFNs}
      else:
        runDict[runNumber] = {}
        res = self.getFilesParam( runLFNs, param )
        if not res['OK']:
          self.logError( 'Error getting %s for %d files of run %d' % ( param, len( runLFNs ), runNumber ), res['Message'] )
        else:
          for lfn in res['Value']:
            runDict[runNumber].setdefault( res['Value'][lfn], [] ).append( lfn )
    if param:
      self.logVerbose( "Grouped %d files by run and %s" % ( len( lfns ), param ) )
    return S_OK( runDict )


  def createTasks( self, storageElementGroups, chunkSize = None ):
    """ Create reasonable size tasks
    """
    tasks = []
    if not chunkSize:
      chunkSize = self.getPluginParam( 'MaxFiles', 100 )
    for stringTargetSEs in sorted( storageElementGroups.keys() ):
      stringTargetLFNs = storageElementGroups[stringTargetSEs]
      for lfnGroup in breakListIntoChunks( sorted( stringTargetLFNs ), chunkSize ):
        tasks.append( ( stringTargetSEs, lfnGroup ) )
    self.logVerbose( "%d tasks created" % len( tasks ) )
    return tasks

  def readCacheFile( self, workDirectory ):
    """ Utility function
    """
    import pickle
    # Now try and get the cached information
    tmpDir = os.environ.get( 'TMPDIR', '/tmp' )
    cacheFiles = ( ( workDirectory, ( 'TransPluginCache' ) ),
                   ( tmpDir, ( 'dirac', 'TransPluginCache' ) ) )
    for ( cacheFile, prefixes ) in cacheFiles:
      if not cacheFile:
        continue
      if isinstance( prefixes, str ):
        prefixes = [prefixes]
      for node in prefixes:
        cacheFile = os.path.join( cacheFile, node )
        if not os.path.exists( cacheFile ):
          os.mkdir( cacheFile )
      cacheFile = os.path.join( cacheFile, "Transformation_%s.pkl" % ( str( self.transID ) ) )
      if not self.cacheFile:
        self.cacheFile = cacheFile
      try:
        f = open( cacheFile, 'r' )
        self.cachedLFNAncestors = pickle.load( f )
        self.cachedNbRAWFiles = pickle.load( f )
        self.cachedLFNSize = pickle.load( f )
        self.cachedRunLfns = pickle.load( f )
        self.cachedProductions = pickle.load( f )
        self.cachedLastRun = pickle.load( f )
        self.cachedLFNProcessedPath = pickle.load( f )
        f.close()
        self.logVerbose( "Cache file %s successfully loaded" % cacheFile )
        # print '*****'
        # print '\n'.join( ['%s %s' % ( key, val ) for key, val in self.cachedLFNProcessedPath.items()] )
        break
      except EOFError:
        f.close()
      except:
        self.logVerbose( "Cache file %s could not be loaded" % cacheFile )

  def getCachedRunLFNs( self, runID, paramValue ):
    return set( self.cachedRunLfns.get( runID, {} ).get( paramValue, [] ) )

  def setCachedRunLfns( self, runID, paramValue, lfnList ):
    self.cachedRunLfns.setdefault( runID, {} )[paramValue] = lfnList

  def getCachedProductions( self ):
    return self.cachedProductions

  def setCachedProductions( self, productions ):
    self.cachedProductions = productions

  def getCachedLastRun( self ):
    return self.cachedLastRun

  def setCachedLastRun( self, lastRun ):
    self.cachedLastRun = lastRun

  def cacheExpired( self, runID ):
    if runID not in self.__runExpired:
      self.__runExpired[runID] = ( random.uniform( 0., 1. ) > self.cacheHitFrequency )
    return self.__runExpired[runID]

  @timeThis
  def getNbRAWInRun( self, runID, evtType ):
    """ Get the number of RAW files in a run
    """
    # Every now and then refresh the cache
    rawFiles = self.cachedNbRAWFiles.get( runID, {} ).get( evtType )
    if not rawFiles:
      res = self.bkClient.getNbOfRawFiles( {'RunNumber':runID, 'EventTypeId':evtType, 'Finished':'Y'} )
      if not res['OK']:
        self.logError( "Cannot get number of RAW files for run %d, evttype %d" % ( runID, evtType ) )
        return 0
      rawFiles = res['Value']
      self.cachedNbRAWFiles.setdefault( runID, {} )[evtType] = rawFiles
      self.logVerbose( "Run %d has %d RAW files" % ( runID, rawFiles ) )
    return rawFiles


  def writeCacheFile( self ):
    """ Utility function
    """
    import pickle
    if self.cacheFile:
      try:
        f = open( self.cacheFile, 'w' )
        pickle.dump( self.cachedLFNAncestors, f )
        pickle.dump( self.cachedNbRAWFiles, f )
        pickle.dump( self.cachedLFNSize, f )
        pickle.dump( self.cachedRunLfns, f )
        pickle.dump( self.cachedProductions, f )
        pickle.dump( self.cachedLastRun, f )
        pickle.dump( self.cachedLFNProcessedPath, f )
        f.close()
        self.logVerbose( "Cache file %s successfully written" % self.cacheFile )
      except:
        self.logError( "Could not write cache file %s" % self.cacheFile )

  def setRunForFiles( self, lfns ):
    res = self.bkClient.getFileMetadata( lfns )
    runFiles = {}
    if res['OK']:
      for lfn, metadata in res['Value']['Successful'].iteritems():
        runFiles.setdefault( metadata['RunNumber'], [] ).append( lfn )
      for run in sorted( runFiles ):
        if not run:
          self.logInfo( "%d files found for run '%s': %s" % ( len( runFiles[run] ), str( run ), str( runFiles[run] ) ) )
          runFiles.pop( run )
          continue
        res = self.transClient.addTransformationRunFiles( self.transID, run, runFiles[run] )
        # print run, runFiles[run], res
        if not res['OK']:
          self.logError( "Error setting %d files to run %d" % ( len( runFiles[run] ), run ), res['Message'] )
          runFiles.pop( run )
    else:
      self.logError( "Error getting metadata for %d files" % len( lfns ), res['Message'] )
    return runFiles

  def getTransQuery( self, transReplicas ):
    # Get BK query for this transformation as the BK path is relative to that one
    res = self.transClient.getBookkeepingQuery( self.transID )
    if not res['OK']:
      self.logError( "Failed to get BK query for transformation", res['Message'] )
      transQuery = {}
    else:
      transQuery = res['Value']
    if 'ProcessingPass' not in transQuery:
      # Get processing pass of the first file... This assumes all have the same...
      lfn = transReplicas.keys()[0]
      res = self.bkClient.getDirectoryMetadata( lfn )
      if not res['OK']:
        self.logError( "Error getting directory metadata", res['Message'] )
        return None
      transQuery = res['Value']['Successful'].get( lfn, [{}] )[0]
      # Strip off most of it
      for key in ( 'FileType', 'Production' ):
        transQuery.pop( key, None )
    return transQuery

  def cleanFiles( self, transFiles, transReplicas, status = None ):
    """
    Remove from transFiles all files without a replica and set their status
    """
    noReplicaFiles = []
    for fileDict in [fileDict for fileDict in transFiles]:
      if fileDict['LFN'] not in transReplicas:
        noReplicaFiles.append( fileDict['LFN'] )
        transFiles.remove( fileDict )
    if noReplicaFiles:
      if status is None:
        status = 'Problematic' if self.plugin not in getRemovalPlugins() else 'Processed'
      info = 'without replicas' if status == 'Problematic' else 'already processed'
      res = self.transClient.setFileStatusForTransformation( self.transID, status, noReplicaFiles )
      if not res['OK']:
        self.logError( 'Error setting file status for %d files' % len( noReplicaFiles ), res['Message'] )
      else:
        self.logInfo( 'Found %d files %s, status set to %s' % ( len( noReplicaFiles ), info, status ) )

  def __getRunDestinations( self, runIDList ):
    runSet = set( runIDList ) - set( self.runDestinations )
    if runSet:
      # Try and get a run destination from TS
      res = self.transClient.getDestinationForRun( list( runSet ) )
      if res['OK']:
        dest = res['Value']
        if dest:
          if isinstance( dest, list ):
            dest = dict( dest )
          for runID in [runID for runID in runSet if dest.get( runID )]:
            self.runDestinations[runID] = dest[runID]

  def getSEForDestination( self, runID, targets ):
    """ get the information on destination SE from within a list """
    self.__getRunDestinations( [runID] )
    site = self.runDestinations.get( runID )
    if site:
      self.logVerbose( 'Destination found for run %d: %s' % ( runID, site ) )
      res = self.dmsHelper.getSEInGroupAtSite( targets, site )
      return res.get( 'Value' )
    return None

  def filterNotProcessedFiles( self, lfns, prodList ):
    """ Select only the files that are Processed in a list of productions """
    res = self.transClient.getTransformationFiles( {'LFN':list( lfns ), 'TransformationID':prodList} )
    if not res['OK']:
      self.logError( "Error getting transformation files", res['Message'] )
      return []
    notProcessed = set( fDict['LFN'] for fDict in res['Value'] if fDict['Status'] != 'Processed' )
    lfnLeft = lfns - notProcessed
    if len( lfnLeft ) != len( lfns ):
      self.logVerbose( 'Reduce files from %d to %d (removing pending files)' % ( len( lfns ), len( lfnLeft ) ) )
    return lfnLeft

  def checkForDescendants( self, lfns, prodList, level = 0 ):
    """ check the files if they have an existing descendant in the list of productions """
    excludeTypes = ( 'LOG', 'BRUNELHIST', 'DAVINCIHIST', 'GAUSSHIST', 'HIST', 'INDEX.ROOT' )
    finalResult = set()
    if isinstance( lfns, basestring ):
      lfns = set( [lfns] )
    elif isinstance( lfns, list ):
      lfns = set( lfns )
    if level:
      lfns = self.filterNotProcessedFiles( lfns, prodList )
    # Get daughters
    res = self.bkClient.getFileDescendants( list( lfns ), depth = 1, checkreplica = False )
    if not res['OK']:
      return res
    success = res['Value']['WithMetadata']
    descToCheck = {}
    # Invert list of descendants
    descMetadata = {}
    for lfn in lfns:
      descendants = success.get( lfn, {} )
      descMetadata.update( descendants )
      for desc, metadata in descendants.iteritems():
        if metadata['FileType'] not in excludeTypes:
          descToCheck.setdefault( desc, set() ).add( lfn )
    res = self.__getProduction( descToCheck )
    if not res['OK']:
      return res
    descProd = res['Value']
    prodDesc = {}
    # Check if we found a descendant in the requested productions
    foundProds = {}
    for desc in descToCheck.keys():
      prod = descProd[desc]
      # If we found an existing descendant in the list of productions, all OK
      if descMetadata[desc]['GotReplica'] == 'Yes' and prod in prodList:
        foundProds.setdefault( prod, set() ).update( descToCheck[desc] )
        finalResult.update( descToCheck[desc] )
        del descToCheck[desc]
      else:
        # Sort remaining descendants in productions
        prodDesc.setdefault( prod, set() ).add( desc )
    for prod in foundProds:
      self.logVerbose( "Found %d files with descendants (out of %d) in production %d at level %d" % ( len( foundProds[prod] ), len( lfns ), prod, level + 1 ) )

    # Check descendants in reverse order of productions
    for prod in sorted( prodDesc, reverse = True ):
      # Check descendants whose parent was not yet found processed
      toCheckInProd = set()
      for desc in prodDesc[prod]:
        if not descToCheck[desc] & finalResult:
          toCheckInProd.add( desc )
      if toCheckInProd:
        res = self.checkForDescendants( toCheckInProd, prodList, level = level + 1 )
        if not res['OK']:
          return res
        foundInProd = set()
        for desc in res['Value']:
          foundInProd.update( descToCheck[desc] )
        if foundInProd:
          self.logVerbose( "Found descendants of %d files at depth %d" % ( len( foundInProd ), level + 2 ) )
          finalResult.update( foundInProd )

    return S_OK( finalResult )


  def __getProduction( self, lfns ):
    """
    Returns the production that was used to create each LFN
    """
    directories = {}
    lfnDirs = {}
    # Get the directories
    for lfn in lfns:
      dir = os.path.dirname( lfn )
      last = os.path.basename( dir )
      if len( last ) == 4 and last.isdigit():
        dir = os.path.dirname( dir )
      directories.setdefault( dir, lfn )
      lfnDirs[lfn] = dir

    dirList = [dir for dir in directories if dir not in self.cachedDirMetadata]
    for dir in dirList:
      res = self.bkClient.getJobInfo( directories[dir] )
      if not res['OK']:
        return res
      self.cachedDirMetadata.update( {dir : res['Value'][0][18]} )

    lfnProd = dict( ( lfn, self.cachedDirMetadata.get( lfnDirs[lfn], 0 ) ) for lfn in lfns )
    return S_OK( lfnProd )

#=================================================================
# Set of utility functions used by LHCbDirac transformation system
#=================================================================

def getRemovalPlugins():
  return ( "DestroyDataset", 'DestroyDatasetWhenProcessed' ,
           "RemoveDataset", "RemoveReplicas", 'RemoveReplicasWhenProcessed', 'ReduceReplicas' )
def getReplicationPlugins():
  return ( "LHCbDSTBroadcast", "LHCbMCDSTBroadcastRandom",
           "ArchiveDataset", "ReplicateDataset",
           'RAWReplication',
           'FakeReplication', 'ReplicateToLocalSE', 'Healing' )

def getShares( sType, normalise = False ):
  """
  Get the shares from the Resources section of the CS
  """
  optionPath = 'Shares/%s' % sType
  res = Operations().getOptionsDict( optionPath )
  if not res['OK']:
    res = gConfig.getOptionsDict( os.path.join( '/Resources', optionPath ) )
  if not res['OK']:
    return res
  if not res['Value']:
    return S_ERROR( "/Resources/Shares/%s option contains no shares" % sType )
  shares = res['Value']
  for site, value in shares.iteritems():
    shares[site] = float( value )
  if normalise:
    shares = normaliseShares( shares )
  if not shares:
    return S_ERROR( "No non-zero shares defined" )
  return S_OK( shares )


def normaliseShares( shares ):
  total = 0.0
  normShares = shares.copy()
  for site in shares:
    total += float( shares[site] )
  if not total:
    return shares
  for site in shares:
    normShares[site] = 100.0 * ( float( shares[site] ) / total )
  return normShares

@timeThis
def groupByRun( files ):
  """ Groups files by run
  files is a list of dictionaries containing the run number
  """
  runDict = {}
  for fileDict in files:
    runID = fileDict.get( 'RunNumber' )
    lfn = fileDict['LFN']
    if lfn:
      runDict.setdefault( runID if runID else 0, [] ).append( lfn )
  return S_OK( runDict )

def addFilesToTransformation( transID, lfns, addRunInfo = True ):
  transClient = TransformationClient()
  bk = BookkeepingClient()
  gLogger.info( "Adding %d files to transformation %s" % ( len( lfns ), transID ) )
  res = transClient.getTransformation( transID )
  if not res['OK']:
    return res
  transType = res['Value']['Type']
  addRunInfo = addRunInfo and transType != 'Removal'
  addedLfns = []
  for lfnChunk in breakListIntoChunks( lfns, 10000 ):
    runDict = {}
    if addRunInfo:
      res = bk.getFileMetadata( lfnChunk )
      if res['OK']:
        resMeta = res['Value'].['Successful']
        for lfn, metadata in resMeta.iteritems():
          runID = metadata.get( 'RunNumber' )
          if runID:
            runDict.setdefault( int( runID ), [] ).append( lfn )
      else:
        return res
    while True:
      res = transClient.addFilesToTransformation( transID, lfnChunk )
      if not res['OK']:
        gLogger.error( "Error adding %d files to transformation, retry..." % len( lfnChunk ), res['Message'] )
        time.sleep( 1 )
      else:
        break
    added = [lfn for ( lfn, status ) in res['Value']['Successful'].iteritems() if status == 'Added']
    addedLfns += added
    if addRunInfo and res['OK']:
      for runID, runLfns in runDict.iteritems():
        runLfns = [lfn for lfn in runLfns if lfn in added]
        if runLfns:
          res = transClient.addTransformationRunFiles( transID, runID, runLfns )
          if not res['OK']:
            break

    if not res['OK']:
      return res
  gLogger.info( "%d files successfully added to transformation" % len( addedLfns ) )
  return S_OK( addedLfns )

def optimizeTasks( tasks ):
  taskDict = {}
  for ses, lfns in tasks:
    taskDict.setdefault( ses, [] ).extend( lfns )
  return taskDict.items()

