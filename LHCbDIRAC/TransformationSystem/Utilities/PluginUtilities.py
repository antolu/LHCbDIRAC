"""
  Utilities for scripts dealing with transformations
"""
import os
import datetime
import random
import time

from DIRAC import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.Core.Utilities.List import breakListIntoChunks
# from DIRAC.Core.Utilities.Time import timeThis
from DIRAC.Core.Utilities.File import mkDir
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.DataManagementSystem.Utilities.DMSHelpers import DMSHelpers
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.Resources.Storage.StorageElement import StorageElement

from DIRAC.TransformationSystem.Client.Utilities import PluginUtilities as DIRACPluginUtilities
from DIRAC.TransformationSystem.Client.Utilities import isArchive, isFailover, getActiveSEs

from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient, BKClientWithRetry
from LHCbDIRAC.BookkeepingSystem.Client.BKQuery import BKQuery
from LHCbDIRAC.TransformationSystem.Client.TransformationClient import TransformationClient
from LHCbDIRAC.ResourceStatusSystem.Client.ResourceManagementClient import ResourceManagementClient

__RCSID__ = "$Id$"

def _stripNumDirectory( dirName ):
  """
  Method to remove numeric directory form a directory path
  """
  while True:
    subDir = os.path.basename( dirName )
    if subDir and not subDir.isdigit():
      return dirName
    dirName = os.path.dirname( dirName )

def _clearTaskLFNs( taskLfns ):
  """
  Clear the input list of lists, keeping its reference
  """
  nbLfns = 0
  for lfnList in taskLfns:
    nbLfns += len( lfnList )
    del lfnList[:]
  return nbLfns



class PluginUtilities( DIRACPluginUtilities ):
  """
  Utility class used by plugins
  """

  def __init__( self, plugin='LHCbStandard', transClient=None, dataManager=None, fc=None,
                bkClient=None, rmClient=None,
                debug=False, transInThread=None, transID=None ):
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

    super( PluginUtilities, self ).__init__( plugin=plugin, transClient=self.transClient,
                                             dataManager=dataManager, fc=fc,
                                             debug=debug, transInThread=transInThread, transID=transID )

    self.freeSpace = {}
    self.transFiles = []
    self.transReplicas = {}
    self.cachedLFNAncestors = {}
    self.cachedNbRAWFiles = {}
    self.usingRAWFiles = None
    self.cachedRunLfns = {}
    self.cachedProductions = {}
    self.cachedLastRun = 0
    self.cachedLFNProcessedPath = {}
    self.cacheFile = ''
    self.filesParam = {}
    self.transRunFiles = {}
    self.notProcessed = {}
    self.cacheHitFrequency = max( 0., 1 - self.getPluginParam( 'RunCacheUpdateFrequency', 0.1 ) )
    self.__runExpired = {}
    self.__recoType = ''
    self.dmsHelper = DMSHelpers()
    self.runDestinations = {}
    self.cachedDirMetadata = {}
    self.runsUsedForShares = set()
    self.shareMetrics = None
    self.lastCall = None
    self.excludedStatuses = self.getPluginParam( 'IgnoreStatusForFlush', [ 'Removed', 'MissingInFC', 'Problematic' ] )
    self.tsParams = ( 'FileSize', 'FileType', 'RunNumber' )
    self.paramName = None
    self.onlineCondDB = None

  def setDebug( self, val ):
    self.debug = val

  def updateSharesUsage( self, counters, se, count, runID ):
    """
    Update the usage counters if share is by files, and the run duration otherwise
    """
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
      self.printShares( "New counters and used fraction (%)", counters, log=self.logVerbose )

  def printShares( self, title, shares, counters=None, log=None ):
    """
    Print formatted shares and if provised counters

    :param title: header of print out
    :type title: string
    :param shares: Shares per SE or site
    :type shares: dictionary
    :param counters: Current values per SE or site
    :type counters: dictionary
    :param log: (sub)Logger instance to be used
    :type log: logging method
    """
    if log is None:
      log = self.logInfo
    if counters is None:
      counters = shares
    normCounters = normaliseShares( counters ) if counters else {}

    if self.shareMetrics:
      title += " (metrics is %s)" % self.shareMetrics
    log( title )
    for se in sorted( shares ):
      if isinstance( shares[se], ( int, long ) ):
        infoStr = "%s: %d " % ( se.rjust( 15 ), shares[se] )
      else:
        infoStr = "%s: %4.1f " % ( se.rjust( 15 ), shares[se] )
      if se in counters:
        infoStr += "| %4.1f %%" % normCounters[se]
      log( infoStr )

  def getPluginShares( self, section=None, backupSE=None ):
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
      result = getShares( 'RAW', normalise=True )
      if result['OK']:
        rawShares = result['Value']
        shares = dict( ( se, rawShares[se] * rawFraction[se] ) for se in set( rawShares ) & set( rawFraction ) )
        tier1Fraction = sum( shares.values() )
        shares[backupSE] = 100. - tier1Fraction
      else:
        return res
      rawPercentage = dict( ( se, 100. * val ) for se, val in rawFraction.iteritems() )
      self.printShares( "Fraction of RAW (%s) to be processed at each SE (%%):" % section,
                        rawPercentage, counters=[] )
    else:
      shares = normaliseShares( res['Value'] )
      self.logInfo( "Obtained the following target distribution shares (%):" )
      for se in sorted( shares ):
        self.logInfo( "%s: %.1f" % ( se.ljust( 15 ), shares[se] ) )

    # Get the existing destinations from the transformationDB, just for printing
    if self.shareMetrics == 'Files':
      res = self.getExistingCounters( requestedSEs=sorted( shares ) )
    else:
      res = self.getSitesRunsDuration( requestedSEs=sorted( shares ) )
    if not res['OK']:
      self.logError( "Failed to get used share", res['Message'] )
      return res
    else:
      existingCount = res['Value']
      self.printShares( "Target shares and usage for production (%):", shares, counters=existingCount )
    if rawFraction:
      return S_OK( ( rawFraction, shares ) )
    else:
      return S_OK( ( existingCount, shares ) )

  def __getRunDuration( self, runID ):
    """
    Get the run duration
    """
    # Get run metadata
    runMetadata = self.transClient.getRunsMetadata( [runID] ).get( 'Value', {} )
    return S_OK( self.__extractRunDuration( runMetadata, runID ) )

  def __extractRunDuration( self, runMetadata, runID ):
    """
    Obtain run duration from the TS run metadata table
    If not available, get it from BK
    """
    duration = runMetadata.get( runID, {} ).get( 'Duration' )
    if duration is None:
      self.logVerbose( 'Run duration not found in TS for run %d, get it from BK' % runID )
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
    return int( duration )

  def getSitesRunsDuration( self, transID=None, normalise=False, requestedSEs=None ):
    """
    Get per site how much time of run was assigned
    """
    res = self.getTransformationRuns( transID=transID )
    if not res['OK']:
      return res
    runDictList = res['Value']
    # Get run metadata
    runMetadata = self.transClient.getRunsMetadata( [runDict['RunNumber'] for runDict in runDictList] ).get( 'Value', {} )

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


  def getExistingCounters( self, transID=None, normalise=False, requestedSEs=None ):
    """
    Used by RAWReplication and RAWProcessing plugins, gets what has been done up to now while distributing runs
    """
    if transID is None:
      transID = self.transID
    res = self.transClient.getCounters( 'TransformationFiles', ['UsedSE'],
                                        {'TransformationID':transID} )
    if not res['OK']:
      return res
    usageDict = {}
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

  def getMetadataFromTSorBK( self, lfns, param ):
    """
    Get BK parameters from BK unless they are already present in the TS
    """
    if isinstance( lfns, basestring ):
      lfns = [lfns]
    filesParam = {}
    # For parameters that are in the TS files DB, no need to go to BK
    if param in self.tsParams:
      par = param if param != 'FileSize' else 'Size'
      # We already have this information
      for fileDict in self.transFiles:
        lfn = fileDict['LFN']
        if lfn in lfns:
          value = fileDict.get( par )
          if value:
            filesParam[lfn] = value

      lfnsLeft = set( lfns ) - set( filesParam )
      self.logVerbose( "Found parameter %s in transFiles for %d files, %d left" % ( param,
                                                                                    len( filesParam ),
                                                                                    len( lfnsLeft ) ) )
    else:
      lfnsLeft = lfns

    if lfnsLeft:
      for chunk in breakListIntoChunks( lfnsLeft, 1000 ):
        loggedError = False
        # Let's loop forever as this MUST succeed
        while True:
          res = self.bkClient.getFileMetadata( chunk )
          if res['OK']:
            success = res['Value']['Successful']
            filesParam.update( dict( ( lfn, success[lfn].get( param ) ) for lfn in success ) )
            break
          if not loggedError:
            self.logError( "Error getting BK file metadata", res['Message'] )
            loggedError = True
          time.sleep( 1 )
      # Check if all parameters are present in TS... If not, set them while we have them here
      lfnDict = {}
      for fileDict in self.transFiles:
        lfn = fileDict['LFN']
        if lfn in success:
          for param in self.tsParams:
            par = param if param != 'FileSize' else 'Size'
            if fileDict.get( par ) is None:
              lfnDict.setdefault( lfn, {} ).update( {par: success[lfn][param]} )
      if lfnDict:
        res = self.transClient.setParameterToTransformationFiles( self.transID, lfnDict )
        self.logVerbose( "Updated files metadata in TS for %d files: %s" % ( len( lfnDict ),
                                                                             'OK' if res['OK'] else 'Failed' ) )

    return filesParam

  def getProductions( self, bkPathList, eventType, transStatus ):
    """
    Get the list of productions matching a given BK path
    """
    now = datetime.datetime.utcnow()
    period = self.getPluginParam( 'Period', 6 )
    cacheLifeTime = self.getPluginParam( 'CacheLifeTime', 24 )
    productions = self.getCachedProductions()
    if 'CacheTime' in productions and ( now - productions['CacheTime'] ) < datetime.timedelta( hours=cacheLifeTime ):
      # If we haven't found productions for one of the processing passes, retry
      cacheOK = len( [bkPath for bkPath in bkPathList if bkPath not in productions.get( 'List', {} )] ) == 0
    else:
      cacheOK = False
    if cacheOK:
      if transStatus != 'Flush' and ( now - productions['LastCall_%s' % self.transID] )\
                                      < datetime.timedelta( hours=period ):
        self.logInfo( "Skip this loop (less than %s hours since last call)" % period )
        return None
    else:
      productions.setdefault( 'List', {} )
      self.logVerbose( "Cache is being refreshed (lifetime %d hours)" % cacheLifeTime )
      for bkPath in bkPathList:
        bkQuery = BKQuery( bkPath, visible='All' )
        bkQuery.setOption( 'EventType', eventType )
        prods = bkQuery.getBKProductions()
        if not prods:
          self.logVerbose( "For bkPath %s, found no productions, wait next time" % ( bkPath ) )
          return None
        productions['List'][bkPath] = []
        self.logVerbose( "For bkPath %s, found productions %s" % \
                              ( bkPath, ','.join( '%s' % prod for prod in prods ) ) )
        for prod in prods:
          res = self.transClient.getTransformation( prod )
          if not res['OK']:
            self.logError( "Error getting transformation %s" % prod, res['Message'] )
          else:
            if res['Value']['Status'] != "Cleaned":
              productions['List'][bkPath].append( int( prod ) )
        self.logInfo( "For bkPath %s, selected productions: %s" % \
                           ( bkPath, ','.join( '%s' % prod for prod in productions['List'][bkPath] ) ) )
      productions['CacheTime'] = now

    productions['LastCall_%s' % self.transID] = now
    self.setCachedProductions( productions )
    return productions

  # @timeThis
  def getFilesParam( self, lfns, param ):
    """
    Return for each LFN the value of a single parameter "param" that can be cached
    """
    # Make sure we request the same parameter
    if self.paramName and self.paramName != param:
      self.logWarn( "Requested parameter %s not compatible with previously used parameter %s, \
get from BK" % ( param, self.paramName ) )
      return self.getMetadataFromTSorBK( lfns, param )

    self.paramName = param
    lfns = set( lfns )
    if not self.filesParam:
      nCached = 0
      for run in self.cachedRunLfns:
        for paramValue in self.cachedRunLfns[run]:
          for lfn in self.cachedRunLfns[run][paramValue]:
            self.filesParam[lfn] = paramValue
            nCached += 1
      self.logVerbose( 'Obtained %s of %d files from disk cache' % ( param, nCached ) )
    filesParam = dict( ( lfn, self.filesParam[lfn] ) for lfn in lfns & set( self.filesParam ) )
    # Get parameter for LFNs not cached yet
    lfns = lfns - set( filesParam )
    if lfns:
      resDict = self.getMetadataFromTSorBK( list( lfns ), param )
      filesParam.update( resDict )
      self.filesParam.update( resDict )
      self.logVerbose( "Obtained %s of %d files from TS or BK" % ( param, len( resDict ) ) )
    return filesParam

  def getStorageFreeSpace( self, candSEs ):
    """
    Get free space in a list of SEs from the RSS
    """
    weight = {}
    for se in candSEs:
      weight[se] = self.getRMFreeSpace( se )
    self.logVerbose( "Free space from RSS: %s" % weight )
    return weight

  def getRMFreeSpace( self, se ):
    """
    Get free space in an SE from the RSS
    """

    isTape = StorageElement( se ).status()['TapeSE']
    # For tape space token, the SRM information is useless, set it fixed
    if isTape:
      return 1000.

    cacheLimit = datetime.datetime.utcnow() - datetime.timedelta( hours=12 )

    if not ( se in self.freeSpace ) or self.freeSpace[ se ][ 'LastCheckTime' ] < cacheLimit:
      res = self.rmClient.getSEStorageSpace( se )
      if not res[ 'OK' ]:
        self.logError( 'Error when getting space for SE %s' % ( se, ), res[ 'Message' ] )
        return 0
      self.freeSpace[ se ] = res[ 'Value' ]

    free = self.freeSpace[ se ][ 'Free' ]
    self.logDebug( 'Free space for SE %s: %d' % ( se, free ) )
    return free

  def rankSEs( self, candSEs ):
    """
    Ranks the SEs according to their free space
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
        for se, weight in weights.iteritems():
          # Minimum space 1 GB in case all are 0
          total += max( weight, 0.001 )
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

  def setTargetSEs( self, numberOfCopies, archive1SEs, archive2SEs,
                    mandatorySEs, secondarySEs, existingSEs, exclusiveSEs=False ):
    """
    Decide on which SEs to target from lists and current status of replication
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

    # Now select the disk replicas
    # 1. add mandatory SEs
    candidateSEs = [se for se in mandatorySEs if not self.isSameSEInList( se, existingSEs )]
    # 2. add existing disk SEs that are either mandatory or secondary
    candidateSEs += [se for se in existingSEs if se in ( mandatorySEs + secondarySEs )\
                                              and not self.isSameSEInList( se, targetSEs + candidateSEs )\
                                              and not isArchive( se )]
    # 3. add ranked list of secondary SEs
    candidateSEs += [se for se in self.rankSEs( secondaryActiveSEs )
                     if not self.isSameSEInList( se, targetSEs + candidateSEs + existingSEs )]
    # 4. Select the proper number of SEs in the candidate ordered list
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
    """
    Select SEs from a list, preferably from existing SEs
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
    """
    Assign target SEs for each LFN, excluding the existing ones
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

  # @timeThis
  def getProcessedFiles( self, lfns ):
    """
    Check which files have been processed by a given production, i.e. have a meaningful descendant
    """
    from LHCbDIRAC.DataManagementSystem.Client.ConsistencyChecks import getFileDescendants
    return getFileDescendants( self.transID, lfns, transClient=self.transClient,
                               dm=self.dm, bkClient=self.bkClient )

  # @timeThis
  def getRAWAncestorsForRun( self, runID, param=None, paramValue=None, getFiles=False ):
    """
    Determine from BK how many ancestors files from a given run we have.
    This is used for deciding when to flush a run (when all RAW files have been processed)
    """
    ancestorFiles = set()
    # The transformation files cannot be cached globally as they evolve at each cycle
    lfns = self.transRunFiles.get( runID, [] )
    if not lfns:
      res = self.getTransformationFiles( runID=runID )
      if not res['OK']:
        self.logError( "Cannot get transformation files for run %s: %s" % ( str( runID ), res['Message'] ) )
        return 0
      lfns = [fileDict['LFN'] for fileDict in res['Value'] if fileDict['Status'] not in self.excludedStatuses]
      self.transRunFiles[runID] = lfns
      self.logVerbose( 'Obtained %d input files for run %d' % ( len( lfns ), runID ) )

    if not lfns:
      return 0

    # Restrict to files with the required parameter
    if param:
      paramValues = self.getFilesParam( lfns, param )
      lfns = [f for f, v in paramValues.iteritems() if v == paramValue]

    if lfns:
      lfnToCheck = lfns[0]
    else:
      return 0
    #
    # If files are RAW files, no need to get the number of ancestors!
    #
    if self.usingRAWFiles is None:
      self.usingRAWFiles = self.getMetadataFromTSorBK( lfnToCheck, 'FileType' )[lfnToCheck] == 'RAW'
    if self.usingRAWFiles:
      return len( lfns ) if not getFiles else lfns
    #
    # Get number of ancestors for known files
    cachedLfns = self.cachedLFNAncestors.get( runID, {} )
    # If we cached a number, clean the cache
    if cachedLfns and True in set( isinstance( val, ( long, int ) ) for val in cachedLfns.itervalues() ):
      cachedLfns = {}
    setLfns = set( lfns )
    hitLfns = setLfns & set( cachedLfns )
    if hitLfns and not getFiles:
      self.logVerbose( "Ancestors cache hit for run %d: %d files cached" % \
                       ( runID, len( hitLfns ) ) )
      for lfn in hitLfns:
        ancestorFiles.update( cachedLfns[lfn] )
      lfns = setLfns - hitLfns

    # If some files are unknown, get the ancestors from BK
    if lfns:
      res = self.getFileAncestors( list( lfns ) )
      if res['OK']:
        ancestorDict = res['Value']['Successful']
      else:
        self.logError( "Error getting ancestors: %s" % res['Message'] )
        ancestorDict = {}
      for lfn in ancestorDict:
        ancFiles = set( os.path.basename( f['FileName'] ) for f in ancestorDict[lfn] if f['FileType'] == 'RAW' )
        ancestorFiles.update( ancFiles )
        self.cachedLFNAncestors.setdefault( runID, {} )[lfn] = ancFiles

    if getFiles:
      return ancestorFiles
    notProcessed = self.__getNotProcessedAncestors( runID, lfnToCheck )
    ancestors = len( ancestorFiles )
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
        res = self.getFileAncestors( lfnToCheck, replica=False )
        if res['OK']:
          fullDst = [f['FileName'] for f in res['Value']['Successful'].get( lfnToCheck, [{}] ) if f.get( 'FileType' ) in self.__recoType]
          if fullDst:
            ancestorFullDST = fullDst[0]
        else:
          self.logError( "Error getting ancestors of %s" % lfnToCheck, res['Message'] )
    if ancestorFullDST:
      self.logDebug( "Ancestor %s found: %s" % ( self.__recoType, ancestorFullDST ) )
      res = self.bkClient.getJobInfo( ancestorFullDST )
      if res['OK']:
        try:
          recoProduction = res['Value'][0][18]
          self.logVerbose( 'Reconstruction production is %d' % recoProduction )
        except Exception as e:
          self.logException( "Exception extracting reco production from %s" % str( res['Value'] ), lException=e )
          recoProduction = None
      else:
        self.logError( "Error getting job information", res['Message'] )
    else:
      self.logVerbose( "No ancestor %s file found" % self.__recoType )
    if recoProduction:
      res = self.transClient.getTransformationFiles( { 'TransformationID':recoProduction, 'RunNumber':runID} )
      if res['OK']:
        notProcessed = len( [fileDict for fileDict in res['Value'] if fileDict['Status'] == 'NotProcessed'] )
    self.notProcessed[runID] = notProcessed
    return notProcessed

  # @timeThis
  def getTransformationFiles( self, runID ):
    return self.transClient.getTransformationFiles( { 'TransformationID' : self.transID, 'RunNumber': runID } )

  # @timeThis
  def getFileAncestors( self, lfns, depth=10, replica=True ):
    return self.bkClient.getFileAncestors( lfns, depth=depth, replica=replica )

  def checkAncestorsAtSE( self, taskLfns, seList ):
    """
    Check if ancestors of a list of files are present at a set of SEs

    :param list taskLfns: list of lfn lists for all tasks
    :param list seList: list of SEs to consider

    :return int missingAtSEs: number of files that are not at the specified SEs
    """
    # We request also that ancestors are at the fromSEs
    lfns = [lfn for lfnList in taskLfns for lfn in lfnList]
    nbLfns = len( lfns )
    ancestors = self.getFileAncestors( lfns, depth=1, replica=True )
    if not ancestors['OK']:
      self.logError( "Error getting ancestors", ancestors['Message'] )
      return _clearTaskLFNs( taskLfns )
    ancestors = ancestors['Value']['Successful']
    ancLfns = [anc['FileName'] for ancList in ancestors.itervalues() for anc in ancList]
    self.logVerbose( 'Checking ancestors presence at %s for %d files' % ( ','.join( sorted( seList ) ),
                                                                          len( ancLfns ) ) )
    res = self.dm.getReplicasForJobs( ancLfns, getUrl=False )
    if not res['OK']:
      self.logError( "Error getting replicas of ancestors", res['Message'] )
      # Fake no replicas for all files
      return _clearTaskLFNs( taskLfns )
    else:
      success = res['Value']['Successful']
    for lfn, ancList in ancestors.iteritems():
      for anc in ancList:
        if not seList & set( success[anc['FileName']] ):
          # Remove the LFN from the list of lists
          for lfnList in taskLfns:
            if lfn in lfnList:
              lfnList.remove( lfn )
    missingAtSEs = nbLfns - len( [lfn for lfnList in taskLfns for lfn in lfnList] )
    if missingAtSEs:
      self.logVerbose( "%d ancestor files found not to be at %s" % ( missingAtSEs, ','.join( sorted( seList ) ) ) )
    return missingAtSEs

  # @timeThis
  def getTransformationRuns( self, runs=None, transID=None ):
    """
    get the run table for a list of runs, if missing, add them

    :param (list,tuple,dict,set,int,long): iterable with run numbers or single run number
    :param int transID: transformation ID. If not speficied, use the current ID

    :return S_OK / S_ERROR: value is the list of run dictionaries
    """
    if transID is None:
      transID = self.transID
    condDict = {'TransformationID':transID}
    if isinstance( runs, ( dict, list, tuple, set ) ):
      condDict['RunNumber'] = list( runs )
    elif isinstance( runs, ( int, long ) ):
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
        self.logInfo( 'Add missing runs in transformation runs table: %s' % ','.join( str( run ) for run in sorted( missingRuns ) ) )
        for runID in missingRuns:
          res = self.transClient.insertTransformationRun( transID, runID, '' )
          if not res['OK']:
            return res
        result = self.transClient.getTransformationRuns( condDict )
    return result

  # @timeThis
  def getFilesGroupedByRunAndParam( self, lfns=None, param='' ):
    """
    Group files by run and another BK parameter (e.g. file type or event type)
    """
    runDict = {}
    # no need to query the BK as we have the answer from files
    if lfns is None:
      files = self.transFiles
    else:
      files = [ fileDict for fileDict in self.transFiles if fileDict['LFN'] in lfns]
    self.logVerbose( "Starting getFilesGroupedByRunAndParam for %d files, %s" % ( len( files ), 'by %s' % param if param else 'no param' ) )
    runGroups = groupByRun( files )
    for runNumber, runLFNs in runGroups.iteritems():
      if not param:
        runDict[runNumber] = {None:runLFNs}
      else:
        runDict[runNumber] = {}
        resDict = self.getFilesParam( runLFNs, param )
        for lfn, paramValue in resDict.iteritems():
          runDict[runNumber].setdefault( paramValue, [] ).append( lfn )

    # If necessary fix files with run number 0
    zeroRunDict = runDict.pop( 0, None )
    if zeroRunDict:
      nZero = 0
      for paramValue, zeroRun in zeroRunDict.iteritems():
        newRuns = self.setRunForFiles( zeroRun )
        for newRun, runLFNs in newRuns.iteritems():
          runDict.setdefault( newRun, {} ).setdefault( paramValue, [] ).extend( runLFNs )
          nZero += len( runLFNs )
      self.logInfo( "Set run number for %d files with run #0, which means it was not set yet" % nZero )

    if param:
      self.logVerbose( "Grouped %d files by run and %s" % ( len( files ), param ) )
    return runDict

  def getFilesGroupedByRun( self, lfns=None ):
    """
    Get files per run in a dictionary and set the run number if not done
    """
    # Split the files in run groups
    if lfns is None:
      files = self.transFiles
    else:
      files = [ fileDict for fileDict in self.transFiles if fileDict['LFN'] in lfns]
    runGroups = groupByRun( files )
    if not runGroups:
      # No files, no tasks!
      self.logVerbose( "No runs found!" )
      return []

    # If some files don't have a run number, get it and set it
    zeroRun = runGroups.pop( 0, None )
    if zeroRun:
      nZero = 0
      newRuns = self.setRunForFiles( zeroRun )
      for newRun, runLFNs in newRuns.iteritems():
        runGroups.setdefault( newRun, [] ).extend( runLFNs )
        nZero += len( runLFNs )
      self.logInfo( "Set run number for %d files with run #0, which means it was not set yet" % nZero )
    self.logVerbose( 'Obtained %d runs' % len( runGroups ) )
    return runGroups

  def createTasks( self, storageElementGroups, chunkSize=None ):
    """
    Create reasonable size tasks
    """
    tasks = []
    if not chunkSize:
      chunkSize = self.getPluginParam( 'MaxFilesPerTask', 100 )
    for stringTargetSEs in sorted( storageElementGroups ):
      stringTargetLFNs = storageElementGroups[stringTargetSEs]
      for lfnGroup in breakListIntoChunks( sorted( stringTargetLFNs ), chunkSize ):
        tasks.append( ( stringTargetSEs, lfnGroup ) )
    self.logVerbose( "%d tasks created" % len( tasks ) )
    return tasks

  def readCacheFile( self, workDirectory ):
    """
    Utility function
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
        mkDir( cacheFile )
      cacheFile = os.path.join( cacheFile, "Transformation_%s.pkl" % ( str( self.transID ) ) )
      if not self.cacheFile:
        self.cacheFile = cacheFile
      try:
        with open( cacheFile, 'r' ) as cacheFile_o:
          self.cachedLFNAncestors = pickle.load( cacheFile_o )
          # Do not cache between cycles, only cache temporarily, but keep same structure in file, i.e. fake load
          _cachedNbRAWFiles = pickle.load( cacheFile_o )
          _cachedLFNSize = pickle.load( cacheFile_o )
          self.cachedRunLfns = pickle.load( cacheFile_o )
          self.cachedProductions = pickle.load( cacheFile_o )
          self.cachedLastRun = pickle.load( cacheFile_o )
          self.cachedLFNProcessedPath = pickle.load( cacheFile_o )
          self.lastCall = pickle.load( cacheFile_o )
        self.logVerbose( "Cache file %s successfully loaded" % cacheFile )
        # print '*****'
        # print '\n'.join( ['%s %s' % ( key, val ) for key, val in self.cachedLFNProcessedPath.items()] )
        break
      except IOError:
        self.logVerbose( "Cache file %s could not be loaded" % cacheFile )

  def getCachedRunLFNs( self, runID, paramValue ):
    """
    Keep track of all files for a given parameter value
    """
    return set( self.cachedRunLfns.get( runID, {} ).get( paramValue, [] ) )

  def setCachedRunLfns( self, runID, paramValue, lfnList ):
    """
    Cache the list of LFNs for a given parameter value
    """
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

  # @timeThis
  def getNbRAWInRun( self, runID, evtType ):
    """
    Get the number of RAW files in a run
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
      if rawFiles:
        self.logVerbose( "Run %d has %d RAW files" % ( runID, rawFiles ) )
      else:
        self.logVerbose( "Run %d is not finished yet" % runID )
    return rawFiles


  def writeCacheFile( self ):
    """
    Utility function
    """
    import pickle
    if self.cacheFile:
      try:
        with open( self.cacheFile, 'w' ) as cacheFile_o:
          pickle.dump( self.cachedLFNAncestors, cacheFile_o )
          pickle.dump( self.cachedNbRAWFiles, cacheFile_o )
          pickle.dump( self.cachedLFNSize, cacheFile_o )
          pickle.dump( self.cachedRunLfns, cacheFile_o )
          pickle.dump( self.cachedProductions, cacheFile_o )
          pickle.dump( self.cachedLastRun, cacheFile_o )
          pickle.dump( self.cachedLFNProcessedPath, cacheFile_o )
          pickle.dump( self.lastCall, cacheFile_o )
        self.logVerbose( "Cache file %s successfully written" % self.cacheFile )
      except Exception as e:
        self.logException( "Could not write cache file", self.cacheFile, lException=e )

  def setRunForFiles( self, lfns ):
    """
    For files that are missing the run number, set it from BK
    """
    res = self.bkClient.getFileMetadata( lfns )
    runFiles = {}
    if res['OK']:
      for lfn, metadata in res['Value']['Successful'].iteritems():
        runFiles.setdefault( metadata['RunNumber'], [] ).append( lfn )
      for run in sorted( runFiles ):
        if not run:
          self.logInfo( "%d files found with void run '%s': \n%s" % ( len( runFiles[run] ), str( run ), '\n'.join( runFiles[run] ) ) )
          runFiles.pop( run )
          continue
        res = self.transClient.addTransformationRunFiles( self.transID, run, runFiles[run] )
        # print run, runFiles[run], res
        if not res['OK']:
          self.logError( "Error setting files to run", " - %d files to run %d: %s" % ( len( runFiles[run] ), run, res['Message'] ) )
          runFiles.pop( run )
    else:
      self.logError( "Error getting metadata for %d files" % len( lfns ), res['Message'] )
    return runFiles

  def getTransQuery( self, transReplicas ):
    """
    Get BK query for the current transformation
    """
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

  def cleanFiles( self, transFiles, transReplicas, status=None ):
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
    """
    Get the site destination for a set of runs and cache this information in memory
    """
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
    """
    for a given run, get the information on destination SE from within a list of SEs
    """
    self.__getRunDestinations( [runID] )
    site = self.runDestinations.get( runID )
    if site:
      self.logVerbose( 'Destination found for run %d: %s' % ( runID, site ) )
      # Avoid spurious warning if no match found
      savedLevel = gLogger.getLevel()
      gLogger.setLevel( 'ERROR' )
      res = self.dmsHelper.getSEInGroupAtSite( targets, site )
      gLogger.setLevel( savedLevel )
      return res.get( 'Value' )
    return None

  def selectProcessedFiles( self, lfns, prodList ):
    """
    Select only the files that are Processed in a list of productions
    """
    res = self.transClient.getTransformationFiles( {'LFN':list( lfns ), 'TransformationID':prodList, 'Status':'Processed'} )
    if not res['OK']:
      self.logError( "Error getting transformation files", res['Message'] )
      return []
    processedLfns = set( fDict['LFN'] for fDict in res['Value'] )
    if len( processedLfns ) != len( lfns ):
      self.logVerbose( 'Reduce files from %d to %d (removing pending files)' % ( len( lfns ), len( processedLfns ) ) )
    return processedLfns

  def checkForDescendants( self, lfns, prodList, depth=0 ):
    """
    check the files if they have an existing descendant in the list of productions
    """
    excludeTypes = ( 'LOG', 'BRUNELHIST', 'DAVINCIHIST', 'GAUSSHIST', 'HIST', 'INDEX.ROOT' )
    finalResult = set()
    if isinstance( lfns, basestring ):
      lfns = set( [lfns] )
    elif isinstance( lfns, list ):
      lfns = set( lfns )
    if depth:
      lfns = self.selectProcessedFiles( lfns, prodList )
    # Get daughters of processed files
    res = self.bkClient.getFileDescendants( list( lfns ), depth=1, checkreplica=False )
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
    for desc in descToCheck.keys():  # pylint: disable=consider-iterating-dictionary
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
      self.logVerbose( "Found %d files with descendants (out of %d) in production %d at depth %d" %
                       ( len( foundProds[prod] ), len( lfns ), prod, depth + 1 ) )

    # Check descendants in reverse order of productions
    for prod in sorted( prodDesc, reverse=True ):
      # Check descendants whose parent was not yet found processed
      toCheckInProd = set()
      for desc in prodDesc[prod]:
        if not descToCheck[desc] & finalResult:
          toCheckInProd.add( desc )
      if toCheckInProd:
        res = self.checkForDescendants( toCheckInProd, prodList, depth=depth + 1 )
        if not res['OK']:
          return res
        foundInProd = set()
        for desc in res['Value']:
          foundInProd.update( descToCheck[desc] )
        if foundInProd:
          self.logVerbose( "Found descendants of %d files at depth %d" % ( len( foundInProd ), depth + 2 ) )
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
      dirName = os.path.dirname( lfn )
      last = os.path.basename( dirName )
      if len( last ) == 4 and last.isdigit():
        dirName = os.path.dirname( dirName )
      directories.setdefault( dirName, lfn )
      lfnDirs[lfn] = dirName

    dirList = [dirName for dirName in directories if dirName not in self.cachedDirMetadata]
    for dirName in dirList:
      res = self.bkClient.getJobInfo( directories[dirName] )
      if not res['OK']:
        return res
      self.cachedDirMetadata.update( {dirName : res['Value'][0][18]} )

    lfnProd = dict( ( lfn, self.cachedDirMetadata.get( lfnDirs[lfn], 0 ) ) for lfn in lfns )
    return S_OK( lfnProd )

  def __getStorageUsage( self, dirList, seList ):
    """
    Get from StorageUsage the actual number of files for a list of directories at a list of SEs
    """
    suClient = RPCClient( 'DataManagement/StorageUsage' )
    result = {}
    if isinstance( seList, ( dict, set, tuple ) ):
      seList = list( seList )
    self.logVerbose( "Get storage usage for directories", ','.join( dirList ) )
    for dirName in dirList:
      res = suClient.getStorageSummary( dirName, None, None, seList )
      if not res['OK']:
        return res
      for se, stat in res['Value'].iteritems():
        result[se] = result.setdefault( se, 0 ) + stat['Files']
    return S_OK( result )

  def getMaxFilesAtSE( self, targetFilesAtDestination, directories, destSEs ):
    """
    Get the number of files already present at SEs for a list of directories
    Using the processing and RAW distribution shares, split the maximum number of files to be staged on these SEs
    and return the number of files that can possibly be added
    """
    dirList = set()
    for dirName in directories:
      # We strip off any numerical directory as we don't care about which productions created the files
      # If not we may have only a few input files from a single production and then we would find there is almost nothing prestaged
      dirList.add( _stripNumDirectory( dirName ) )
    # Add possibility to throttle the frequency of the plugin, but not clear if this is useful
    period = self.getPluginParam( 'Period', 0 )
    now = datetime.datetime.utcnow()
    if self.lastCall and ( now - self.lastCall ) < datetime.timedelta( hours=period ):
      self.logInfo( "Skip this loop (less than %s hours since last call)" % period )
      return S_OK( None )
    self.lastCall = now
    self.writeCacheFile()

    # Get current stprage usage per SE
    storageUsage = self.__getStorageUsage( dirList, destSEs )
    if not storageUsage['OK']:
      return storageUsage
    storageUsage = storageUsage['Value']
    self.printShares( "Current storage usage per SE:", storageUsage, counters=[], log=self.logInfo )

    # Get the shares (including processing fraction) at each site (from RAW SEs)
    shares = self.getPluginShares( section='CPUforRAW', backupSE='CERN-RAW' )
    if not shares['OK']:
      return shares
    self.shareMetrics = None
    shares = shares['Value'][1]
    self.printShares( "Shares per RAW SE:", shares, counters=[], log=self.logVerbose )

    # Get the number of files already assigned for each SE, as it should be substracted from possible number
    res = self.transClient.getTransformationFiles( {'TransformationID':self.transID, 'Status':'Assigned'} )
    if not res['OK']:
      return res
    recentFiles = dict.fromkeys( destSEs, 0 )
    for fileDict in res['Value']:
      dirName = _stripNumDirectory( os.path.dirname( fileDict['LFN'] ) )
      usedSE = fileDict['UsedSE']
      if dirName in dirList and usedSE in destSEs:
        recentFiles[usedSE] += 1

    # Get the number of files Processed in the last 12 hours
    res = self.transClient.getTransformationFiles( {'TransformationID':self.transID, 'Status':'Processed'} )
    if not res['OK']:
      return res
    limitTime = datetime.datetime.now() - datetime.timedelta( hours=12 )
    for fileDict in res['Value']:
      dirName = _stripNumDirectory( os.path.dirname( fileDict['LFN'] ) )
      usedSE = fileDict['UsedSE']
      if dirName in dirList and usedSE in destSEs and fileDict['LastUpdate'] > limitTime:
        recentFiles[usedSE] += 1
    self.printShares( "Number of files Assigned or recently Processed", recentFiles, counters=[], log=self.logVerbose )

    # Share targetFilesAtDestination on the SEs taking into account current usage
    maxFilesAtSE = {}
    for rawSE, share in shares.iteritems():
      selectedSEs = self.closerSEs( [rawSE], destSEs, local=True )
      if len( selectedSEs ):
        share *= targetFilesAtDestination / 100.
        selectedSE = selectedSEs[0]
        maxFilesAtSE[selectedSE] = max( 0, int( share - storageUsage.get( selectedSE, 0 ) - recentFiles.get( selectedSE, 0 ) ) )
    self.printShares( "Maximum number of files per SE:", maxFilesAtSE, counters=[], log=self.logInfo )
    return S_OK( maxFilesAtSE )

  def _getFileSize( self, lfns ):
    """
    Overwrite the DIRAC method and get the file size from the TS tables
    """
    return S_OK( self.getMetadataFromTSorBK( lfns, 'FileSize' ) )

  def checkCondDBRunTick( self, runID ):
    """
    Check for the presence of the run tick in the ConDB
    """
    if not self.onlineCondDB:
      self.onlineCondDB = self.getPluginParam( "OnlineCondDB", "/cvmfs/lhcb.cern.ch/lib/lhcb/git-conddb/ONLINE.git" )

    if not os.path.exists( self.onlineCondDB ):
      self.logWarn( "OnlineCondDB not found", self.onlineCondDB )
      return None
    cmd = "git --git-dir %s  grep -q '^%d$' HEAD -- Conditions/Online/valid_runs.txt" % ( self.onlineCondDB, int( runID ) )
    retCode = os.system( cmd )
    return not bool( retCode )

#=================================================================
# Set of utility functions used by LHCbDirac transformation system
#=================================================================

def getRemovalPlugins():
  """
  Returns list of removal plugins
  """
  return ( "DestroyDataset", 'DestroyDatasetWhenProcessed' ,
           'RemoveReplicasKeepDestination', "ReduceReplicasKeepDestination",
           "RemoveDataset", "RemoveReplicas", 'RemoveReplicasWhenProcessed',
           'RemoveReplicasWithAncestors', 'ReduceReplicas', )
def getReplicationPlugins():
  """
  Returns list of replication plugins
  """
  return ( "LHCbDSTBroadcast", "LHCbMCDSTBroadcastRandom",
           "ArchiveDataset", "ReplicateDataset",
           'RAWReplication', "ReplicateToRunDestination",
           'FakeReplication', 'ReplicateToLocalSE', 'ReplicateWithAncestors',
           'Healing', )

def getShares( sType, normalise=False ):
  """
  Get the shares from the Operations section of the CS
  If not found, look in the Resources section
  """
  optionPath = 'Shares/%s' % sType
  res = Operations().getOptionsDict( optionPath )
  if not res['OK']:
    res = gConfig.getOptionsDict( os.path.join( '/Resources', optionPath ) )
  if not res['OK']:
    return res
  if not res['Value']:
    return S_ERROR( "/Resources/%s option contains no shares" % optionPath )
  shares = res['Value']
  for site, value in shares.iteritems():
    shares[site] = float( value )
  if normalise:
    shares = normaliseShares( shares )
  if not shares:
    return S_ERROR( "No non-zero shares defined" )
  return S_OK( shares )


def normaliseShares( shares ):
  """
  Normalise to 1 the sum of shares

  :param shares: shares per SE or site
  :type shares: dict
  """
  total = 0.0
  normShares = shares.copy()
  for site in shares:
    total += float( shares[site] )
  if not total:
    return shares
  for site in shares:
    normShares[site] = 100.0 * ( float( shares[site] ) / total )
  return normShares

# @timeThis
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
  return runDict

def addFilesToTransformation( transID, lfns, addRunInfo=True ):
  """
  Add files to a transformation, including the run number if required
  As this is only used in the plugin to add ancestors, no need to add other metadata
  """
  transClient = TransformationClient()
  bk = BookkeepingClient()
  gLogger.info( "Adding %d files to transformation %s" % ( len( lfns ), transID ) )
  res = transClient.getTransformation( transID )
  if not res['OK']:
    return res
  transPlugin = res['Value']['Plugin']
  pluginsWithNoRunInfo = Operations().getValue( 'TransformationPlugins/PluginsWithNoRunInfo', [] )
  addRunInfo = addRunInfo and transPlugin not in pluginsWithNoRunInfo
  addedLfns = set()
  for lfnChunk in breakListIntoChunks( lfns, 1000 ):
    runDict = {}
    if addRunInfo:
      res = bk.getFileMetadata( lfnChunk )
      if res['OK']:
        resMeta = res['Value']['Successful']
        for lfn, metadata in resMeta.iteritems():
          runID = metadata.get( 'RunNumber' )
          if runID:
            runDict.setdefault( int( runID ), set() ).add( lfn )
      else:
        break
    errorLogged = False
    while True:
      res = transClient.addFilesToTransformation( transID, lfnChunk )
      if not res['OK']:
        if not errorLogged:
          errorLogged = True
          gLogger.error( "Error adding %d files to transformation, retry..." % len( lfnChunk ), res['Message'] )
        time.sleep( 1 )
      else:
        break
    added = set( lfn for ( lfn, status ) in res['Value']['Successful'].iteritems() if status == 'Added' )
    addedLfns.update( added )
    if addRunInfo and res['OK']:
      gLogger.info( "Add information for %d runs to transformation %s" % ( len( runDict ), transID ) )
      for runID, runLfns in runDict.iteritems():
        runLfns &= added
        if runLfns:
          res = transClient.addTransformationRunFiles( transID, runID, list( runLfns ) )
          if not res['OK']:
            break

  if not res['OK']:
    return res
  gLogger.info( "%d files successfully added to transformation" % len( addedLfns ) )
  return S_OK( addedLfns )
