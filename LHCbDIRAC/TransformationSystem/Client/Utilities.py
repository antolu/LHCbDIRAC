"""
  Utilities for scripts dealing with transformations
"""

__RCSID__ = "$Id$"

import DIRAC
from DIRAC.Core.Base import Script
from DIRAC import gConfig, gLogger, S_OK, S_ERROR
from LHCbDIRAC.DataManagementSystem.Client.DMScript import DMScript, convertSEs
import os, time

class Setter:
  def __init__( self, obj, name ):
    self.name = name
    self.obj = obj
  def setOption( self, val ):
    if val:
      val = val.split( ',' )
    else:
      val = []
    self.obj.options[self.name] = val

    return DIRAC.S_OK()

class PluginScript( DMScript ):
  def __init__( self ):
    DMScript.__init__( self )

  def registerPluginSwitches( self ):
    self.registerBKSwitches()
    self.setSEs = {}
    parameterSEs = ( "KeepSEs", "Archive1SEs", "Archive2SEs",
                     "MandatorySEs", "SecondarySEs", "DestinationSEs", "FromSEs" )

    Script.registerSwitch( "", "Plugin=",
                           "   Plugin name (mandatory)", self.setPlugin )
    Script.registerSwitch( "t:", "Type=",
                           "   Transformation type [Replication] (Removal automatic)", self.setTransType )
    Script.registerSwitch( "", "NumberOfReplicas=",
                           "   Number of copies to create or to remove", self.setReplicas )
    for param in parameterSEs:
      self.setSEs[param] = Setter( self, param )
      Script.registerSwitch( "", param + '=',
                             "   List of SEs for the corresponding parameter of the plugin", self.setSEs[param].setOption )
    Script.registerSwitch( "g:", "GroupSize=",
                           "   GroupSize parameter for merging (GB) or nb of files" , self.setGroupSize )
    Script.registerSwitch( "", "Parameters=",
                           "   Additional plugin parameters ({<key>:<val>,[<key>:val>]}", self.setParameters )
    Script.registerSwitch( "", "RequestID=",
                           "   Sets the request ID (default 0)", self.setRequestID )
    Script.registerSwitch( "", "ProcessingPasses=",
                           "   List of processing passes for the DeleteReplicasWhenProcessed plugin", self.setProcessingPasses )
    Script.registerSwitch( "", "Period=",
                           "   minimal period at which a plugin is executed (if instrumented)", self.setPeriod )
    Script.registerSwitch( "", "CleanTransformations",
                           "   (only for DestroyDataset) clean transformations from the files being destroyed", self.setCleanTransformations )
    Script.registerSwitch( '', 'Debug', '   Sets a debug flag in the plugin', self.setDebug )

  def setPlugin( self, val ):
    self.options['Plugin'] = val
    return DIRAC.S_OK()

  def setTransType( self, val ):
    self.options['Type'] = val
    return DIRAC.S_OK()

  def setReplicas ( self, val ):
    self.options['NumberOfReplicas'] = val
    return DIRAC.S_OK()

  def setGroupSize ( self, groupSize ):
    try:
      if float( int( groupSize ) ) == float( groupSize ):
        groupSize = int( groupSize )
      else:
        groupSize = float( groupSize )
      self.options['GroupSize'] = groupSize
    except:
      pass
    return DIRAC.S_OK()

  def setParameters ( self, val ):
    self.options['Parameters'] = val
    return DIRAC.S_OK()

  def setRequestID ( self, val ):
    self.options['RequestID'] = val
    return DIRAC.S_OK()

  def setProcessingPasses( self, val ):
    self.options['ProcessingPasses'] = val.split( ',' )
    return DIRAC.S_OK()

  def setPeriod( self, val ):
    self.options['Period'] = val
    return DIRAC.S_OK()

  def setCleanTransformations( self, val ):
    self.options['CleanTransformations'] = True
    return DIRAC.S_OK()

  def setDebug( self, val ):
    self.options['Debug'] = True
    return DIRAC.S_OK()

  def getPluginParameters( self ):
    if 'Parameters' in self.options:
      params = eval( self.options['Parameters'] )
    else:
      params = {}
    pluginParams = ( 'NumberOfReplicas', 'GroupSize', 'ProcessingPasses', 'Period', 'CleanTransformations', 'Debug' )
    #print self.options
    for key in [k for k in self.options if k in pluginParams]:
      params[key] = self.options[key]
    for key in [k for k in self.options if k.endswith( 'SE' ) or k.endswith( 'SEs' )]:
      params[key] = convertSEs( self.options[key] )
    return params

from DIRAC.Core.Utilities.List import breakListIntoChunks, randomize
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.Core.Utilities.SiteSEMapping import getSitesForSE

class PluginUtilities:
  """
  Utility class used by plugins
  """
  def __init__( self, plugin, transClient, replicaManager, bkClient, rmClient, resourceStatus, debug, transInThread ):
    self.plugin = plugin
    self.transClient = transClient
    self.bkClient = bkClient
    self.rm = replicaManager
    self.rmClient = rmClient
    self.resourceStatus = resourceStatus
    self.freeSpace = {}
    self.debug = debug
    self.transID = None
    self.params = {}
    self.seConfig = {}
    self.cachedLFNAncestors = {}
    self.cachedLFNSize = {}
    self.cachedNbRAWFiles = {}
    self.cachedRunLfns = {}
    self.cachedProductions = {}
    self.cacheFile = ''
    self.filesParam = {}
    self.transRunFiles = {}
    self.groupSize = 0
    self.maxFiles = 0
    self.transInThread = transInThread
    self.transString = ''

  def logVerbose( self, message, param='' ):
    if self.debug:
      gLogger.info( '(V)' + self.transString + message, param )
    else:
      gLogger.verbose( self.transString + message, param )

  def logDebug( self, message, param='' ):
    gLogger.debug( self.transString + message, param )

  def logInfo( self, message, param='' ):
    gLogger.info( self.transString + message, param )

  def logWarn( self, message, param='' ):
    gLogger.warn( self.transString + message, param )

  def logError( self, message, param='' ):
    gLogger.error( self.transString + message, param )

  def logException( self, message, param='', lException=False ):
    gLogger.exception( self.transString + message, param, lException )

  def setParameters( self, params ):
    self.params = params
    self.transID = params['TransformationID']
    self.transString = self.transInThread.get( self.transID, ' [NoThread] [%d] ' % self.transID ) + '%s: ' % self.plugin

  def setDebug( self, val ):
    self.debug = val

  def getPluginParam( self, name, default=None ):
    """ Get plugin parameters using specific settings or settings defined in the CS
        Caution: the type returned is that of the default value
    """
    # get the value of a parameter looking 1st in the CS
    if default != None:
      valueType = type( default )
    else:
      valueType = None
    # First look at a generic value...
    optionPath = "TransformationPlugins/%s" % ( name )
    value = Operations().getValue( optionPath, None )
    self.logVerbose( "Default plugin param %s: '%s'" % ( optionPath, value ) )
    # Then look at a plugin-specific value
    optionPath = "TransformationPlugins/%s/%s" % ( self.plugin, name )
    value = Operations().getValue( optionPath, value )
    self.logVerbose( "Specific plugin param %s: '%s'" % ( optionPath, value ) )
    if value != None:
      default = value
    # Finally look at a transformation-specific parameter
    value = self.params.get( name, default )
    self.logVerbose( "Transformation plugin param %s: '%s'" % ( name, value ) )
    if valueType and type( value ) != valueType:
      if valueType == type( [] ):
        value = getListFromString( value )
      elif valueType == type( 0 ):
        value = int( value )
      elif valueType == type( 0. ):
        value = float( value )
      elif valueType == type( True ):
        if value in ( 'False', 'No' ):
          value = False
        else:
          value = bool( value )
      elif valueType != type( '' ):
        self.logWarn( "Unknown parameter type (%s) for %s, passed as string" % ( str( valueType ), name ) )
    self.logVerbose( "Final plugin param %s: '%s'" % ( name, value ) )
    return value

  def getCPUShares( self , transID=None, backupSE=None ):
    return self.getShares( transID=transID, backupSE=backupSE )

  def getShares( self, section=None, transID=None, backupSE=None ):
    if not transID:
      transID = self.transID
    if not section:
      sharesSections = { 'DataReconstruction': 'CPUforRAW', 'DataReprocessing' : 'CPUforReprocessing'}
      res = self.transClient.getTransformation( transID )
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
    if backupSE:
      rawFraction = res['Value']
      targetSEs = sorted( rawFraction )
      result = getShares( 'RAW', normalise=True )
      if result['OK']:
        rawShares = result['Value']
        tier1Fraction = 0.
        shares = {}
        for se in [se for se in rawShares if se in rawFraction]:
          shares[se] = rawShares[se] * rawFraction[se]
          tier1Fraction += shares[se]
        shares[backupSE] = 100. - tier1Fraction
      else:
        rawShares = None
      self.logInfo( "Fraction of RAW (%s) to be processed at each SE (%%):" % section )
      for se in targetSEs:
        self.logInfo( "%s: %.1f" % ( se.ljust( 15 ), 100. * rawFraction[se] ) )
    else:
      shares = normaliseShares( res['Value'] )
      rawFraction = None
      self.logInfo( "Obtained the following target distribution shares (%):" )
      for se in sorted( shares ):
        self.logInfo( "%s: %.1f" % ( se.ljust( 15 ), shares[se] ) )

    # Get the existing destinations from the transformationDB, just for printing
    res = self.getExistingCounters( transID, requestedSEs=sorted( shares ) )
    if not res['OK']:
      self.logError( "Failed to get used share", res['Message'] )
      return res
    else:
      existingCount = res['Value']
      normalisedExistingCount = normaliseShares( existingCount ) if existingCount else {}
      self.logInfo( "Target shares and usage for production (%):" )
      for se in sorted( shares ):
        infoStr = "%s: %4.1f |" % ( se.ljust( 15 ), shares[se] )
        if se in normalisedExistingCount:
          infoStr += " %4.1f" % normalisedExistingCount[se]
        self.logInfo( infoStr )
    if rawFraction:
      return S_OK( ( rawFraction, shares ) )
    else:
      return S_OK( ( existingCount, shares ) )

  def getExistingCounters( self, transID, normalise=False, requestedSEs=None ):
    """
    Used by RAWShares and AtomicRun, gets what has been done up to now while distributing runs
    """

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
      seDict = {}
      for se, count in usageDict.items():
        if se in requestedSEs:
          seDict[se] = seDict.setdefault( se, 0 ) + count
        else:
          self.logWarn( "%s is in counters but not in required list" % se )
      usageDict = seDict.copy()
    if normalise:
      usageDict = normaliseShares( usageDict )
    return S_OK( usageDict )

  def getBookkeepingMetadata( self, lfns, param ):
    filesParam = {}
    for chunk in breakListIntoChunks( lfns, 1000 ):
      res = self.bkClient.getFileMetadata( chunk )
      if res['OK']:
        for lfn, metadata in res['Value'].items():
          filesParam[lfn] = metadata.get( param )
          if lfn not in self.cachedLFNSize:
            self.cachedLFNSize[lfn] = metadata.get( 'FileSize' )
      else:
        return res
    return S_OK( filesParam )

  def getFilesParam( self, lfns, param ):
    start = time.time()
    if not self.filesParam:
      nCached = 0
      for run in self.cachedRunLfns:
        for paramValue in self.cachedRunLfns[run]:
          for lfn in self.cachedRunLfns[run][paramValue]:
            self.filesParam[lfn] = paramValue
            nCached += 1
      self.logVerbose( 'Found %d files cached for param %s' % ( nCached, param ) )
    filesParam = dict( zip( lfns, [self.filesParam.get( lfn ) for lfn in lfns] ) )
    newLFNs = [lfn for lfn in lfns if not filesParam[lfn]]
    if newLFNs:
      res = self.getBookkeepingMetadata( newLFNs, param )
      if res['OK']:
        for lfn in res['Value']:
          filesParam[lfn] = res['Value'][lfn]
          self.filesParam[lfn] = filesParam[lfn]
      else:
        return res
      self.logVerbose( "Obtained BK %s of %d files in %.3f seconds" % ( param, len( newLFNs ), time.time() - start ) )
    return S_OK( filesParam )

  def getActiveSEs( self, selist ):
    """ Utility function - uses RSS
    """
    activeSE = []

    try:
      res = self.resourceStatus.getStorageElementStatus( selist, statusType='Write', default='Unknown' )
      if res[ 'OK' ]:
        for k, v in res[ 'Value' ].items():
          if v.get( 'Write' ) in [ 'Active', 'Bad' ]:
            activeSE.append( k )
      else:
        self.logError( "Error getting active SEs from RSS for %s" % str( selist ), res['Message'] )
    except:
      for se in selist:
        res = gConfig.getOption( '/Resources/StorageElements/%s/WriteAccess' % se, 'Unknown' )
        if res['OK'] and res['Value'] == 'Active':
          activeSE.append( se )

    return activeSE
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
    from DIRAC.Resources.Storage.StorageElement import StorageElement
    if se in self.freeSpace:
      return self.freeSpace[se]['freeSpace']
    # get the site and space token, for the time being short cut ;-)
    res = StorageElement( se ).getStorageParameters( 'SRM2' )
    if res['OK']:
      params = res['Value']
      token = params['SpaceToken']
      if token == 'LHCb-Tape':
        freeSpace = 1000.
        self.freeSpace[se] = {'freeSpace' : freeSpace}
        return freeSpace
      res = getSiteForSE( se )
      if res['OK'] or not res['Value']:
        site = res['Value'].split( '.' )[1]
    if not res['OK'] or not site:
      self.logError( 'Unable to determine site or space token for SE %s:' % se, res['Message'] )
      return 0

    # Check first if cached for the same site and token
    for value in self.freeSpace.values():
      if value.get( 'site' ) == site and value.get( 'token' ) == token:
        self.freeSpace[se] = {'site':site, 'token':token, 'freeSpace':value['freeSpace']}
        return self.freeSpace[se]['freeSpace']
    # if not get the information from RSS
    res = self.rmClient.getSLSStorage( site=site, token=token )
    if res['OK']:
      if len( res['Value'] ) == 0 or len( res['Value'][0] ) < 9:
        self.logError( "Incorrect return value from RSS for site %s, token %s: %s" % ( site, token, res['Value'] ) )
        return 0
      freeSpace = res['Value'][0][8]
      self.freeSpace[se] = {'site':site, 'token':token, 'freeSpace':freeSpace}
      self.logVerbose( 'Free space for SE %s, site %s, token %s: %d' % ( se, site, token, freeSpace ) )
      return freeSpace
    else:
      self.logError( 'Error when getting space for SE %s, site %s, token %s' % ( se, site, token ), res['Message'] )
      return 0

  def rankSEs( self, candSEs ):
    """ Ranks the SEs according to their free space
    """
    if len( candSEs ) <= 1:
      return candSEs
    import random
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
        for se, w in weights.items():
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

  def setTargetSEs( self, numberOfCopies, archive1SEs, archive2SEs, mandatorySEs, secondarySEs, existingSEs,
                      exclusiveSEs=False ):
    """ Decide on which SEs to target from lists and current status of replication
        Policy is max one archive1, one archive 2, all mandatory SEs and required number of copies elsewhere
    """
    # Select active SEs
    nbArchive1 = min( 1, len( archive1SEs ) )
    nbArchive2 = min( 1, len( archive2SEs ) )
    archive1ActiveSEs = self.getActiveSEs( archive1SEs )
    if not archive1ActiveSEs:
      archive1ActiveSEs = archive1SEs
    archive2ActiveSEs = self.getActiveSEs( archive2SEs )
    if not archive2ActiveSEs:
      archive2ActiveSEs = archive2SEs
    secondaryActiveSEs = self.getActiveSEs( secondarySEs )

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
      for se in [se for se in candSEs if se not in existingSEs]:
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

  def uniqueSEs( self, ses ):
    newSEs = []
    for se in ses:
      if not self.isSameSEInList( se, newSEs ):
        newSEs.append( se )
    return newSEs

  def isSameSE( self, se1, se2 ):
    if se1 == se2:
      return True
    for se in ( se1, se2 ):
      if se not in self.seConfig:
        from DIRAC.Resources.Storage.StorageElement import StorageElement
        self.seConfig[se] = {}
        res = StorageElement( se ).getStorageParameters( 'SRM2' )
        if res['OK']:
          params = res['Value']
          for item in ( 'Host', 'Path' ):
            self.seConfig[se][item] = params[item].replace( 't1d1', 't0d1' )
        else:
          self.logError( "Error getting StorageElement parameters for %s" % se, res['Message'] )

    return self.seConfig[se1] == self.seConfig[se2]

  def isSameSEInList( self, se1, seList ):
    if se1 in seList:
      return True
    for se in seList:
      if self.isSameSE( se1, se ):
        return True
    return False

  def getProcessedFiles( self, lfns ):
    """
    Check which files have been processed by a given production, i.e. have a meaningful descendant
    """
    from LHCbDIRAC.DataManagementSystem.Client.ConsistencyChecks import getFileDescendants
    return getFileDescendants( self.transID, lfns, transClient=self.transClient, rm=self.rm, bkClient=self.bkClient )

  def getRAWAncestorsForRun( self, runID, param='', paramValue='' ):
    """ Determine from BK how many ancestors files from a given runs do have
        This is used for deciding when to flush a run (when all RAW files have been processed)
    """
    startTime1 = time.time()
    ancestors = 0
    # The transformation files cannot be cached globally as they evolve at each cycle
    lfns = self.transRunFiles.get( runID, [] )
    if not lfns:
      res = self.transClient.getTransformationFiles( { 'TransformationID' : self.transID, 'RunNumber': runID } )
      self.logVerbose( "Timing for getting transformation files: %.3f s" % ( time.time() - startTime1 ) )
      if not res['OK']:
        self.logError( "Cannot get transformation files for run %s: %s" % ( str( runID ), res['Message'] ) )
        return 0
      lfns = [fileDict['LFN'] for fileDict in res['Value']]
      self.transRunFiles[runID] = lfns

    # Restrict to files with the required parameter
    if param:
      res = self.getFilesParam( lfns, param )
      if not res['OK']:
        self.logError( "Error getting BK param %s:" % param, res['Message'] )
        return 0
      paramValues = res['Value']
      lfns = [f for f in paramValues if paramValues[f] == paramValue]

    # Get number of ancestors for known files
    cachedLFNs = self.cachedLFNAncestors.get( runID, {} )
    hitLFNs = [lfn for lfn in lfns if lfn in cachedLFNs]
    if hitLFNs:
      self.logVerbose( "Ancestors cache hit for run %d: %d files cached" % \
                       ( runID, len( hitLFNs ) ) )
      for lfn in hitLFNs:
        ancestors += cachedLFNs[lfn]
      lfns = [lfn for lfn in lfns if lfn not in hitLFNs]

    # If some files are unknown, get the ancestors from BK
    if lfns:
      startTime = time.time()
      res = self.bkClient.getFileAncestors( lfns, depth=10 )
      self.logVerbose( "Timing for getting all ancestors with metadata of %d files: %.3f s" % ( len( lfns ),
                                                                                                  time.time() - startTime ) )
      if res['OK']:
        ancestorDict = res['Value']['Successful']
      else:
        self.logError( "Error getting ancestors: %s" % res['Message'] )
        ancestorDict = {}
      for lfn in ancestorDict:
        n = len( [f for f in ancestorDict[lfn] if f['FileType'] == 'RAW'] )
        self.cachedLFNAncestors.setdefault( runID, {} )[lfn] = n
        ancestors += n
    self.logVerbose( "Full timing for getRAWAncestors: %.3f seconds" % ( time.time() - startTime1 ) )
    return ancestors

  def groupByRunAndParam( self, lfns, files, param='' ):
    """ Group files by run and another BK parameter (e.g. file type or event type)
    """
    runDict = {}
    startTime = time.time()
    # no need to query the BK as we have the answer from files
    lfns = [ fileDict for fileDict in files if fileDict['LFN'] in lfns]
    self.logVerbose( "Starting groupByRunAndParam for %d files, %s" % ( len( lfns ), 'by %s' % param if param else 'no param' ) )
    res = groupByRun( lfns )
    self.logVerbose( "Grouped %d files by run in %.1f seconds" % ( len( lfns ), time.time() - startTime ) )
    runGroups = res['Value']
    for runNumber, runLFNs in runGroups.items():
      if not param:
        runDict[runNumber] = {None:runLFNs}
      else:
        runDict[runNumber] = {}
        res = self.getFilesParam( runLFNs, param )
        if not res['OK']:
          self.logError( 'Error getting %s for %d files of run %d' % ( param, len( runLFNs ), runNumber ), res['Message'] )
        else:
          for lfn, paramValue in res['Value'].items():
            runDict[runNumber].setdefault( paramValue, [] ).append( lfn )
    if param:
      self.logVerbose( "Grouped %d files by run and %s in %.1f seconds" % ( len( lfns ), param, time.time() - startTime ) )
    return S_OK( runDict )

  def groupByReplicas( self, files, status ):
    if not self.groupSize:
      self.groupSize = self.getPluginParam( 'GroupSize', 10 )
    tasks = []
    flush = ( status == 'Flush' )
    self.logVerbose( "groupByReplicas: %d files, groupSize %d, flush %s" % ( len( files ), self.groupSize, flush ) )
    nTasks = 0
    files = files.copy()
    # Consider files by groups of SEs, a file is only in one group
    # Then consider files site by site, but a file can now be at more than one site
    for groupSE in ( True, False ):
      if not files:
        break
      seFiles = getFileGroups( files, groupSE=groupSE )
      # Consider disk SEs first
      for replicaSE in sortSEs( seFiles ):
        lfns = seFiles[replicaSE]
        if lfns:
          tasksLfns = breakListIntoChunks( lfns, self.groupSize )
          lfnsInTasks = []
          for taskLfns in tasksLfns:
            if ( flush and not groupSE ) or ( len( taskLfns ) >= self.groupSize ):
              tasks.append( ( replicaSE, taskLfns ) )
              lfnsInTasks += taskLfns
          # In case the file was at more than one site, remove it from the other sites' list
          # Remove files from global list
          for lfn in lfnsInTasks:
            files.pop( lfn )
          if not groupSE:
            # Remove files from other SEs
            for se in [se for se in seFiles if se != replicaSE]:
              seFiles[se] = [lfn for lfn in seFiles[se] if lfn not in lfnsInTasks]
      self.logVerbose( "groupByReplicas: %d tasks created with groupSE %s" % ( len( tasks ) - nTasks, str( groupSE ) ) )
      self.logVerbose( "groupByReplicas: %d files have not been included in tasks" % len( files ) )
      nTasks = len( tasks )

    return S_OK( tasks )


  def groupBySize( self, files, status ):
    startTime = time.time()
    if not self.groupSize:
      self.groupSize = float( self.getPluginParam( 'GroupSize', 1 ) ) * 1000 * 1000 * 1000 # input size in GB converted to bytes
    requestedSize = self.groupSize
    flush = ( status == 'Flush' )
    self.logVerbose( "groupBySize: %d files, groupSize %d, flush %s" % ( len( files ), self.groupSize, flush ) )
    if not self.maxFiles:
      self.maxFiles = self.getPluginParam( 'MaxFiles', 100 )
    maxFiles = self.maxFiles
    # Get the file sizes
    res = self.getFileSize( files.keys() )
    if not res['OK']:
      return res
    fileSizes = res['Value']
    tasks = []
    nTasks = 0
    # Group files by SE
    files = files.copy()
    for groupSE in ( True, False ):
      if not files:
        break
      seFiles = getFileGroups( files, groupSE=groupSE )
      for replicaSE in sorted( seFiles ) if groupSE else sortSEs( seFiles ):
        lfns = seFiles[replicaSE]
        lfnsInTasks = []
        taskLfns = []
        taskSize = 0
        lfns = sorted( lfns, key=fileSizes.get )
        for lfn in lfns:
          size = fileSizes.get( lfn, 0 )
          if size:
            if size > requestedSize:
              tasks.append( ( replicaSE, [lfn] ) )
              lfnsInTasks.append( lfn )
            else:
              taskSize += size
              taskLfns.append( lfn )
              if ( taskSize > requestedSize ) or ( len( taskLfns ) >= maxFiles ):
                tasks.append( ( replicaSE, taskLfns ) )
                lfnsInTasks += taskLfns
                taskLfns = []
                taskSize = 0
        if flush and taskLfns:
          tasks.append( ( replicaSE, taskLfns ) )
          lfnsInTasks += taskLfns
        # Remove files from global list
        for lfn in lfnsInTasks:
          files.pop( lfn )
        if not groupSE:
          # Remove files from other SEs
          for se in [se for se in seFiles if se != replicaSE]:
            seFiles[se] = [lfn for lfn in seFiles[se] if lfn not in lfnsInTasks]
        # Remove the selected files from the size cache
        self.clearCachedFileSize( lfnsInTasks )
      self.logVerbose( "groupBySize: %d tasks created with groupSE %s" % ( len( tasks ) - nTasks, str( groupSE ) ) )
      self.logVerbose( "groupBySize: %d files have not been included in tasks" % len( files ) )
      nTasks = len( tasks )

    self.logVerbose( "Grouped %d files by size in %.1f seconds" % ( len( files ), time.time() - startTime ) )
    return S_OK( tasks )

  def createTasks( self, storageElementGroups, chunkSize=100 ):
    """ Create reasonable size tasks
    """
    tasks = []
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
      if type( prefixes ) == type( '' ):
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
        f.close()
        self.logVerbose( "Cache file %s successfully loaded" % cacheFile )
        break
      except:
        self.logVerbose( "Cache file %s could not be loaded" % cacheFile )

  def getCachedRunLFNs( self, runID, paramValue ):
    return self.cachedRunLfns.get( runID, {} ).get( paramValue, [] )

  def setCachedRunLfns( self, runID, paramValue, lfnList ):
    self.cachedRunLfns.setdefault( runID, {} )[paramValue] = lfnList

  def getCachedProductions( self ):
    return self.cachedProductions

  def setCachedProductions( self, productions ):
    self.cachedProductions = productions

  def getNbRAWInRun( self, runID, evtType ):
    """ Get the number of RAW files in a run
    """
    rawFiles = self.cachedNbRAWFiles.get( runID, {} ).get( evtType )
    if not rawFiles:
      startTime = time.time()
      res = self.bkClient.getNbOfRawFiles( {'RunNumber':runID, 'EventTypeId':evtType} )
      if not res['OK']:
        rawFiles = 0
        self.logError( "Cannot get number of RAW files for run %d, evttype %d" % ( runID, evtType ) )
      else:
        rawFiles = res['Value']
        self.cachedNbRAWFiles.setdefault( runID, {} )[evtType] = rawFiles
        self.logVerbose( "Run %d has %d RAW files (timing: %3f s)" % ( runID, rawFiles, time.time() - startTime ) )
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
        f.close()
        self.logVerbose( "Cache file %s successfully written" % self.cacheFile )
      except:
        self.logError( "Could not write cache file %s" % self.cacheFile )

  def getFileSize( self, lfns ):
    """ Get file size from a cache, if not from the catalog
    """
    fileSizes = {}
    startTime1 = time.time()
    for lfn in [lfn for lfn in lfns if lfn in self.cachedLFNSize]:
      fileSizes[lfn] = self.cachedLFNSize[lfn]
    if fileSizes:
      self.logVerbose( "Cache hit for File size for %d files" % len( fileSizes ) )
    lfns = [lfn for lfn in lfns if lfn not in self.cachedLFNSize]
    if lfns:
      startTime = time.time()
      res = self.rm.getCatalogFileSize( lfns )
      if not res['OK']:
        return S_ERROR( "Failed to get sizes for files" )
      if res['Value']['Failed']:
        return S_ERROR( "Failed to get sizes for all files" )
      fileSizes.update( res['Value']['Successful'] )
      self.cachedLFNSize.update( ( res['Value']['Successful'] ) )
      self.logVerbose( "Timing for getting size of %d files from catalog: %.3f seconds" % ( len( lfns ), ( time.time() - startTime ) ) )
    self.logVerbose( "Timing for getting size of files: %.3f seconds" % ( time.time() - startTime1 ) )
    return S_OK( fileSizes )

  def clearCachedFileSize( self, lfns ):
    """ Utility function
    """
    for lfn in [lfn for lfn in lfns if lfn in self.cachedLFNSize]:
      self.cachedLFNSize.pop( lfn )

  def setRunForFiles( self, lfns ):
    res = self.bkClient.getFileMetadata( lfns )
    runFiles = {}
    if res['OK']:
      for lfn, metadata in res['Value'].items():
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


#=================================================================
# Set of utility functions used by LHCbDirac transformation system
#=================================================================

def getRemovalPlugins():
  return ( "DestroyDataset", "DeleteDataset", "DeleteReplicas",
           'DeleteReplicasWhenProcessed', 'DestroyDatasetWhenProcessed' )
def getReplicationPlugins():
  return ( "LHCbDSTBroadcast", "LHCbMCDSTBroadcast", "LHCbMCDSTBroadcastRandom", "ArchiveDataset", "ReplicateDataset",
           "RAWShares", 'FakeReplication', 'ReplicateToLocalSE', 'Healing' )

def getShares( sType, normalise=False ):
  """
  Get the shares from the Resources section of the CS
  """
  from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
  optionPath = 'Shares/%s' % sType
  res = Operations().getOptionsDict( optionPath )
  if not res['OK']:
    res = gConfig.getOptionsDict( os.path.join( '/Resources', optionPath ) )
  if not res['OK']:
    return res
  if not res['Value']:
    return S_ERROR( "/Resources/Shares/%s option contains no shares" % sType )
  shares = res['Value']
  for site, value in shares.items():
    shares[site] = float( value )
  if normalise:
    shares = normaliseShares( shares )
  if not shares:
    return S_ERROR( "No non-zero shares defined" )
  return S_OK( shares )


def normaliseShares( shares ):
  total = 0.0
  for site in shares:
    total += float( shares[site] )
  if not total:
    return None
  for site in shares:
    shares[site] = 100.0 * ( float( shares[site] ) / total )
  return shares

def sortExistingSEs( lfnSEs, lfns=None ):
  """ Sort SEs according to the number of files in each (most first)
  """
  seFrequency = {}
  archiveSEs = []
  if not lfns:
    lfns = lfnSEs.keys()
  else:
    lfns = [lfn for lfn in lfns if lfn in lfnSEs]
  for lfn in lfns:
    existingSEs = lfnSEs[lfn]
    archiveSEs += [s for s in existingSEs if isArchive( s ) and s not in archiveSEs]
    for se in [s for s in existingSEs if not isFailover( s ) and s not in archiveSEs]:
      seFrequency[se] = seFrequency.setdefault( se, 0 ) + 1
  sortedSEs = seFrequency.keys()
  # sort SEs in reverse order of frequency
  sortedSEs.sort( key=seFrequency.get, reverse=True )
  # add the archive SEs at the end
  return sortedSEs + archiveSEs

def groupByRun( files ):
  """ Groups files by run
  files is a list of dictionaries containing the run number
  """
  runDict = {}
  for fileDict in files:
    runID = fileDict.get( 'RunNumber' ) if fileDict.get( 'RunNumber' ) else 0
    lfn = fileDict['LFN']
    if lfn:
      runDict.setdefault( runID, [] ).append( lfn )
  return S_OK( runDict )

def isArchive( se ):
  return se.endswith( "-ARCHIVE" )

def isFailover( se ):
  return se.endswith( "-FAILOVER" )

def getFileGroups( fileReplicas, groupSE=True ):
  """
  Group files by set of SEs
  If groupSE == False, group by SE, in which case a file can be in more than one element
  """
  fileGroups = {}
  for lfn, replicas in fileReplicas.items():
    if not replicas:
      continue
    if not groupSE or len( replicas ) == 1:
      for rep in replicas:
        fileGroups.setdefault( rep, [] ).append( lfn )
    else:
      replicaSEs = str.join( ',', sorted( replicas ) )
      fileGroups.setdefault( replicaSEs, [] ).append( lfn )
  return fileGroups

def getSiteForSE( se ):
  """ Get site name for the given SE
  """
  result = getSitesForSE( se, gridName='LCG' )
  if not result['OK']:
    return result
  if result['Value']:
    return S_OK( result['Value'][0] )
  return S_OK( '' )

seSvcClass = {}
def sortSEs( ses ):
  for se in ses:
    if len( se.split( ',' ) ) != 1:
      return sorted( ses )
    if se not in seSvcClass:
      from DIRAC.Resources.Storage.StorageElement import StorageElement
      seSvcClass[se] = StorageElement( se ).getStatus()['Value']['DiskSE']
  diskSEs = [se for se in ses if seSvcClass[se]]
  tapeSEs = [se for se in ses if se not in diskSEs]
  return sorted( diskSEs ) + sorted( tapeSEs )

def getListFromString( strParam ):
  """ Converts a string representing a list into a list
      The string may have [] or not, quotes or not around members
  """
  import types
  if type( strParam ) == types.StringType:
    if strParam == "[]" or strParam == '':
      return []
    if strParam[0] == '[' and strParam[-1] == ']':
      strParam = strParam[1:-1]
    l = strParam.split( ',' )
    ll = []
    for a in l:
      a = a.strip()
      if not a:
        ll.append( a )
      elif a[0] == "'" and a[-1] == "'":
        ll.append( a[1:-1] )
      elif a[0] == '"' and a[-1] == '"':
        ll.append( a[1:-1] )
      else:
        ll.append( a )
    return ll
  return strParam

def closerSEs( existingSEs, targetSEs, local=False ):
  """ Order the targetSEs such that the first ones are closer to existingSEs. Keep all elements in targetSEs
  """
  sameSEs = [se for se in targetSEs if se in existingSEs]
  targetSEs = [ se for se in targetSEs if se not in existingSEs]
  if targetSEs:
    # Some SEs are left, look for sites
    existingSites = [getSiteForSE( se )['Value'] for se in existingSEs if not isArchive( se )]
    closeSEs = [se for se in targetSEs if getSiteForSE( se )['Value'] in existingSites]
    otherSEs = [se for se in targetSEs if se not in closeSEs]
    targetSEs = randomize( closeSEs )
    if not local:
      targetSEs += randomize( otherSEs )
  return ( targetSEs + sameSEs ) if not local else targetSEs

