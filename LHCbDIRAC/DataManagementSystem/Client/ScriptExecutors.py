"""
Set of functions used by the DMS scripts
"""

__RCSID__ = "$Id: ScriptExecutors.py 76721 2014-07-22 08:05:22Z phicharp $"

from DIRAC                                                  import gLogger, gConfig, S_OK, S_ERROR, exit
from DIRAC.Core.Utilities.List                              import breakListIntoChunks
from DIRAC.DataManagementSystem.Client.DataManager          import DataManager
from DIRAC.Resources.Catalog.FileCatalog                    import FileCatalog
from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient   import BookkeepingClient
from LHCbDIRAC.TransformationSystem.Client.TransformationClient        import TransformationClient
from DIRAC.Resources.Storage.StorageElement                 import StorageElement
from DIRAC.Core.Base                                        import Script
from LHCbDIRAC.DataManagementSystem.Client.DMScript         import DMScript, printDMResult
import sys, os, time, random

def __checkSEs( args, expand = True ):
  if expand:
    expanded = []
    for arg in args:
      expanded += arg.split( ',' )
  else:
    expanded = args
  seList = []
  res = gConfig.getSections( '/Resources/StorageElements' )
  if res['OK']:
    for ses in list( expanded ):
      sel = [se for se in ses.split( ',' ) if se in res['Value']]
      if sel :
        seList.append( ','.join( sel ) )
        expanded.remove( ses )
  return seList, expanded

def getAllSEs():
  res = gConfig.getSections( '/Resources/StorageElements' )
  if not res['OK']:
    gLogger.fatal( 'Error getting list of SEs', res['Message'] )
    exit( 1 )
  # Archive SEs will be removed from the list later
  return [se for se in res['Value']]

def getArchiveSEs():
  return getSEs( 'Tier1-ARCHIVE' )

def getSEs( seGroup ):
  dmScript = DMScript()
  dmScript.setSEs( seGroup )
  return set( dmScript.getOption( 'SEs', [] ) )

def parseArguments( dmScript, allSEs = False ):
  # SEs passes as option arguments
  if allSEs:
    seList = getAllSEs()
  else:
    seList = dmScript.getOption( 'SEs', [] )
    sites = dmScript.getOption( 'Sites', [] )
    if sites:
      seList += getSEsForSites( sites )

  # LFNs and SEs passed as positional arguments
  args = Script.getPositionalArgs()
  args = [aa for arg in args for aa in arg.split( ',' )]
  ses, args = __checkSEs( args )
  if not allSEs:
    seList += ses
  for lfn in args:
    dmScript.setLFNsFromFile( lfn )
  lfnList = dmScript.getOption( 'LFNs', [] )

  # LFNs from BK
  if not lfnList:
    bkQuery = dmScript.getBKQuery( visible = 'All' )
    if set( bkQuery.getQueryDict() ) - set( ['Visible', 'ReplicaFlag'] ):
      gLogger.always( "Executing BKQuery:", bkQuery )
      lfnList = bkQuery.getLFNs()

  return sorted( lfnList ), seList

def getSEsForSites( sites ):
  ses = []
  for site in sites:
    res = gConfig.getOptionsDict( '/Resources/Sites/LCG/%s' % site )
    if not res['OK']:
      gLogger.error( 'Site %s not known' % site )
    else:
      ses.extend( res['Value']['SE'].replace( ' ', '' ).split( ',' ) )
  return ses

def executeRemoveReplicas( dmScript, allDisk = False ):
  checkFC = True
  force = False

  lfnList, seList = parseArguments( dmScript, allSEs = allDisk )
  if not lfnList:
    gLogger.fatal( "No LFNs have been supplied" )
    exit( 1 )
  if not allDisk:
    # Only remove from selected SEs
    minReplicas = 1
  else:
    # Remove from all SEs
    minReplicas = 0

  for switch in Script.getUnprocessedSwitches():
    if switch[0] == "n" or switch[0].lower() == "nolfc":
      checkFC = False
    elif switch[0] == 'Force':
      force = True
    elif switch[0] == 'ReduceReplicas':
      if allDisk:
        gLogger.fatal( "Option ReduceReplicas is incompatible with removing all disk replicas" )
      try:
        minReplicas = max( 1, int( switch[1] ) )
        # Set a default for Users
        if not seList:
          dmScript.setSEs( 'Tier1-USER' )
          seList = dmScript.getOption( 'SEs', [] )
      except:
        gLogger.fatal( "Invalid number of replicas:", switch[1] )
        exit( 1 )

  # This should be improved, with disk SEs first...
  if not seList:
    gLogger.fatal( "Provide SE name (list) as last argument or with --SE option" )
    Script.showHelp()
    exit( -1 )

  removeReplicas( lfnList, seList, minReplicas, checkFC, allDisk, force )

def removeReplicas( lfnList, seList, minReplicas = 1, checkFC = True, allDisk = False, force = False ):
  if not checkFC:
    res = removeReplicasNoFC( lfnList, sorted( seList ) )
    if not res['OK']:
      gLogger.fatal( "Completely failed removing replicas without FC", res['Message'] )
      exit( -1 )
    successfullyRemoved = res['Value']['Successful']
    errorReasons = res['Value']['Failed']
  else:
    res = removeReplicasWithFC( lfnList, sorted( seList ), minReplicas, allDisk, force )
    if not res['OK']:
      gLogger.fatal( "Completely failed removing replicas with FC", res['Message'] )
      exit( -1 )
    successfullyRemoved = res['Value']['Successful']
    errorReasons = res['Value']['Failed']

  # Print result
  if successfullyRemoved:
    for se in successfullyRemoved:
      nrep = len( successfullyRemoved[se] )
      if nrep:
        gLogger.always( "Successfully removed %d replicas from %s" % ( nrep, se ) )
  for reason, seDict in errorReasons.items():
    for se, lfns in seDict.items():
      gLogger.always( "Failed to remove %d replicas from %s with reason: %s" % ( len( lfns ), se, reason ) )
  if not successfullyRemoved and not errorReasons and not checkFC:
    gLogger.always( "Replicas were found at no SE in %s" % str( seList ) )
  exit( 0 )

def removeReplicasWithFC( lfnList, seList, minReplicas = 1, allDisk = False, force = False ):
  dm = DataManager()
  bk = BookkeepingClient()
  #########################
  # Normal removal using FC
  #########################
  archiveSEs = getArchiveSEs()
  errorReasons = {}
  successfullyRemoved = {}
  notExisting = {}
  gLogger.setLevel( 'FATAL' )
  seList = set( seList )
  # Set files invisible in BK if removing all disk replicas
  for lfnChunk in breakListIntoChunks( sorted( lfnList ), 500 ):
    if allDisk:
      res = bk.setFilesInvisible( lfnChunk )
      if not res['OK']:
        gLogger.error( "Error setting files invisible in BK", res['Message'] )
        exit( -3 )
    res = dm.getReplicas( lfnChunk )
    if not res['OK']:
      gLogger.fatal( "Failed to get replicas", res['Message'] )
      exit( -2 )
    if res['Value']['Failed']:
      lfnChunk = list( set( lfnChunk ) - set( res['Value']['Failed'] ) )
    seReps = {}
    filesToRemove = []
    for lfn in res['Value']['Successful']:
      rep = set( res['Value']['Successful'][lfn] )
      if force and not rep & archiveSEs:
        # There are no archives, but remove all disk replicas, i.e. removeFile
        filesToRemove.append( lfn )
        continue
      rep -= archiveSEs
      if not seList & rep:
        if allDisk:
          errorReasons.setdefault( 'Only ARCHIVE replicas', {} ).setdefault( 'anywhere', [] ).append( lfn )
          continue
        errorReasons.setdefault( 'No replicas at requested sites (%d existing)' % ( len( rep ) ), {} ).setdefault( 'anywhere', [] ).append( lfn )
      elif len( rep ) <= minReplicas:
        seString = ','.join( sorted( seList & rep ) )
        errorReasons.setdefault( 'No replicas to remove (%d existing/%d requested)' % ( len( rep ), minReplicas ), {} ).setdefault( seString, [] ).append( lfn )
      else:
        seString = ','.join( sorted( rep ) )
        seReps.setdefault( seString, [] ).append( lfn )
    if filesToRemove:
      res = dm.removeFile( filesToRemove )
      if not res['OK']:
        gLogger.fatal( "Failed to remove files", res['Message'] )
        exit( -2 )
      for lfn, reason in res['Value']['Failed'].items():
        reason = str( reason )
        if 'File does not exist' not in reason:
          errorReasons.setdefault( str( reason ), {} ).setdefault( 'AllSEs', [] ).append( lfn )
      successfullyRemoved.setdefault( 'AllSEs', [] ).extend( res['Value']['Successful'].keys() )
    for seString in seReps:
      ses = set( seString.split( ',' ) )
      lfns = seReps[seString]
      removeSEs = list( seList & ses )
      remaining = len( ses - seList )
      if remaining < minReplicas:
        # Not enough replicas outside seList, remove only part, otherwisae remove all
        removeSEs = random.shuffle( removeSEs )[0:remaining - minReplicas]
      for seName in sorted( removeSEs ):
        res = dm.removeReplica( seName, lfns )
        if not res['OK']:
          gLogger.fatal( "Failed to remove replica", res['Message'] )
          exit( -2 )
        for lfn, reason in res['Value']['Failed'].items():
          reason = str( reason )
          if 'No such file or directory' in reason:
            notExisting.setdefault( lfn, [] ).append( seName )
          else:
            errorReasons.setdefault( reason, {} ).setdefault( seName, [] ).append( lfn )
        successfullyRemoved.setdefault( seName, [] ).extend( res['Value']['Successful'].keys() )

  gLogger.setLevel( 'ERROR' )
  if notExisting:
    res = dm.getReplicas( notExisting.keys() )
    if not res['OK']:
      gLogger.error( "Error getting replicas of %d non-existing files" % len( notExisting ), res['Message'] )
      errorReasons.setdefault( str( res['Message'] ), {} ).setdefault( 'getReplicas', [] ).extend( notExisting.keys() )
    else:
      for lfn, reason in res['Value']['Failed'].items():
        errorReasons.setdefault( str( reason ), {} ).setdefault( None, [] ).append( lfn )
        notExisting.pop( lfn, None )
      replicas = res['Value']['Successful']
      for lfn in notExisting:
        for se in [se for se in notExisting[lfn] if se in replicas.get( lfn, [] )]:
          res = FileCatalog().removeReplica( {lfn:{'SE':se, 'PFN':replicas[lfn][se]}} )
          if not res['OK']:
            gLogger.error( 'Error removing replica in the FC for a non-existing replica', res['Message'] )
            errorReasons.setdefault( str( res['Message'] ), {} ).setdefault( se, [] ).append( lfn )
          else:
            for lfn, reason in res['Value']['Failed'].items():
              errorReasons.setdefault( str( reason ), {} ).setdefault( se, [] ).append( lfn )
              notExisting.pop( lfn, None )
      if notExisting:
        for lfn in notExisting:
          for se in notExisting[lfn]:
            successfullyRemoved.setdefault( se, [] ).append( lfn )
        gLogger.always( "Removed from FC %d non-existing files" % len( notExisting ) )
  return S_OK( {'Successful': successfullyRemoved, 'Failed': errorReasons} )


def removeReplicasNoFC( lfnList, seList ):
  dm = DataManager()
  bk = BookkeepingClient()
  ##################################
  # Try and remove PFNs if not in FC
  ##################################
  gLogger.always( 'Removing %d physical replica from %s, for replicas not in the FC' \
    % ( len( lfnList ), str( seList ) ) )
  # Remove the replica flag in BK just in case
  errorReasons = {}
  successfullyRemoved = {}
  gLogger.verbose( 'Removing replica flag in BK' )
  notInFC = []
  notInBK = {}
  bkOK = 0
  chunkSize = 500
  showProgress = len( lfnList ) > 3 * chunkSize
  if showProgress:
    sys.stdout.write( 'Removing replica flag in BK for files not in FC (chunks of %d files): ' % chunkSize )
  for lfnChunk in breakListIntoChunks( lfnList, chunkSize ):
    if showProgress:
      sys.stdout.write( '.' )
      sys.stdout.flush()
    res = dm.getReplicas( lfnChunk )
    if res['OK'] and res['Value']['Failed']:
      bkToRemove = res['Value']['Failed'].keys()
      notInFC += bkToRemove
      res = bk.removeFiles( bkToRemove )
      if not res['OK']:
        if res['Message']:
          gLogger.error( "Error removing replica flag in BK for %d files" % len( bkToRemove ), res['Message'] )
        else:
          notInBK.setdefault( 'File is not in BK', [] ).extend( bkToRemove )
      else:
        bkFailed = res['Value'].get( 'Failed', res['Value'] )
        if type( bkFailed ) == type( {} ):
          for lfn, reason in bkFailed.items():
            notInBK.setdefault( str( reason ), [] ).append( lfn )
        elif type( bkFailed ) == type( [] ):
          notInBK.setdefault( 'Not in BK', [] ).extend( bkFailed )
        bkOK += len( bkToRemove ) - len( bkFailed )
  if showProgress:
    gLogger.always( '' )
  for reason, lfns in notInBK.items():
    gLogger.always( "Failed to remove replica flag in BK for %d files with error: %s" % ( len( lfns ), reason ) )
  if bkOK:
    gLogger.always( "Replica flag was removed in BK for %d files" % bkOK )
  notInFC = set( notInFC )
  for seName in seList:
    se = StorageElement( seName )
    if showProgress:
      sys.stdout.write( 'Checking at %s (chunks of %d replicas): ' % ( seName, chunkSize ) )
    for lfnChunk in breakListIntoChunks( lfnList, chunkSize ):
      if showProgress:
        sys.stdout.write( '.' )
        sys.stdout.flush()
      else:
        gLogger.verbose( 'Check if replicas are registered in FC at %s..' % seName )
      lfnChunk = set( lfnChunk )
      lfnsToRemove = list( lfnChunk & notInFC )
      toCheck = list( lfnChunk - notInFC )
      if toCheck:
        gLogger.setLevel( 'FATAL' )
        res = dm.getReplicaIsFile( toCheck, seName )
        gLogger.setLevel( 'ERROR' )
        if not res['OK']:
          lfnsToRemove += toCheck
        else:
          if res['Value']['Successful']:
            gLogger.always( "%d replicas are in the FC, they will not be removed from %s" \
              % ( len( res['Value']['Successful'] ), seName ) )
          lfnsToRemove += res['Value']['Failed'].keys()
      if not lfnsToRemove:
        continue
      pfnsToRemove = {}
      gLogger.verbose( '%d replicas NOT registered in FC: removing physical file at %s.. Get the PFNs' \
        % ( len( lfnsToRemove ), seName ) )
      for lfn in lfnsToRemove:
        res = se.getPfnForLfn( lfn )
        if not res['OK'] or lfn not in res['Value']['Successful']:
          reason = res.get( 'Message', res.get( 'Value', {} ).get( 'Failed', {} ).get( lfn ) )
          errorReasons.setdefault( str( reason ), {} ).setdefault( seName, [] ).append( lfn )
          gLogger.error( 'ERROR getting LFN: %s' % lfn, res['Message'] )
        else:
          pfn = res['Value']['Successful'].get( lfn )
          if pfn:
            pfnsToRemove[pfn] = lfn
          else:
            reason = res['Value']['Failed'].get( lfn, 'Unknown error' )
            errorReasons.setdefault( str( reason ), {} ).setdefault( seName, [] ).append( lfn )
      if not pfnsToRemove:
        continue
      gLogger.verbose( "Removing surls: %s" % '\n'.join( pfnsToRemove ) )
      res = se.exists( pfnsToRemove.keys() )
      if not res['OK']:
        gLogger.error( 'ERROR checking file:', res['Message'] )
        continue
      pfns = [pfn for pfn, exists in res['Value']['Successful'].items() if exists]
      if not pfns:
        continue
      gLogger.setLevel( 'FATAL' )
      res = se.removeFile( pfns )
      gLogger.setLevel( 'ERROR' )
      if not res['OK']:
        gLogger.error( 'ERROR removing storage file: ', res['Message'] )
      else:
        gLogger.verbose( "StorageElement.removeFile returned: ", res )
        failed = res['Value']['Failed']
        for pfn, reason in failed.items():
          lfn = pfnsToRemove[pfn]
          if 'No such file or directory' in str( reason ):
            successfullyRemoved.setdefault( seName, [] ).append( lfn )
          else:
            errorReasons.setdefault( str( reason ), {} ).setdefault( seName, [] ).append( lfn )
        successfullyRemoved.setdefault( seName, [] ).extend( [pfnsToRemove[pfn] for pfn in res['Value']['Successful']] )
    if showProgress:
      gLogger.always( '' )
  return S_OK( {'Successful': successfullyRemoved, 'Failed': errorReasons} )

def executeAccessURL( dmScript ):
  """
  Actual script executor
  """
  protocol = None
  for switch in Script.getUnprocessedSwitches():
    if switch[0] == 'Protocol':
      protocol = switch[1].lower()
  lfnList, seList = parseArguments( dmScript )
  if not lfnList:
    gLogger.always( "No list of LFNs provided" )
    Script.showHelp()
  else:
    getAccessURL( lfnList, seList, protocol )
  exit( 0 )

def getAccessURL( lfnList, seList, protocol = None ):
  dm = DataManager()
  res = dm.getActiveReplicas( lfnList )
  replicas = res.get( 'Value', {} ).get( 'Successful', {} )
  if not seList:
    seList = sorted( set( [se for lfn in lfnList for se in replicas.get( lfn, {} )] ) )
    if len( seList ) > 1:
      gLogger.always( "Using the following list of SEs: %s" % str( seList ) )
  bk = BookkeepingClient()
  # gLogger.setLevel( "FATAL" )
  notFoundLfns = set( lfnList )
  results = {'OK':True, 'Value':{'Successful':{}, 'Failed':{}}}
  level = gLogger.getLevel()
  gLogger.setLevel( 'FATAL' )
  # Check if files are MDF
  bkRes = bk.getFileTypeVersion( lfnList )
  mdfFiles = set( [lfn for lfn, fileType in bkRes.get( 'Value', {} ).items() if fileType == 'MDF'] )
  for se in seList:
    lfns = [lfn for lfn in lfnList if se in replicas.get( lfn, [] )]
    if lfns:
      res = StorageElement( se ).getAccessUrl( lfns, protocol = protocol )
      success = res.get( 'Value', {} ).get( 'Successful' )
      if res['OK'] and success:
        for lfn in set( success ) & mdfFiles:
          success[lfn] = 'mdf:' + success[lfn]
        notFoundLfns -= set( success )
        results['Value']['Successful'].setdefault( se, {} ).update( success )
  gLogger.setLevel( level )

  if notFoundLfns:
    results['Value']['Failed'] = dict.fromkeys( sorted( notFoundLfns ), 'File not found in required SEs' )

  printDMResult( results, empty = "File not at SE", script = "dirac-dms-lfn-accessURL" )

def executeRemoveFiles( dmScript ):

  lfnList, _ses = parseArguments( dmScript )
  setProcessed = False
  switches = Script.getUnprocessedSwitches()
  for switch in switches:
    if switch[0] == 'SetProcessed':
      setProcessed = True

  removeFiles( lfnList, setProcessed )
  exit( 0 )

def removeFiles( lfnList, setProcessed = False ):
  dm = DataManager()
  fc = FileCatalog()
  transClient = TransformationClient()

  errorReasons = {}
  successfullyRemoved = []
  notExisting = []
  # Avoid spurious error messages
  gLogger.setLevel( 'FATAL' )
  chunkSize = 100
  verbose = len( lfnList ) >= 5 * chunkSize
  if verbose:
    sys.stdout.write( "Removing %d files (chunks of %d) " % ( len( lfnList ), chunkSize ) )
  for lfnChunk in breakListIntoChunks( lfnList, chunkSize ):
    if verbose:
      sys.stdout.write( '.' )
      sys.stdout.flush()
    res = dm.removeFile( lfnChunk, force = False )
    if not res['OK']:
      gLogger.fatal( "Failed to remove data", res['Message'] )
      exit( -2 )
    for lfn, reason in res['Value']['Failed'].items():
      reasonStr = str( reason )
      if type( reason ) == type( {} ) and reason == {'BookkeepingDB': 'File does not exist'}:
        pass
      elif 'No such file or directory' in reasonStr or 'File does not exist' in reasonStr:
        notExisting.append( lfn )
      else:
        errorReasons.setdefault( reasonStr, [] ).append( lfn )
    successfullyRemoved += res['Value']['Successful'].keys()
  if verbose:
    gLogger.always( '' )
  gLogger.setLevel( 'ERROR' )

  if successfullyRemoved + notExisting:
    res = transClient.getTransformationFiles( {'LFN':successfullyRemoved + notExisting } )
    if not res['OK']:
      gLogger.error( "Error getting transformation files", res['Message'] )
    else:
      transFiles = res['Value']
      lfnsToSet = {}
      if setProcessed:
        ignoreStatus = ( 'Removed' )
      else:
        ignoreStatus = ( 'Processed', 'Removed' )
        ignoredFiles = {}
        for fileDict in [fileDict for fileDict in transFiles if fileDict['Status'] == 'Processed']:
          ignoredFiles.setdefault( fileDict['TransformationID'], [] ).append( fileDict['LFN'] )
        if ignoredFiles:
          for transID, lfns in ignoredFiles.items():
            gLogger.always( 'Ignored %d files in status Processed in transformation %d' % ( len( lfns ), transID ) )

      for fileDict in [tf for tf in transFiles if tf['Status'] not in ignoreStatus]:
        lfnsToSet.setdefault( fileDict['TransformationID'], [] ).append( fileDict['LFN'] )
      # If required, set files Removed in transformations
      for transID, lfns in lfnsToSet.items():
        res = transClient.setFileStatusForTransformation( transID, 'Removed', lfns, force = True )
        if not res['OK']:
          gLogger.error( 'Error setting %d files to Removed' % len( lfns ), res['Message'] )
        else:
          gLogger.always( 'Successfully set %d files as Removed in transformation %d' % ( len( lfns ), transID ) )

  if notExisting:
    # The files are not yet removed from the catalog!! :(((
    if verbose:
      sys.stdout.write( "Removing %d non-existing files from FC (chunks of %d) " % ( len( notExisting ), chunkSize ) )
    notExistingRemoved = []
    for lfnChunk in breakListIntoChunks( notExisting, chunkSize ):
      if verbose:
        sys.stdout.write( '.' )
        sys.stdout.flush()
      res = dm.getReplicas( lfnChunk )
      if not res['OK']:
        gLogger.error( "Error getting replicas of %d non-existing files" % len( lfnChunk ), res['Message'] )
        errorReasons.setdefault( str( res['Message'] ), [] ).extend( lfnChunk )
      else:
        replicas = res['Value']['Successful']
        for lfn in replicas:
          for se in replicas[lfn]:
            res = fc.removeReplica( {lfn:{'SE':se, 'PFN':replicas[lfn][se]}} )
            if not res['OK']:
              gLogger.error( 'Error removing replica in the FC for a non-existing file', res['Message'] )
              errorReasons.setdefault( str( res['Message'] ), [] ).append( lfn )
            else:
              for lfn, reason in res['Value']['Failed'].items():
                errorReasons.setdefault( str( reason ), [] ).append( lfn )
                lfnChunk.remove( lfn )
        if lfnChunk:
          res = fc.removeFile( lfnChunk )
          if not res['OK']:
            gLogger.error( "Error removing %d non-existing files from the FC" % len( lfnChunk ), res['Message'] )
            errorReasons.setdefault( str( res['Message'] ), [] ).extend( lfnChunk )
          else:
            for lfn, reason in res['Value']['Failed'].items():
              errorReasons.setdefault( str( reason ), [] ).append( lfn )
              lfnChunk.remove( lfn )
        notExistingRemoved += lfnChunk
    if verbose:
      gLogger.always( '' )
    if notExistingRemoved:
      successfullyRemoved += notExistingRemoved
      gLogger.always( "Removed from FC %d non-existing files" % len( notExistingRemoved ) )

  if successfullyRemoved:
    gLogger.always( "Successfully removed %d files" % len( successfullyRemoved ) )
  for reason, lfns in errorReasons.items():
    gLogger.always( "Failed to remove %d files with error: %s" % ( len( lfns ), reason ) )

def executeLfnReplicas( dmScript ):

  lfnList, _ses = parseArguments( dmScript )

  active = True
  switches = Script.getUnprocessedSwitches()
  for switch in switches:
    if switch[0] in ( "a", "All" ):
      active = False

  if not lfnList:
    gLogger.fatal( "No LFNs supplies" )
    Script.showHelp()
    exit( 1 )
  printLfnReplicas( lfnList, active )

def printLfnReplicas( lfnList, active = True ):
  dm = DataManager()
  fc = FileCatalog()
  while True:
    res = dm.getActiveReplicas( lfnList ) if active else dm.getReplicas( lfnList )
    if not res['OK']:
      break
    if active and not res['Value']['Successful'] and not res['Value']['Failed']:
      active = False
    else:
      break
  if res['OK']:
    if active:
      res = dm.checkActiveReplicas( res['Value'] )
      value = res['Value']
    else:
      replicas = res['Value']['Successful']
      value = {'Failed': res['Value']['Failed'], 'Successful' : {}}
      for lfn in sorted( replicas ):
        for se in sorted( replicas[lfn] ):
          res = fc.getReplicaStatus( {lfn:se} )
          if not res['OK']:
            value['Failed'][lfn] = "Can't get replica status"
          else:
            value['Successful'].setdefault( lfn, {} )[se] = "(%s) %s" % ( res['Value']['Successful'][lfn], replicas[lfn][se] )
      res = S_OK( value )
  # exit( printDMResult( dirac.getReplicas( lfnList, active=active, printOutput=False ),
  #                           empty="No allowed SE found", script="dirac-dms-lfn-replicas" ) )
  exit( printDMResult( res,
                             empty = "No allowed replica found", script = "dirac-dms-lfn-replicas" ) )

def executePfnMetadata( dmScript ):

  lfnList, seList = parseArguments( dmScript )

  check = False
  exists = False
  summary = False
  for opt, val in Script.getUnprocessedSwitches():
    if opt == 'Check':
      check = True
    elif opt == 'Exists':
      exists = True
      check = True
    elif opt == 'Summary':
      summary = True
      check = True
      exists = True

  if not lfnList:
    Script.showHelp()
    exit( 1 )
  printPfnMetadata( lfnList, seList, check, exists, summary )
  exit( 0 )

def printPfnMetadata( lfnList, seList, check = False, exists = False, summary = False ):
  from DIRAC.Core.Utilities.Adler import compareAdler
  if len( seList ) > 1:
    gLogger.always( "Using the following list of SEs: %s" % str( seList ) )
  if len( lfnList ) > 100:
    gLogger.always( "Getting metadata for %d files, be patient" % len( lfnList ) )

  fc = FileCatalog()

  gLogger.setLevel( "FATAL" )
  metadata = {'Successful':{}, 'Failed':{}}
  replicas = {}
  # restrict SEs to those where the replicas are
  for lfnChunk in breakListIntoChunks( lfnList, 100 ):
    res = fc.getReplicas( lfnChunk, allStatus = True )
    if not res['OK']:
      gLogger.fatal( 'Error getting replicas for %d files' % len( lfnChunk ), res['Message'] )
      exit( 2 )
    else:
      replicas.update( res['Value']['Successful'] )
    for lfn in res['Value']['Failed']:
      metadata['Failed'][lfn] = 'FC: ' + res['Value']['Failed'][lfn]
  for lfn in sorted( replicas ):
    if seList and not [se for se in replicas[lfn] if se in seList]:
      metadata['Failed'][lfn] = 'No such file at %s in FC' % ' '.join( seList )
      replicas.pop( lfn )
      lfnList.remove( lfn )
  metadata['Failed'].update( dict.fromkeys( [url for url in lfnList if url not in replicas and url not in metadata['Failed']], 'FC: No active replicas' ) )
  result = None
  if not seList:
    # take all SEs in replicas and add a fake '' to printout the SE name
    seList = [''] + sorted( list( set( [se for lfn in replicas for se in replicas[lfn]] ) ) )
  if replicas:
    for se in seList:
      fileList = [url for url in lfnList if se in replicas.get( url, [] )]
      if not fileList:
        continue
      oSe = StorageElement( se )
      for fileChunk in breakListIntoChunks( fileList, 100 ):
        res = oSe.getFileMetadata( fileChunk )
        if res['OK']:
          seMetadata = res['Value']
          for url in seMetadata['Successful']:
            pfnMetadata = seMetadata['Successful'][url].copy()
            metadata['Successful'].setdefault( url, {} )[se] = pfnMetadata if not exists else {'Exists': 'True (%sCached)' % ( '' if pfnMetadata.get( 'Cached' ) else 'Not ' )}
            if exists and not pfnMetadata.get( 'Size' ):
              metadata['Successful'][url][se].update( {'Exists':'Zero size'} )
            if check:
              res1 = fc.getFileMetadata( url )
              if res1['OK']:
                lfnMetadata = res1['Value']['Successful'][url]
                ok = True
                diff = 'False -'
                for field in ( 'Checksum', 'Size' ):
                  if lfnMetadata[field] != pfnMetadata[field]:
                    if field == 'Checksum' and compareAdler( lfnMetadata[field], pfnMetadata[field] ):
                      continue
                    ok = False
                    diff += ' %s: (LFN %s, PFN %s)' % ( field, lfnMetadata[field], pfnMetadata[field] )
                if len( seList ) > 1:
                  metadata['Successful'][url][se]['MatchLFN'] = ok if ok else diff
                else:
                  metadata['Successful'][url]['MatchLFN'] = ok if ok else diff
          for url in seMetadata['Failed']:
            metadata['Failed'].setdefault( url, {} )[se] = seMetadata['Failed'][url] if not exists else {'Exists': False}
        else:
          for url in fileChunk:
            metadata['Failed'][url] = res['Message'] + ' at %s' % se

  if not summary:
    printDMResult( S_OK( metadata ), empty = "File not at SE" )
  else:
    nFiles = 0
    counterKeys = ['Not in FC', 'No active replicas', 'Not existing', 'Exists', 'Checksum OK', 'Checksum bad']
    counters = dict.fromkeys( counterKeys, 0 )
    for lfn, reason in metadata['Failed'].items():
      nFiles += 1
      if type( reason ) == type( '' ):
        if reason == 'FC: No active replicas':
          counters['No active replicas'] += 1
        elif reason.startswith( 'FC:' ):
          counters['Not in FC'] += 1
        else:
          counters['Not existing'] += 1
      elif type( reason ) == type( {} ):
        for se in reason:
          if reason[se]['Exists']:
            counters['Exists'] += 1
          else:
            counters['Not existing'] += 1
    for lfn, seDict in metadata['Successful'].items():
      nFiles += 1
      for se in seDict:
        if seDict[se]['MatchLFN'] == True:
          counters['Checksum OK'] += 1
        else:
          counters['Checksum bad'] += 1
    gLogger.always( 'For %d files:' % nFiles )
    for key in counterKeys:
      if counters[key]:
        gLogger.always( '%s: %d' % ( key.rjust( 20 ), counters[key] ) )

  exit( 0 )

def orderSEs( listSEs ):
  listSEs = sorted( listSEs )
  orderedSEs = [se for se in listSEs if se.endswith( "-ARCHIVE" )]
  orderedSEs += [se for se in listSEs if not se.endswith( "-ARCHIVE" )]
  return orderedSEs

def executeReplicaStats( dmScript ):
  getSize = False
  prNoReplicas = False
  prWithArchives = False
  prWithReplicas = False
  for switch in Script.getUnprocessedSwitches():
    if switch[0] in ( "S", "Size" ):
      getSize = True
    elif switch[0] == 'DumpNoReplicas':
      prNoReplicas = True
    elif switch[0] == 'DumpWithArchives':
      prWithArchives = [int( xx ) for xx in switch[1].split( ',' )]
    elif switch[0] == 'DumpWithReplicas':
      prWithReplicas = [int( xx ) for xx in switch[1].split( ',' )]


  directories = dmScript.getOption( 'Directory' )
  if not directories:
    lfnList, _ses = parseArguments( dmScript )

  printReplicaStats( directories, lfnList, getSize, prNoReplicas, prWithReplicas, prWithArchives )
  exit( 0 )

def printReplicaStats( directories, lfnList, getSize = False, prNoReplicas = False, prWithReplicas = False, prWithArchives = False ):
  from DIRAC.Core.Utilities.SiteSEMapping                        import getSitesForSE
  dm = DataManager()

  repStats = {}
  noReplicas = {}
  withReplicas = {}
  withArchives = {}
  lfnReplicas = {}
  if directories:
    for directory in directories:
      res = dm.getReplicasFromDirectory( directory )
      if not res['OK']:
        gLogger.error( res['Message'] )
        continue
      lfnReplicas.update( res['Value'] )
  elif lfnList:
    res = dm.getReplicas( lfnList )
    if not res['OK']:
      gLogger.fatal( res['Message'] )
      exit( 2 )
    lfnReplicas = res['Value']['Successful']
    if res['Value']['Failed']:
      repStats[0] = len( res['Value']['Failed'] )
      withReplicas[0] = res['Value']['Failed'].keys()
      for lfn in res['Value']['Failed']:
        noReplicas[lfn] = -1

  if not lfnReplicas:
    gLogger.fatal( "No files found that have a replica...." )
    return

  if repStats.get( 0 ):
    gLogger.always( "%d files found without a replica" % repStats[0] )

  repSEs = {}
  repSites = {}
  maxRep = 0
  maxArch = 0
  nfiles = 0
  totSize = 0
  if getSize:
    lfnSize = {}
    left = len( lfnReplicas )
    for lfns in breakListIntoChunks( lfnReplicas.keys(), 500 ):
      left -= len( lfns )
      sys.stdout.write( "... getting size for %d LFNs (%d left), be patient...    \r" % ( len( lfns ), left ) )
      sys.stdout.flush()
      r = FileCatalog().getFileSize( lfns )
      if r['OK']:
        lfnSize.update( r['Value']['Successful'] )
    for lfn, size in lfnSize.items():
      totSize += size
  for lfn, replicas in lfnReplicas.items():
    SEs = replicas.keys()
    nrep = len( replicas )
    narchive = -1
    for se in list( SEs ):
      if se.endswith( "-FAILOVER" ):
        nrep -= 1
        repStats[-100] = repStats.setdefault( -100, 0 ) + 1
        if nrep == 0:
          repStats[-101] = repStats.setdefault( -101, 0 ) + 1
        SEs.remove( se )
      if se.endswith( "-ARCHIVE" ):
        nrep -= 1
        narchive -= 1
    repStats[nrep] = repStats.setdefault( nrep, 0 ) + 1
    withReplicas.setdefault( nrep, [] ).append( lfn )
    withArchives.setdefault( -narchive - 1, [] ).append( lfn )
    if nrep == 0:
      noReplicas[lfn] = -narchive - 1
    # narchive is negative ;-)
    repStats[narchive] = repStats.setdefault( narchive, 0 ) + 1
    for se in replicas:
      if se not in repSEs:
        repSEs[se] = [0, 0]
      repSEs[se][0] += 1
      if getSize:
        repSEs[se][1] += lfnSize[lfn]

    maxRep = max( maxRep, nrep )
    maxArch = max( maxArch, -narchive )
    nfiles += 1

  gigaByte = 1000. * 1000. * 1000.
  teraByte = gigaByte * 1000.
  if directories:
    dirStr = " in %s" % str( directories )
  else:
    dirStr = " with replicas"
  if totSize:
    gLogger.always( "%d files found (%.3f gigaByte)%s" % ( nfiles, totSize / gigaByte, dirStr ) )
  else:
    gLogger.always( "%d files found%s" % ( nfiles, dirStr ) )
  gLogger.always( "\nReplica statistics:" )
  if -100 in repStats:
    gLogger.always( "Failover replicas: %d files" % repStats[-100] )
    if -101 in repStats:
      gLogger.always( "   ...of which %d are only in Failover" % repStats[-101] )
    else:
      gLogger.always( "   ...but all of them are also somewhere else" )
  if maxArch:
    for nrep in range( 1, maxArch + 1 ):
      gLogger.always( "%d archives: %d files" % ( nrep - 1, repStats.setdefault( -nrep, 0 ) ) )
  for nrep in range( maxRep + 1 ):
    gLogger.always( "%d replicas: %d files" % ( nrep, repStats.setdefault( nrep, 0 ) ) )

  gLogger.always( "\nSE statistics:" )
  for se in orderSEs( repSEs.keys() ):
    if se.endswith( "-FAILOVER" ):
      continue
    if not se.endswith( "-ARCHIVE" ):
      res = getSitesForSE( se, gridName = 'LCG' )
      if res['OK']:
        try:
          site = res['Value'][0]
        except:
          continue
        if site not in repSites:
          repSites[site] = [0, 0]
        repSites[site][0] += repSEs[se][0]
        repSites[site][1] += repSEs[se][1]
    string = "%16s: %s files" % ( se, repSEs[se][0] )
    if getSize:
      string += " - %.3f teraByte" % ( repSEs[se][1] / teraByte )
    gLogger.always( string )

  gLogger.always( "\nSites statistics:" )
  for site in sorted( repSites.keys() ):
    string = "%16s: %d files" % ( site, repSites[site][0] )
    if getSize:
      string += " - %.3f teraByte" % ( repSites[site][1] / teraByte )
    gLogger.always( string )

  if prNoReplicas and noReplicas:
    gLogger.always( "\nFiles without a disk replica:" )
    for rep in sorted( noReplicas ):
      gLogger.always( "%s (%d archives)" % ( rep, noReplicas[rep] ) )

  if type( prWithArchives ) == type( [] ):
    for n in [m for m in prWithArchives if m in withArchives]:
      gLogger.always( '\nFiles with %d archives:' % n )
      for rep in sorted( withArchives[n] ):
        gLogger.always( rep )

  if type( prWithReplicas ) == type( [] ):
    for n in [m for m in prWithReplicas if m in withReplicas]:
      gLogger.always( '\nFiles with %d disk replicas:' % n )
      for rep in sorted( withReplicas[n] ):
        gLogger.always( rep )
  exit( 0 )

def executeReplicateLfn( dmScript ):
  seList, args = __checkSEs( Script.getPositionalArgs(), expand = False )
  destList = []
  sourceSE = []
  localCache = ''
  try:
    destList = __checkSEs( seList[0].split( ',' ), expand = False )[0]
    sourceSE = seList[1].split( ',' )
  except:
    pass
  # gLogger.always( seList, destList, sourceSE
  if not destList or len( sourceSE ) > 1:
    gLogger.always( "No destination SE" if not destList else "More than one source SE" )
    Script.showHelp()
  if sourceSE:
    sourceSE = sourceSE[0]

  if args:
    if os.path.isdir( args[-1] ):
      localCache = args.pop()

  for lfn in args:
    dmScript.setLFNsFromFile( lfn )
  lfnList = dmScript.getOption( 'LFNs', [] )
  if not lfnList:
    gLogger.always( "No LFNs provided..." )
    Script.showHelp()

  replicateLfn( lfnList, sourceSE, destList, localCache )

def replicateLfn( lfnList, sourceSE, destList, localCache = None ):
  dm = DataManager()
  # print lfnList, destList, sourceSE, localCache
  finalResult = {'OK':True, 'Value':{"Failed":{}, "Successful":{}}}
  for lfn in lfnList:
    for seName in destList:
      result = dm.replicateAndRegister( lfn, seName, sourceSE, localCache = localCache )
      if not result['OK']:
        finalResult['Value']["Failed"].setdefault( seName, {} ).update( {lfn:result['Message']} )
      else:
        success = result['Value']['Successful']
        failed = result['Value']['Failed']
        if failed:
          finalResult['Value']['Failed'].setdefault( seName, {} ).update( failed )
        if success:
          if success[lfn].get( 'register' ) == 0 and success[lfn].get( 'replicate' ) == 0:
            success[lfn] = 'Already present'
          finalResult['Value']['Successful'].setdefault( seName, {} ).update( success )

  exit( printDMResult( finalResult ) )

def executeSetProblematicFiles( dmScript ):

  reset = False
  fullInfo = False
  action = True
  switches = Script.getUnprocessedSwitches()
  for switch in switches:
    if switch[0] == 'Reset':
      reset = True
    if switch[0] == 'Full':
      fullInfo = True
    if switch[0] == 'NoAction':
      action = False

  lfnList, targetSEs = parseArguments( dmScript )
  if len( lfnList ) == 0:
    gLogger.fatal( "There are no files to process... check parameters..." )
    exit( 0 )

  setProblematicFiles( lfnList, targetSEs, reset, fullInfo, action )
  exit( 0 )

def setProblematicFiles( lfnList, targetSEs, reset = False, fullInfo = False, action = True ):
  startTime = time.time()
  fc = FileCatalog()
  tr = TransformationClient()
  bk = BookkeepingClient()

  gLogger.always( "Now processing %d files" % len( lfnList ) )
  chunkSize = 1000
  putDots = len( lfnList ) > chunkSize
  if putDots:
    sys.stdout.write( 'Getting replicas from FC (chunks of %d): ' % chunkSize )
  replicas = {'Successful':{}, 'Failed':{}}
  for chunk in breakListIntoChunks( lfnList, chunkSize ):
    if putDots:
      sys.stdout.write( '.' )
      sys.stdout.flush()
    res = fc.getReplicas( chunk, allStatus = True )
    if not res['OK']:
      gLogger.error( "Error getting file replicas:", res['Message'] )
      exit( 1 )
    replicas['Successful'].update( res['Value']['Successful'] )
    replicas['Failed'].update( res['Value']['Failed'] )

  if putDots:
    gLogger.always( '' )
    putDots = False
  repsDict = {}
  repsMultDict = {}
  transDict = {}
  notFound = []
  bkToggle = []
  notFoundAtSE = []
  transNotSet = {}
  gLogger.always( 'Checking with FC' )
  for lfn in lfnList:
    if lfn in replicas['Failed']:
      notFound.append( lfn )
    elif lfn in replicas['Successful']:
      reps = replicas['Successful'][lfn]
      overlapSEs = [se for se in reps if not targetSEs or se in targetSEs]
      if not overlapSEs:
        notFoundAtSE.append( lfn )
        continue
      # Set the file problematic in the LFC
      if len( overlapSEs ) == 1:
        repsDict[lfn] = {'SE': overlapSEs[0], 'Status':'-' if reset else 'P', 'PFN': reps[overlapSEs[0]]}
      else:
        repsMultDict[lfn] = [{'SE': se, 'Status':'-' if reset else 'P', 'PFN': reps[se] } for se in overlapSEs]
      # Now see if the file is present in a transformation
      otherSEs = [se for se in reps if se not in overlapSEs]
      if not otherSEs or reset:
        bkToggle.append( lfn )

  if bkToggle:
    chunkSize = 100
    putDots = len( bkToggle ) > chunkSize
    if putDots:
      sys.stdout.write( 'Checking with Transformation system (chunks of %d): ' % chunkSize )
    transStatusOK = { True:( 'Problematic', 'MissingLFC', 'MissingInFC', 'ProbInFC', 'MaxReset' ), False:( 'Unused', 'MaxReset', 'Assigned' )}
    for chunk in breakListIntoChunks( bkToggle, chunkSize ):
      if putDots:
        sys.stdout.write( '.' )
        sys.stdout.flush()
      res = tr.getTransformationFiles( {'LFN': chunk} )
      if res['OK']:
        for trDict in res['Value']:
          transID = trDict['TransformationID']
          status = trDict['Status']
          if not reset and status == 'Problematic':
            continue
          lfn = trDict['LFN']
          if status in transStatusOK[reset]:
            transDict.setdefault( transID, [] ).append( lfn )
          else:
            transNotSet.setdefault( status, [] ).append( ( lfn, transID ) )

  # Now take actions and print results
  if putDots:
    gLogger.always( '' )
  gLogger.setLevel( 'INFO' if fullInfo else 'WARNING' )
  if notFound:
    gLogger.always( "\n%d files not found in FC" % len( notFound ) )
    for lfn in notFound:
      gLogger.info( '\t%s' % lfn )

  if notFoundAtSE:
    gLogger.always( "%d files not found in FC at any of %s" % ( len( notFoundAtSE ), targetSEs ) )
    for lfn in notFoundAtSE:
      gLogger.info( '\t%s' % lfn )

  status = '-' if reset else 'P'
  if repsDict:
    res = fc.setReplicaStatus( repsDict ) if action else {'OK':True}
    if not res['OK']:
      gLogger.error( "Error setting replica status to %s in FC for %d files" % ( status, len( repsDict ) ), res['Message'] )
    else:
      gLogger.always( "Replicas set (%s) in FC for %d files" % ( status, len( repsDict ) ) )
    for lfn in repsDict:
      gLogger.info( '\t%s' % lfn )
  if repsMultDict:
    nbReps = 0
    for lfn in repsMultDict:
      for repDict in repsMultDict[lfn]:
        res = fc.setReplicaStatus( {lfn:repDict} ) if action else {'OK':True}
        if not res['OK']:
          gLogger.error( "Error setting replica status to %s in FC for %d files" % ( status, len( repsDict ) ), res['Message'] )
        else:
          nbReps += 1
    gLogger.always( "%d replicas set (%s) in FC for %d files" % ( nbReps, status, len( repsMultDict ) ) )
    for lfn in repsMultDict:
      gLogger.info( '\t%s' % lfn )

  if bkToggle:
    if reset:
      stat = 'set'
      res = bk.addFiles( bkToggle ) if action else {'OK':True}
    else:
      stat = 'removed'
      res = bk.removeFiles( bkToggle ) if action else {'OK':True}
    if not res['OK']:
      gLogger.error( "Replica flag not %s in BK for %d files" % ( stat, len( bkToggle ) ), res['Message'] )
    else:
      success = res['Value']['Successful']
      if success:
        gLogger.always( "Replica flag %s in BK for %d files" % ( stat, len( success ) ) )
        for lfn in success:
          gLogger.info( '\t%s' % lfn )

  if transDict:
    n = 0
    for lfns in transDict.values():
      n += len( lfns )
    status = 'Unused' if reset else 'Problematic'
    gLogger.always( "\n%d files were set %s in the transformation system" % ( n, status ) )
    for transID in sorted( transDict ):
      lfns = sorted( transDict[transID] )
      res = tr.setFileStatusForTransformation( transID, status, lfns, force = True ) if action else {'OK':True}
      if not res['OK']:
        gLogger.error( "\tError setting %d files %s for transformation %s" % ( len( lfns ), status, transID ), res['Message'] )
      else:
        gLogger.always( "\t%d files set %s for transformation %s" % ( len( lfns ), status, transID ) )
      for lfn in lfns:
        gLogger.info( '\t\t%s' % lfn )

  if transNotSet:
    n = 0
    for lfns in transNotSet.values():
      n += len( lfns )
    status = "Unused" if reset else "Problematic"
    gLogger.always( "\n%d files could not be set %s a they were not in an acceptable status:" % ( n, status ) )
    for status in sorted( transNotSet ):
      transDict = {}
      for lfn, transID in transNotSet[status]:
        transDict.setdefault( transID, [] ).append( lfn )
      for transID in transDict:
        gLogger.always( "\t%d files were in status %s in transformation %s" % ( len( transDict[transID] ), status, str( transID ) ) )
        for lfn in transDict[transID]:
          gLogger.verbose( '\t\t%s' % lfn )

  gLogger.always( "Execution completed in %.2f seconds" % ( time.time() - startTime ) )

def executeLfnMetadata( dmScript ):
  """
  Print out the FC metadata of a list of LFNs
  """
  lfnList, _ses = parseArguments( dmScript )
  if not lfnList:
    gLogger.fatal( "No list of LFNs provided" )
    Script.showHelp()
    exit( 0 )

  gLogger.setLevel( "FATAL" )
  res = FileCatalog().getFileMetadata( lfnList )
  if res['OK']:
    printDMResult( res, empty = "File not in FC" )
  else:
    gLogger.fatal( "Error getting metadata for %s" % ( lfnList ), res['Message'] )
    printDMResult( res, empty = "File not in FC", script = "dirac-dms-lfn-metadata" )
  exit( 0 )

def executeGetFile( dmScript ):
  lfnList, _ses = parseArguments( dmScript )

  dirList = dmScript.getOption( 'Directory', [''] )
  if len( dirList ) > 1:
    gLogger.fatal( "Not allowed to specify more than one destination directory" )
    exit( 2 )

  dm = DataManager()
  res = dm.getFile( lfnList, destinationDir = dirList[0] )
  exit( printDMResult( res,
                             empty = "No allowed replica found", script = "dirac-dms-get-file" ) )

def __buildLfnDict( item_list ):
  """
    From the input list, populate the dictionary
  """
  lfn_dict = {}
  lfn_dict['lfn'] = item_list[0].replace( 'LFN:', '' ).replace( 'lfn:', '' )
  lfn_dict['localfile'] = item_list[1]
  lfn_dict['SE'] = item_list[2]
  guid = None
  if len( item_list ) > 3:
    guid = item_list[3]
  lfn_dict['guid'] = guid
  return lfn_dict

def executeAddFile():
  """
    Add a file to a Grid storage element
  """
  args = Script.getPositionalArgs()
  if len( args ) < 1 or len( args ) > 4:
    Script.showHelp()
    exit( 1 )
  lfnList = []
  if len( args ) == 1:
    inputFileName = args[0]
    if os.path.exists( inputFileName ):
      inputFile = open( inputFileName, 'r' )
      for line in inputFile:
        items = line.rstrip().split()
        items[0] = items[0].replace( 'LFN:', '' ).replace( 'lfn:', '' )
        lfnList.append( __buildLfnDict( items ) )
      inputFile.close()
  else:
    lfnList.append( __buildLfnDict( args ) )

  if not lfnList:
    gLogger.fatal( "No arguments given" )
    Script.showHelp()
    exit( 2 )

  exitCode = 0

  dm = DataManager()
  logLevel = gLogger.getLevel()
  for lfnDict in lfnList:
    localFile = lfnDict['localfile']
    if not os.path.exists( localFile ):
      gLogger.error( "File %s doesn't exist locally" % localFile )
      exitCode = 1
      continue
    if not os.path.isfile( localFile ):
      gLogger.error( "%s is not a file" % localFile )
      exitCode = 2
      continue

    if not lfnDict['guid']:
      from LHCbDIRAC.Core.Utilities.File import makeGuid
      lfnDict['guid'] = makeGuid( localFile )[localFile]
    lfn = lfnDict['lfn']
    gLogger.always( "\nUploading %s as %s" % ( localFile, lfn ) )
    gLogger.setLevel( 'FATAL' )
    res = dm.putAndRegister( lfn, localFile, lfnDict['SE'], lfnDict['guid'] )
    gLogger.setLevel( logLevel )
    if not res['OK']:
      exitCode = 3
      gLogger.error( 'Error: failed to upload %s to %s' % ( localFile, lfnDict['SE'] ), res['Message'] )
    else:
      if lfn in res['Value']['Successful']:
        gLogger.always( 'Successfully uploaded %s to %s (%.1f seconds)' % ( localFile, lfnDict['SE'],
                                                                            res['Value']['Successful'][lfn]['put'] ) )
      else:
        gLogger.error( 'Error: failed to upload %s to %s' % ( lfn, lfnDict['SE'] ), res['Value']['Failed'][lfn] )

  exit( exitCode )

def __isOlderThan( cTimeStruct, days ):
  from datetime import datetime, timedelta
  return cTimeStruct < ( datetime.utcnow() - timedelta( days = days ) )

def executeListDirectory( dmScript, days = 0, months = 0, years = 0, wildcard = None ):
  """
  List a FC directory contents recursively
  """
  emptyDirsFlag = False
  outputFlag = False
  if wildcard == None:
    wildcard = '*'
  for switch in Script.getUnprocessedSwitches():
    if switch[0] == "Days":
      days = int( switch[1] )
    elif switch[0] == "Months":
      months = int( switch[1] )
    elif switch[0] == "Years":
      years = int( switch[1] )
    elif switch[0] == "Wildcard":
      wildcard = switch[1]
    elif switch[0] == "Emptydirs":
      emptyDirsFlag = True
    elif switch[0] == 'Output':
      outputFlag = True


  verbose = False
  if days or months or years:
    verbose = True
  totalDays = 0
  if years:
    totalDays += 365 * years
  if months:
    totalDays += 30 * months
  if days:
    totalDays += days

  import fnmatch
  fc = FileCatalog()
  baseDirs = dmScript.getOption( 'Directory', [] )
  args = Script.getPositionalArgs()
  for arg in args:
    baseDirs += arg.split( ',' )

  for baseDir in baseDirs:
    gLogger.info( 'Will search for files in %s' % baseDir )
    activeDirs = [baseDir]

    allFiles = set()
    emptyDirs = set()
    while len( activeDirs ) > 0:
      currentDir = activeDirs.pop( 0 )
      res = fc.listDirectory( currentDir, verbose )
      if not res['OK']:
        gLogger.error( "Error retrieving directory contents", "%s %s" % ( currentDir, res['Message'] ) )
      elif currentDir in res['Value']['Failed']:
        gLogger.error( "Error retrieving directory contents", "%s %s" % ( currentDir, res['Value']['Failed'][currentDir] ) )
      else:
        dirContents = res['Value']['Successful'][currentDir]
        empty = True
        for subdir, metadata in dirContents['SubDirs'].items():
          if ( not verbose ) or __isOlderThan( metadata['CreationDate'], totalDays ):
            activeDirs.append( subdir )
          empty = False
        for filename, fileInfo in dirContents['Files'].items():
          metadata = fileInfo['MetaData']
          if ( not verbose ) or __isOlderThan( metadata['CreationDate'], totalDays ):
            if fnmatch.fnmatch( filename, wildcard ):
              allFiles.add( filename )
          empty = False
        gLogger.notice( "%s: %d files, %d sub-directories" % ( currentDir, len( dirContents['Files'] ), len( dirContents['SubDirs'] ) ) )
        if empty:
          emptyDirs.add( currentDir )

    if outputFlag:
      outputFileName = '%s.lfns' % baseDir[1:].replace( '/', '-' )
      outputFile = open( outputFileName, 'w' )
      outputFile.write( '\n'.join( sorted( allFiles ) ) )
      outputFile.close()
      gLogger.notice( '%d matched files have been put in %s' % ( len( allFiles ), outputFileName ) )
    else:
      gLogger.always( '\n'.join( sorted( allFiles ) ) )

    if emptyDirsFlag:
      outputFileName = '%s.emptydirs' % baseDir[1:].replace( '/', '-' )
      outputFile = open( outputFileName, 'w' )
      outputFile.write( '\n'.join( sorted( emptyDirs ) ) )
      outputFile.close()
      gLogger.notice( '%d empty directories have been put in %s' % ( len( emptyDirs ), outputFileName ) )

  exit( 0 )
