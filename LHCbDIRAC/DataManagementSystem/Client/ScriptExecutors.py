"""
Set of functions used by the DMS scripts
"""

import sys
import os
import time
import random

from DIRAC                                                  import gLogger, gConfig, S_OK
from DIRAC.Core.Utilities.List                              import breakListIntoChunks
from DIRAC.Core.Base                                        import Script
from DIRAC.DataManagementSystem.Client.DataManager          import DataManager
from DIRAC.Resources.Catalog.FileCatalog                    import FileCatalog
from DIRAC.Resources.Storage.StorageElement                 import StorageElement
from DIRAC.DataManagementSystem.Utilities.DMSHelpers        import DMSHelpers, resolveSEGroup
from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient   import BookkeepingClient
from LHCbDIRAC.TransformationSystem.Client.TransformationClient        import TransformationClient
from LHCbDIRAC.DataManagementSystem.Client.DMScript         import printDMResult, ProgressBar

__RCSID__ = "$Id$"


def __checkSEs( args, expand = True ):
  if expand:
    expanded = []
    for arg in args:
      expanded += arg.split( ',' )
  else:
    expanded = args
  seList = []
  allSEs = set( DMSHelpers().getStorageElements() )
  for ses in list( expanded ):
    sel = allSEs.intersection( ses.split( ',' ) )
    if sel :
      seList.append( ','.join( list( sel ) ) )
      expanded.remove( ses )
  return seList, expanded

def __getSEsFromOptions( dmScript ):
  seList = dmScript.getOption( 'SEs', [] )
  sites = dmScript.getOption( 'Sites', [] )
  if sites:
    siteSEs = []
    dmsHelper = DMSHelpers()
    for site in sites:
      siteSEs += dmsHelper.getSEsForSite( site ).get( 'Value', [] )
    if seList and siteSEs:
      seList = list( set( seList ) & set( siteSEs ) )
    else:
      seList += siteSEs
  return seList

def parseArguments( dmScript, allSEs = False ):
  """
  Analyse the options passed using the DMScript options,
  returns a list of LFNs and a list of SEs
  """
  dmsHelper = DMSHelpers()
  if allSEs:
    seList = dmsHelper.getStorageElements()
  else:
    seList = __getSEsFromOptions( dmScript )

  # LFNs and seList passed as positional arguments
  ses, args = __checkSEs( Script.getPositionalArgs() )
  if not allSEs:
    seList += ses
  for lfn in args:
    dmScript.setLFNsFromFile( lfn )
  lfnList = dmScript.getOption( 'LFNs', [] )

  # LFNs from BK
  if not lfnList:
    from LHCbDIRAC.BookkeepingSystem.Client.BKQuery import BKQuery
    bkQuery = dmScript.getBKQuery()
    if bkQuery:
      bkFile = bkQuery.getPath()
      # Trick to be able to pass a file containing BKpaths
      if os.path.exists( bkFile ):
        f = open( bkFile, 'r' )
        lines = f.read().splitlines()
        f.close()
        bkQueries = [BKQuery( ll.strip().split()[0] ) for ll in lines ]
        gLogger.notice( "Executing %d BKQueries" % len( bkQueries ) )
      else:
        bkQueries = [bkQuery]
      multi = len( bkQueries ) > 1
      for bkQuery in bkQueries:
        if isinstance( bkQuery, BKQuery ):
          if not multi:
            gLogger.notice( "Executing BKQuery:", bkQuery )
          lfnList += bkQuery.getLFNs( printOutput = not multi )
      if multi:
        gLogger.notice( 'Got %d LFNs' % len( lfnList ) )

  return sorted( lfnList ), seList

def executeRemoveReplicas( dmScript, allDisk = False ):
  """
  get options for remove-replicas and cal for it
  """
  checkFC = True
  force = False

  lfnList, seList = parseArguments( dmScript, allSEs = allDisk )
  if not lfnList:
    gLogger.fatal( "No LFNs have been supplied" )
    return 1
  if not allDisk:
    # Only remove from selected seList
    minReplicas = 1
  else:
    # Remove from all seList
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
        return 1

  # This should be improved, with disk seList first...
  if not seList:
    gLogger.fatal( "Provide SE name (list) as last argument or with --SE option" )
    Script.showHelp()
    return -1

  return removeReplicas( lfnList, seList, minReplicas, checkFC, allDisk, force )

def removeReplicas( lfnList, seList, minReplicas = 1, checkFC = True, allDisk = False, force = False ):
  """
  remove replicas from a list of SEs or all disk SEs
  2 different methods are used to remove registered and unregistered replicas
  If the file is entirely removed, it is set Removed in the TS
  """
  if not checkFC:
    res = removeReplicasNoFC( lfnList, sorted( seList ) )
    if not res['OK']:
      gLogger.fatal( "Completely failed removing replicas without FC", res['Message'] )
      return -1
    successfullyRemoved = res['Value']['Successful']
    fullyRemoved = res['Value']['FullyRemoved']
    errorReasons = res['Value']['Failed']
  else:
    res = removeReplicasWithFC( lfnList, sorted( seList ), minReplicas, allDisk, force )
    if not res['OK']:
      gLogger.fatal( "Completely failed removing replicas with FC", res['Message'] )
      return -1
    successfullyRemoved = res['Value']['Successful']
    if allDisk:
      fullyRemoved = successfullyRemoved
    else:
      fullyRemoved = set()
    errorReasons = res['Value']['Failed']

  if fullyRemoved or allDisk:
    lfnList = fullyRemoved
    for lfns in [lfns for reason, siteLFNs in errorReasons.iteritems() for lfns in siteLFNs.itervalues() if reason == 'Only ARCHIVE replicas']:
      lfnList.update( dict.fromkeys( lfns, [] ) )
    if lfnList:
      removeFilesInTransformations( list( lfnList ) )

  # Print result
  if successfullyRemoved:
    for se, rep in successfullyRemoved.iteritems():
      nrep = len( rep )
      if nrep:
        gLogger.notice( "Successfully removed %d replicas from %s" % ( nrep, se ) )
  for reason, seDict in errorReasons.iteritems():
    for se, lfns in seDict.iteritems():
      gLogger.notice( "Failed to remove %d replicas from %s with reason: %s" % ( len( lfns ), se, reason ) )
  if not successfullyRemoved and not errorReasons and not checkFC:
    gLogger.notice( "Replicas were found at no SE in %s" % str( seList ) )
  return 0

def removeReplicasWithFC( lfnList, seList, minReplicas = 1, allDisk = False, force = False ):
  """
  Remove registered files
  """
  dm = DataManager()
  bk = BookkeepingClient()
  #########################
  # Normal removal using FC
  #########################
  archiveSEs = set( resolveSEGroup( 'Tier1-ARCHIVE' ) )
  errorReasons = {}
  successfullyRemoved = {}
  fullyRemoved = set()
  notExisting = {}
  savedLevel = gLogger.getLevel()
  seList = set( seList )
  chunkSize = max( 50, min( 500, len( lfnList ) / 10 ) )
  progressBar = ProgressBar( len( lfnList ), title = 'Removing replicas' + ( ' and setting them invisible in BK' if allDisk else '' ), chunk = chunkSize )
  # Set files invisible in BK if removing all disk replicas
  for lfnChunk in breakListIntoChunks( sorted( lfnList ), chunkSize ):
    progressBar.loop()
    if allDisk:
      res = bk.setFilesInvisible( lfnChunk )
      if not res['OK']:
        gLogger.error( "\nError setting files invisible in BK", res['Message'] )
        return -3
    res = dm.getReplicas( lfnChunk, getUrl = False )
    if not res['OK']:
      gLogger.fatal( "\nFailed to get replicas", res['Message'] )
      return -2
    if res['Value']['Failed']:
      successfullyRemoved.setdefault( 'SEs (not in FC)', set() ).update( res['Value']['Failed'] )
      fullyRemoved.update( res['Value']['Failed'] )

    repsToRemove = {}
    filesToRemove = []
    for lfn in res['Value']['Successful']:
      existingReps = set( res['Value']['Successful'][lfn] )
      if allDisk and force and not existingReps & archiveSEs:
        # There are no archives, but remove all disk replicas, i.e. removeFile
        filesToRemove.append( lfn )
        continue
      if allDisk:
        existingReps -= archiveSEs
      if not seList & existingReps:
        if allDisk:
          reason = 'Only ARCHIVE replicas'
        else:
          reason = 'No replicas at requested SEs (%d existing)' % len( existingReps )
        errorReasons.setdefault( reason, {} ).setdefault( 'anywhere', [] ).append( lfn )
      elif len( existingReps ) <= minReplicas:
        if force and not existingReps - seList:
          filesToRemove.append( lfn )
        else:
          seString = ','.join( sorted( seList & existingReps ) )
          errorReasons.setdefault( 'No replicas to remove (%d existing/%d requested)' % ( len( existingReps ), minReplicas ), {} ).setdefault( seString, [] ).append( lfn )
      else:
        removeSEs = sorted( existingReps & seList )
        remaining = len( existingReps - seList )
        if remaining < minReplicas:
          # Not enough replicas outside seList, remove only part, otherwisae remove all
          random.shuffle( removeSEs )
          seString = ','.join( removeSEs[remaining - minReplicas:] )
          errorReasons.setdefault( 'Not all replicas could be removed in order to keep at least %d' % minReplicas, {} ).setdefault( seString, [] ).append( lfn )
          removeSEs = removeSEs[0:remaining - minReplicas]
        if removeSEs:
          removeSEs = tuple( removeSEs )
          repsToRemove.setdefault( removeSEs, [] ).append( lfn )

    # If some files need to be fully removed, do it
    if filesToRemove:
      if savedLevel not in ( 'DEBUG', 'VERBOSE' ):
        gLogger.setLevel( 'FATAL' )
      res = dm.removeFile( filesToRemove )
      gLogger.setLevel( savedLevel )
      if not res['OK']:
        gLogger.fatal( "Failed to remove files", res['Message'] )
        return -2
      for lfn, reason in res['Value']['Failed'].iteritems():
        reason = str( reason )
        if 'File does not exist' not in reason:
          errorReasons.setdefault( str( reason ), {} ).setdefault( 'AllSEs', [] ).append( lfn )
      successfullyRemoved.setdefault( 'all SEs', set() ).update( res['Value']['Successful'] )
      fullyRemoved.update( res['Value']['Successful'] )

    # Now remove replicas at required SEs
    for removeSEs, lfns in repsToRemove.iteritems():
      for seName in removeSEs:
        if savedLevel not in ( 'DEBUG', 'VERBOSE' ):
          gLogger.setLevel( 'FATAL' )
        res = dm.removeReplica( seName, lfns )
        gLogger.setLevel( savedLevel )
        if not res['OK']:
          gLogger.verbose( "Failed to remove replica", res['Message'] )
          errorReasons.setdefault( res['Message'], {} ).setdefault( seName, [] ).extend( lfns )
        else:
          for lfn, reason in res['Value']['Failed'].iteritems():
            reason = str( reason )
            if 'No such file or directory' in reason:
              notExisting.setdefault( lfn, set() ).add( seName )
            else:
              errorReasons.setdefault( reason, {} ).setdefault( seName, [] ).append( lfn )
          successfullyRemoved.setdefault( seName, set() ).update( res['Value']['Successful'] )
  progressBar.endLoop()

  # Remove replicas from FC if they do not exist physically
  if notExisting:
    res = dm.getReplicas( notExisting.keys() )
    if not res['OK']:
      gLogger.error( "Error getting replicas of %d non-existing files" % len( notExisting ), res['Message'] )
      errorReasons.setdefault( str( res['Message'] ), {} ).setdefault( 'getReplicas', [] ).extend( notExisting.keys() )
    else:
      for lfn, reason in res['Value']['Failed'].iteritems():
        errorReasons.setdefault( str( reason ), {} ).setdefault( None, [] ).append( lfn )
        notExisting.pop( lfn, None )
      replicas = res['Value']['Successful']
      for lfn, ses in notExisting.iteritems():
        for se in ses & set( replicas.get( lfn, [] ) ):
          res = FileCatalog().removeReplica( {lfn:{'SE':se, 'PFN':replicas[lfn][se]}} )
          if not res['OK']:
            gLogger.error( 'Error removing replica in the FC for a non-existing replica', res['Message'] )
            errorReasons.setdefault( str( res['Message'] ), {} ).setdefault( se, [] ).append( lfn )
          elif res['Value']['Failed']:
            for lfn, reason in res['Value']['Failed'].iteritems():
              errorReasons.setdefault( str( reason ), {} ).setdefault( se, [] ).append( lfn )
              notExisting.pop( lfn, None )
      if notExisting:
        removed = 0
        for lfn in notExisting:
          for se in notExisting[lfn]:
            removed += 1
            successfullyRemoved.setdefault( se, set() ).add( lfn )
        gLogger.notice( "Removed from FC %d non-existing replicas" % removed )
  return S_OK( {'Successful': successfullyRemoved, 'FullyRemoved': fullyRemoved, 'Failed': errorReasons} )


def removeReplicasNoFC( lfnList, seList ):
  """
  Remove unregistered files
  """
  dm = DataManager()
  bk = BookkeepingClient()
  savedLevel = gLogger.getLevel()
  ##################################
  # Try and remove PFNs if not in FC
  ##################################
  gLogger.notice( 'Removing %d physical replica from %s, for replicas not in the FC' \
    % ( len( lfnList ), str( seList ) ) )
  # Remove the replica flag in BK just in case
  errorReasons = {}
  successfullyRemoved = {}
  gLogger.verbose( 'Removing replica flag in BK' )
  notInFC = set()
  notInBK = {}
  bkOK = 0
  chunkSize = max( 50, min( 500, len( lfnList ) / 10 ) )
  progressBar = ProgressBar( len( lfnList ), title = 'Removing replica flag in BK for files not in FC', chunk = chunkSize )
  for lfnChunk in breakListIntoChunks( lfnList, chunkSize ):
    progressBar.loop()
    res = dm.getReplicas( lfnChunk, getUrl = False )
    if res['OK'] and res['Value']['Failed']:
      bkToRemove = res['Value']['Failed'].keys()
      notInFC.update( bkToRemove )
      res = bk.removeFiles( bkToRemove )
      if not res['OK']:
        if res['Message']:
          reason = res['Message']
        else:
          reason = 'File is not in BK'
        notInBK.setdefault( reason, [] ).extend( bkToRemove )
      else:
        bkFailed = res['Value'].get( 'Failed', [] )
        if isinstance( bkFailed, dict ):
          for lfn, reason in bkFailed.iteritems():
            notInBK.setdefault( str( reason ), [] ).append( lfn )
        elif isinstance( bkFailed, list ):
          notInBK.setdefault( 'Not in BK', [] ).extend( bkFailed )
        bkOK += len( bkToRemove ) - len( bkFailed )
  progressBar.endLoop( message = ( 'Removed replica flag for %d files' % bkOK ) if bkOK else 'No such files found' )
  for reason, lfns in notInBK.iteritems():
    gLogger.notice( "Failed to remove replica flag in BK for %d files with error: %s" % ( len( lfns ), reason ) )

  inFC = {}
  for seName in seList:
    se = StorageElement( seName )
    progressBar = ProgressBar( len( lfnList ), title = 'Checking and removing files from %s' % seName, chunk = chunkSize )
    for lfnChunk in breakListIntoChunks( lfnList, chunkSize ):
      progressBar.loop()
      lfnChunk = set( lfnChunk )
      lfnsToRemove = lfnChunk & notInFC
      toCheck = list( lfnChunk - notInFC )
      if toCheck:
        gLogger.setLevel( 'FATAL' )
        res = dm.getReplicaIsFile( toCheck, seName )
        gLogger.setLevel( savedLevel )
        if not res['OK']:
          lfnsToRemove.update( toCheck )
        else:
          if res['Value']['Successful']:
            inFC.setdefault( seName, set() ).update( res['Value']['Successful'] )
          lfnsToRemove.update( res['Value']['Failed'] )
      if not lfnsToRemove:
        continue
      savedLevel = gLogger.getLevel()
      gLogger.setLevel( 'FATAL' )
      res = se.exists( list( lfnsToRemove ) )
      gLogger.setLevel( savedLevel )
      if not res['OK']:
        gLogger.error( '\nERROR checking storage files', res['Message'] )
        continue
      lfns = [lfn for lfn, exists in res['Value']['Successful'].iteritems() if exists]
      lfns += [lfn for lfn, reason in res['Value']['Failed'].iteritems() if 'SRM_FILE_BUSY' in reason]
      if not lfns:
        continue
      gLogger.setLevel( 'FATAL' )
      res = se.removeFile( lfns )
      gLogger.setLevel( savedLevel )
      if not res['OK']:
        gLogger.error( '\nERROR removing storage file: ', res['Message'] )
      else:
        failed = res['Value']['Failed']
        for lfn, reason in failed.iteritems():
          if 'No such file or directory' in str( reason ):
            successfullyRemoved.setdefault( seName, set() ).add( lfn )
          else:
            errorReasons.setdefault( str( reason ), {} ).setdefault( seName, [] ).append( lfn )
        successfullyRemoved.setdefault( seName, set() ).update( res['Value']['Successful'] )
    removed = len( successfullyRemoved.get( seName, [] ) )
    progressBar.endLoop( message = ( '%d files removed' % removed ) if removed else 'No replicas found to be removed' )
  if inFC:
    for se, lfns in inFC.iteritems():
      gLogger.notice( '%d files have replica in FC at %s, not removed' % ( len( lfns ), se ) )
  return S_OK( {'Successful': successfullyRemoved, 'FullyRemoved' : notInFC, 'Failed': errorReasons} )

def executeAccessURL( dmScript ):
  """
  Actual script executor
  """
  protocol = None
  for switch in Script.getUnprocessedSwitches():
    if switch[0] == 'Protocol':
      protocol = switch[1].lower().split( ',' )
      if 'root' in protocol and 'xroot' not in protocol:
        protocol.insert( protocol.index( 'root' ), 'xroot' )
      elif 'xroot' in protocol and 'root' not in protocol:
        protocol.insert( protocol.index( 'xroot' ) + 1, 'root' )
  lfnList, seList = parseArguments( dmScript )
  if not lfnList:
    gLogger.notice( "No list of LFNs provided" )
    Script.showHelp()
    return 1
  else:
    return getAccessURL( lfnList, seList, protocol )

def getAccessURL( lfnList, seList, protocol = None ):
  """
  Get TURL at a list of SEs
  """
  dm = DataManager()
  res = dm.getReplicas( lfnList, getUrl = False )
  replicas = res.get( 'Value', {} ).get( 'Successful', {} )
  if not seList:
    seList = sorted( set( se for lfn in lfnList for se in replicas.get( lfn, {} ) ) )
    if len( seList ) > 1:
      gLogger.notice( "Using the following list of SEs: %s" % str( seList ) )
  bk = BookkeepingClient()
  notFoundLfns = set( lfnList )
  results = {'OK':True, 'Value':{'Successful':{}, 'Failed':{}}}
  savedLevel = gLogger.getLevel()
  gLogger.setLevel( 'FATAL' )
  # Check if files are MDF
  bkRes = bk.getFileTypeVersion( lfnList )
  mdfFiles = set( lfn for lfn, fileType in bkRes.get( 'Value', {} ).iteritems() if fileType == 'MDF' )
  for se in seList:
    lfns = [lfn for lfn in lfnList if se in replicas.get( lfn, [] )]
    if lfns:
      res = StorageElement( se ).getURL( lfns, protocol = protocol )
      success = res.get( 'Value', {} ).get( 'Successful' )
      failed = res.get( 'Value', {} ).get( 'Failed' )
      if res['OK']:
        if success:
          for lfn in set( success ) & mdfFiles:
            success[lfn] = 'mdf:' + success[lfn]
          notFoundLfns -= set( success )
          results['Value']['Successful'].setdefault( se, {} ).update( success )
        if failed:
          results['Value']['Failed'].setdefault( se, {} ).update( failed )
      else:
        results['Value']['Failed'].setdefault( se, {} ).update( dict.fromkeys( lfns, res['Message'] ) )
  gLogger.setLevel( savedLevel )

  for se, failed in results['Value']['Failed'].iteritems():
    for lfn in list( failed ):
      if lfn not in notFoundLfns:
        failed.pop( lfn )
      else:
        notFoundLfns.remove( lfn )
  if notFoundLfns:
    results['Value']['Failed'] = dict.fromkeys( sorted( notFoundLfns ), 'File not found in required seList' )

  return printDMResult( results, empty = "File not at SE", script = "dirac-dms-lfn-accessURL" )

def executeRemoveFiles( dmScript ):
  """
  get options for remove-files
  """

  lfnList, _ses = parseArguments( dmScript )
  setProcessed = False
  switches = Script.getUnprocessedSwitches()
  for switch in switches:
    if switch[0] == 'IncludeProcessedFiles':
      setProcessed = True

  return removeFiles( lfnList, setProcessed )

def removeFiles( lfnList, setProcessed = False ):
  """
  Remove files, and set them Removed in the TS
  If setProcessed is True, even Processed files are set Removed
  """
  dm = DataManager()
  fc = FileCatalog()

  errorReasons = {}
  successfullyRemoved = []
  notExisting = []
  # Avoid spurious error messages
  savedLevel = gLogger.getLevel()
  chunkSize = max( 10, min( 100, len( lfnList ) / 10 ) )
  progressBar = ProgressBar( len( lfnList ), title = "Removing %d files" % len( lfnList ), chunk = chunkSize )
  for lfnChunk in breakListIntoChunks( lfnList, chunkSize ):
    progressBar.loop()
    gLogger.setLevel( 'FATAL' )
    res = dm.removeFile( lfnChunk, force = False )
    gLogger.setLevel( savedLevel )
    if not res['OK']:
      gLogger.error( "\nFailed to remove data", res['Message'] )
      continue
    for lfn, reason in res['Value']['Failed'].iteritems():
      reasonStr = str( reason )
      if isinstance( reason, dict ) and str( reason ) == "{'BookkeepingDB': 'File does not exist'}":
        pass
      elif 'No such file or directory' in reasonStr or 'File does not exist' in reasonStr:
        notExisting.append( lfn )
      else:
        errorReasons.setdefault( reasonStr, [] ).append( lfn )
    successfullyRemoved += res['Value']['Successful'].keys()
  progressBar.endLoop()

  if successfullyRemoved + notExisting:
    removeFilesInTransformations( successfullyRemoved + notExisting, setProcessed = setProcessed )

  if notExisting:
    # The files are not yet removed from the catalog!! :(((
    progressBar = ProgressBar( len( notExisting ), title = "Removing %d non-existing files from FC " % len( notExisting ), chunk = chunkSize )
    notExistingRemoved = []
    for lfnChunk in breakListIntoChunks( notExisting, chunkSize ):
      progressBar.loop()
      res = dm.getReplicas( lfnChunk )
      if not res['OK']:
        gLogger.error( "\nError getting replicas of %d non-existing files" % len( lfnChunk ), res['Message'] )
        errorReasons.setdefault( str( res['Message'] ), [] ).extend( lfnChunk )
      else:
        replicas = res['Value']['Successful']
        for lfn in replicas:
          for se in replicas[lfn]:
            res = fc.removeReplica( {lfn:{'SE':se, 'PFN':replicas[lfn][se]}} )
            if not res['OK']:
              gLogger.error( '\nError removing replica in the FC for a non-existing file', res['Message'] )
              errorReasons.setdefault( str( res['Message'] ), [] ).append( lfn )
            else:
              for lfn, reason in res['Value']['Failed'].iteritems():
                errorReasons.setdefault( str( reason ), [] ).append( lfn )
                lfnChunk.remove( lfn )
        if lfnChunk:
          res = fc.removeFile( lfnChunk )
          if not res['OK']:
            gLogger.error( "\nError removing %d non-existing files from the FC" % len( lfnChunk ), res['Message'] )
            errorReasons.setdefault( str( res['Message'] ), [] ).extend( lfnChunk )
          else:
            for lfn, reason in res['Value']['Failed'].iteritems():
              if isinstance( reason, dict ) and str( reason ) == "{'BookkeepingDB': 'File does not exist'}":
                pass
              else:
                errorReasons.setdefault( str( reason ), [] ).append( lfn )
                lfnChunk.remove( lfn )
        notExistingRemoved += lfnChunk
    progressBar.endLoop()
    if notExistingRemoved:
      successfullyRemoved += notExistingRemoved
      gLogger.notice( "Removed from FC %d non-existing files" % len( notExistingRemoved ) )

  if successfullyRemoved:
    gLogger.notice( "Successfully removed %d files" % len( successfullyRemoved ) )
  maxLfns = 20
  for reason, lfns in errorReasons.iteritems():
    nbLfns = len( lfns )
    gLogger.notice( "Failed to remove %d files with error: %s%s" % ( nbLfns, reason, ' (first %d)' % maxLfns if nbLfns > maxLfns else '' ) )
    gLogger.notice( '\n'.join( lfns[:maxLfns] ) )
  return 0

def removeFilesInTransformations( lfns, setProcessed = False ):
  """
  Set files Removed in transformations
  """
  transClient = TransformationClient()
  res = transClient.getTransformationFiles( {'LFN':lfns } )
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
        for transID, lfns in ignoredFiles.iteritems():
          gLogger.notice( '%d files in status Processed in transformation %d: status unchanged' % ( len( lfns ), transID ) )

    for fileDict in [tf for tf in transFiles if tf['Status'] not in ignoreStatus]:
      lfnsToSet.setdefault( fileDict['TransformationID'], [] ).append( fileDict['LFN'] )
    # If required, set files Removed in transformations
    for transID, lfns in lfnsToSet.iteritems():
      res = transClient.setFileStatusForTransformation( transID, 'Removed', lfns, force = True )
      if not res['OK']:
        gLogger.error( 'Error setting %d files to Removed' % len( lfns ), res['Message'] )
      else:
        gLogger.notice( 'Successfully set %d files as Removed in transformation %d' % ( len( lfns ), transID ) )

def executeLfnReplicas( dmScript ):
  """
  get options for lfn-replicas
  """

  lfnList, _ses = parseArguments( dmScript )

  active = True
  preferDisk = False
  diskOnly = False
  switches = Script.getUnprocessedSwitches()
  for switch in switches:
    if switch[0] in ( "a", "All" ):
      active = False
    if switch[0] == 'DiskOnly':
      diskOnly = True
    if switch[0] == 'PreferDisk':
      preferDisk = True

  if not lfnList:
    gLogger.fatal( "No LFNs supplies" )
    Script.showHelp()
    return 1
  return printLfnReplicas( lfnList, active, diskOnly, preferDisk )

def printLfnReplicas( lfnList, active = True, diskOnly = False, preferDisk = False ):
  """
  get the replica list for a list of LFNs and print them depending on options
  """
  dm = DataManager()
  fc = FileCatalog()
  while True:
    dmMethod = dm.getActiveReplicas if active else dm.getReplicas
    res = dmMethod( lfnList, diskOnly = diskOnly, preferDisk = preferDisk )
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
        value['Successful'].setdefault( lfn, {} )
        for se in sorted( replicas[lfn] ):
          res = fc.getReplicaStatus( {lfn:se} )
          if not res['OK']:
            value['Failed'][lfn] = "Can't get replica status"
          else:
            value['Successful'][lfn][se] = "(%s) %s" % ( res['Value']['Successful'][lfn], replicas[lfn][se] )
      res = S_OK( value )
  return printDMResult( res, empty = "No %sreplica found" % ( 'active disk ' if diskOnly else 'allowed ' if active else '' ), script = "dirac-dms-lfn-replicas" )

def executePfnMetadata( dmScript ):
  """
  get options for pfn-metadata
  """

  lfnList, seList = parseArguments( dmScript )

  check = False
  exists = False
  summary = False
  for opt, _val in Script.getUnprocessedSwitches():
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
    return 1
  return printPfnMetadata( lfnList, seList, check, exists, summary )

def printPfnMetadata( lfnList, seList, check = False, exists = False, summary = False ):
  """
  get physical files metadata at a set of SEs
  If requested, it compares the checksum with the FC one
  The printout may be full, terse or just a statistics summary
  """
  from DIRAC.Core.Utilities.Adler import compareAdler
  if len( seList ) > 1:
    gLogger.notice( "Using the following list of SEs: %s" % str( seList ) )
  if len( lfnList ) > 100:
    gLogger.notice( "Getting metadata for %d files, be patient" % len( lfnList ) )

  fc = FileCatalog()

  # gLogger.setLevel( "FATAL" )
  metadata = {'Successful':{}, 'Failed':{}}
  replicas = {}
  # restrict seList to those where the replicas are
  for lfnChunk in breakListIntoChunks( lfnList, 100 ):
    res = fc.getReplicas( lfnChunk, allStatus = True )
    if not res['OK']:
      gLogger.fatal( 'Error getting replicas for %d files' % len( lfnChunk ), res['Message'] )
      return 2
    else:
      replicas.update( res['Value']['Successful'] )
    for lfn in res['Value']['Failed']:
      metadata['Failed'][lfn] = 'FC: ' + res['Value']['Failed'][lfn]
  for lfn in sorted( replicas ):
    if seList and not [se for se in replicas[lfn] if se in seList]:
      metadata['Failed'][lfn] = 'No such file at %s in FC' % ' '.join( seList )
      replicas.pop( lfn )
      lfnList.remove( lfn )
  metadata['Failed'].update( dict.fromkeys( ( url for url in lfnList if url not in replicas and url not in metadata['Failed'] ), 'FC: No active replicas' ) )
  if not seList:
    # take all seList in replicas and add a fake '' to printout the SE name
    seList = [''] + sorted( set( se for lfn in replicas for se in replicas[lfn] ) )
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
            if isinstance( pfnMetadata.get( 'Mode' ), ( int, long ) ):
              pfnMetadata['Mode'] = '%o' % pfnMetadata['Mode']
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
    return printDMResult( S_OK( metadata ), empty = "File not at SE" )
  else:
    nFiles = 0
    counterKeys = ['Not in FC', 'No active replicas', 'Not existing', 'Exists', 'Checksum OK', 'Checksum bad']
    counters = dict.fromkeys( counterKeys, 0 )
    for lfn, reason in metadata['Failed'].iteritems():
      nFiles += 1
      if isinstance( reason, basestring ):
        if reason == 'FC: No active replicas':
          counters['No active replicas'] += 1
        elif reason.startswith( 'FC:' ):
          counters['Not in FC'] += 1
        else:
          counters['Not existing'] += 1
      elif isinstance( reason, dict ):
        for se in reason:
          if reason[se]['Exists']:
            counters['Exists'] += 1
          else:
            counters['Not existing'] += 1
    for lfn, seDict in metadata['Successful'].iteritems():
      nFiles += 1
      for se in seDict:
        if seDict[se]['MatchLFN'] == True:
          counters['Checksum OK'] += 1
        else:
          counters['Checksum bad'] += 1
    gLogger.notice( 'For %d files:' % nFiles )
    for key in counterKeys:
      if counters[key]:
        gLogger.notice( '%s: %d' % ( key.rjust( 20 ), counters[key] ) )
  return 0

def orderSEs( listSEs ):
  """
  Orders a list of SEs with ARCHIVE last
  """
  listSEs = sorted( listSEs )
  dmsHelper = DMSHelpers()
  orderedSEs = [se for se in listSEs if dmsHelper.isSEArchive( se )]
  orderedSEs += [se for se in listSEs if not dmsHelper.isSEArchive( se )]
  return orderedSEs

def executeReplicaStats( dmScript ):
  """
  get options for replica-stats
  """
  getSize = False
  prNoReplicas = False
  prWithArchives = False
  prWithReplicas = False
  prFailover = False
  prSEList = []
  prNotSEList = []
  dumpAtSE = False
  dumpNotAtSE = False
  for switch in Script.getUnprocessedSwitches():
    if switch[0] in ( "S", "Size" ):
      getSize = True
    elif switch[0] == 'DumpNoReplicas':
      prNoReplicas = True
    elif switch[0] == 'DumpWithArchives':
      if switch[1].lower() == 'any':
        prWithArchives = range( 1, 10 )
      else:
        prWithArchives = [int( xx ) for xx in switch[1].split( ',' )]
    elif switch[0] == 'DumpWithReplicas':
      if switch[1].lower() == 'any':
        prWithReplicas = range( 1, 100 )
      else:
        prWithReplicas = [int( xx ) for xx in switch[1].split( ',' )]
    elif switch[0] == 'DumpFailover':
      prFailover = True
    elif switch[0] == 'DumpAtSE':
      dmScript.setSEs( switch[1] )
      dumpAtSE = True
    elif switch[0] == 'DumpAtSite':
      dmScript.setSites( switch[1] )
    elif switch[0] == 'DumpNotAtSE':
      dmScript.setSEs( switch[1] )
      dumpNotAtSE = True

  if dumpAtSE and dumpNotAtSE:
    gLogger.notice( 'You cannot dump At and Not At SE!' )
    return 1

  directories = dmScript.getOption( 'Directory' )
  lfnList, seList = parseArguments( dmScript )
  if dumpAtSE or dumpNotAtSE:
    prSEList = seList

  return printReplicaStats( directories, lfnList, getSize = getSize, prNoReplicas = prNoReplicas,
                     prWithReplicas = prWithReplicas, prWithArchives = prWithArchives,
                     prFailover = prFailover, prSEList = prSEList, notAtSE = dumpNotAtSE )

def printReplicaStats( directories, lfnList, getSize = False, prNoReplicas = False,
                       prWithReplicas = False, prWithArchives = False, prFailover = False, prSEList = None, notAtSE = False ):
  """
  get storage statistics on a dataset (directories or LFN list
  If requested, lists of LFNs with some criteria can be printed out
  """
  dm = DataManager()
  dmsHelper = DMSHelpers()

  repStats = {}
  noReplicas = {}
  withReplicas = {}
  withArchives = {}
  withFailover = set()
  lfnReplicas = {}
  if not prSEList:
    prSEList = set()
  elif not isinstance( prSEList, set ):
    prSEList = set( prSEList )
  if directories:
    for directory in directories:
      res = dm.getReplicasFromDirectory( directory )
      if not res['OK']:
        gLogger.error( res['Message'] )
        continue
      lfnReplicas.update( res['Value'] )
  elif lfnList:
    chunkSize = max( 50, min( 500, len( lfnList ) / 10 ) )
    lfnReplicas = {}
    progressBar = ProgressBar( len( lfnList ), title = 'Getting replicas for %d LFNs' % len( lfnList ), chunk = chunkSize )
    for lfnChunk in breakListIntoChunks( lfnList, chunkSize ):
      progressBar.loop()
      res = dm.getReplicas( lfnChunk, getUrl = False )
      if not res['OK']:
        gLogger.fatal( res['Message'] )
        return 2
      lfnReplicas.update( res['Value']['Successful'] )
      if res['Value']['Failed']:
        repStats[0] = repStats.setdefault( 0, 0 ) + len( res['Value']['Failed'] )
        withReplicas.setdefault( 0, set() ).update( res['Value']['Failed'] )
        for lfn in res['Value']['Failed']:
          noReplicas[lfn] = -1
    progressBar.endLoop()

  if not lfnReplicas:
    gLogger.fatal( "No files found that have a replica...." )
    return 0

  if repStats.get( 0 ):
    gLogger.notice( "%d files found without a replica" % repStats[0] )

  repSEs = {}
  repSites = {}
  maxRep = 0
  maxArch = 0
  nfiles = 0
  totSize = 0
  dumpFromSE = {}
  if getSize:
    lfnSize = {}
    chunkSize = max( 50, min( 500, len( lfnReplicas ) / 10 ) )
    progressBar = ProgressBar( len( lfnReplicas ), title = 'Getting size for %d LFNs' % len( lfnReplicas ), chunk = chunkSize )
    for lfns in breakListIntoChunks( lfnReplicas, chunkSize ):
      progressBar.loop()
      r = FileCatalog().getFileSize( lfns )
      if r['OK']:
        lfnSize.update( r['Value']['Successful'] )
    progressBar.endLoop()
    totSize += sum( val for val in lfnSize.itervalues() )
  for lfn, replicas in lfnReplicas.iteritems():
    seList = set( replicas )
    dumpSE = seList & prSEList
    if dumpSE and not notAtSE:
      seStr = ','.join( sorted( dumpSE ) )
      dumpFromSE.setdefault( seStr, set() ).add( lfn )
    elif not dumpSE and notAtSE:
      dumpFromSE.setdefault( 'any', set() ).add( lfn )
    nrep = len( replicas )
    narchive = -1
    for se in set( seList ):
      if dmsHelper.isSEFailover( se ):
        withFailover.add( lfn )
        nrep -= 1
        repStats[-100] = repStats.setdefault( -100, 0 ) + 1
        if nrep == 0:
          repStats[-101] = repStats.setdefault( -101, 0 ) + 1
        seList.remove( se )
      if dmsHelper.isSEArchive( se ):
        nrep -= 1
        narchive -= 1
    repStats[nrep] = repStats.setdefault( nrep, 0 ) + 1
    withReplicas.setdefault( nrep, set() ).add( lfn )
    withArchives.setdefault( -narchive - 1, set() ).add( lfn )
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
    gLogger.notice( "%d files found (%.3f gigaByte)%s" % ( nfiles, totSize / gigaByte, dirStr ) )
  else:
    gLogger.notice( "%d files found%s" % ( nfiles, dirStr ) )
  gLogger.notice( "\nReplica statistics:" )
  if -100 in repStats:
    gLogger.notice( "Failover replicas: %d files" % repStats[-100] )
    if -101 in repStats:
      gLogger.notice( "   ...of which %d are only in Failover" % repStats[-101] )
    else:
      gLogger.notice( "   ...but all of them are also somewhere else" )
  if maxArch:
    for nrep in range( 1, maxArch + 1 ):
      gLogger.notice( "%d archives: %d files" % ( nrep - 1, repStats.setdefault( -nrep, 0 ) ) )
  for nrep in range( maxRep + 1 ):
    gLogger.notice( "%d replicas: %d files" % ( nrep, repStats.setdefault( nrep, 0 ) ) )

  gLogger.notice( "\nSE statistics:" )
  for se in orderSEs( repSEs ):
    if dmsHelper.isSEFailover( se ):
      continue
    if not dmsHelper.isSEArchive( se ):
      res = dmsHelper.getSitesForSE( se )
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
    gLogger.notice( string )

  gLogger.notice( "\nSites statistics:" )
  for site in sorted( repSites ):
    string = "%16s: %d files" % ( site, repSites[site][0] )
    if getSize:
      string += " - %.3f teraByte" % ( repSites[site][1] / teraByte )
    gLogger.notice( string )

  if prNoReplicas and noReplicas:
    gLogger.notice( "\nFiles without a disk replica:" )
    if prFailover:
      prList = set( noReplicas ) & withFailover
    else:
      prList = noReplicas
    for rep in sorted( prList ):
      gLogger.notice( "%s (%d archives)" % ( rep, noReplicas[rep] ) )

  if isinstance( prWithArchives, list ):
    for n in [m for m in prWithArchives if m in withArchives]:
      gLogger.notice( '\nFiles with %d archives:' % n )
      for rep in sorted( withArchives[n] ):
        gLogger.notice( rep )

  if isinstance( prWithReplicas, list ):
    for n in [m for m in prWithReplicas if m in withReplicas]:
      gLogger.notice( '\nFiles with %d disk replicas:' % n )
      if prFailover:
        prList = withReplicas[n] & withFailover
      else:
        prList = withReplicas[n]
      for rep in sorted( prList ):
        gLogger.notice( rep )
  elif not prNoReplicas and prFailover and withFailover:
    for rep in sorted( withFailover ):
      gLogger.notice( rep )

  if prSEList:
    atOrNot = 'not ' if notAtSE else ''
    gLogger.notice( '\nFiles %spresent at %s' % ( atOrNot, ','.join( sorted( prSEList ) ) ) )
    if not dumpFromSE:
      gLogger.notice( "No files found %sat these SEs" % atOrNot )
    for se in dumpFromSE:
      if not notAtSE:
        gLogger.notice( 'At %s' % se )
      for lfn in dumpFromSE[se]:
        gLogger.notice( '\t%s' % lfn )
  return 0

def executeReplicateLfn( dmScript ):
  """
  get options for replicate-lfn
  """
  # The source SE may be given as second positional argument, therefore do not aggregate
  seList, args = __checkSEs( Script.getPositionalArgs(), expand = False )
  if not seList:
    # No source SE, hence fake with '' and join the destination SEs
    seList = [','.join( __getSEsFromOptions( dmScript ) ), '']
  destList = []
  sourceSE = []
  localCache = ''
  try:
    # Extract the destination and source SEs
    destList, _args = __checkSEs( seList[0].split( ',' ), expand = False )
    sourceSE = seList[1].split( ',' )
  except:
    pass
  # gLogger.notice( seList, destList, sourceSE
  if not destList or len( sourceSE ) > 1:
    gLogger.notice( "No destination SE" if not destList else "More than one source SE" )
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
    gLogger.notice( "No LFNs provided..." )
    Script.showHelp()

  finalResult = replicateLfn( lfnList, sourceSE, destList, localCache )
  return printDMResult( finalResult )

def executeReplicateToRunDestination( dmScript ):
  """
  get information from file for destination according to the run destination
  """
  seList, args = __checkSEs( Script.getPositionalArgs(), expand = False )
  if not seList:
    seList = __getSEsFromOptions( dmScript )
  if not seList:
    gLogger.notice( "No destination SE" )
    Script.showHelp()
    return 1

  for lfn in args:
    dmScript.setLFNsFromFile( lfn )
  lfnList = dmScript.getOption( 'LFNs', [] )
  if not lfnList:
    gLogger.notice( "No LFNs provided..." )
    Script.showHelp()

  bkClient = BookkeepingClient()
  tsClient = TransformationClient()
  finalResult = {'OK':True, 'Value':{"Failed":{}, "Successful":{}}}
  groupByRun = {}
  for lfn in lfnList:
    res = bkClient.getFileMetadata( lfn )
    if not res['OK'] or lfn in res['Value']['Failed']:
      finalResult['Value']['Failed'][lfn] = res['Message']
    else:
      runNumber = res['Value']['Successful'][lfn]['RunNumber']
      groupByRun.setdefault( runNumber, [] ).append( lfn )
  dmsHelper = DMSHelpers()
  groupBySE = {}
  for runNumber, lfns in groupByRun.iteritems():
    res = tsClient.getDestinationForRun( runNumber )
    if not res['OK'] or runNumber not in res['Value']:
      finalResult['Value']['Failed'].update( dict.fromkeys( lfns, res['Message'] ) )
    else:
      dest = res['Value'][runNumber]
      res = dmsHelper.getSEInGroupAtSite( seList, dest )
      if res['OK']:
        destSE = res['Value']
        groupBySE.setdefault( destSE, [] ).extend( lfns )
      else:
        finalResult['Value']['Failed'].update( dict.fromkeys( lfns, res['Message'] ) )
  for destSE, lfns in groupBySE.iteritems():
    result = replicateLfn( lfns, '', [destSE], verbose = True )
    finalResult['Value']['Successful'].update( result['Value']['Successful'] )
    finalResult['Value']['Failed'].update( result['Value']['Failed'] )
  return printDMResult( finalResult )

def replicateLfn( lfnList, sourceSE, destList, localCache = None, verbose = False ):
  """
  replicate a list of LFNs to a list of SEs
  """
  dm = DataManager()
  # print lfnList, destList, sourceSE, localCache
  finalResult = {'OK':True, 'Value':{"Failed":{}, "Successful":{}}}
  if len( lfnList ) > 1 or len ( destList ) > 1 or verbose:
    gLogger.notice( 'Replicating %d files to %s' % ( len( lfnList ), ','.join( destList ) ) +
                    ( ' from %s' % sourceSE if sourceSE else '' ) )
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
  return finalResult

def executeSetProblematicFiles( dmScript ):
  """
  get options for set-problematic-files
  """

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
    return 1
  else:
    return setProblematicFiles( lfnList, targetSEs, reset, fullInfo, action )

def setProblematicFiles( lfnList, targetSEs, reset = False, fullInfo = False, action = True ):
  """
  sets replicas problematic in the FC
  """
  startTime = time.time()
  fc = FileCatalog()
  tr = TransformationClient()
  bk = BookkeepingClient()

  gLogger.notice( "Now processing %d files" % len( lfnList ) )
  chunkSize = max( 50, min( 500, len( lfnList ) / 10 ) )
  progressBar = ProgressBar( len( lfnList ), title = 'Getting replicas from FC ', chunk = chunkSize )
  replicas = {'Successful':{}, 'Failed':{}}
  for chunk in breakListIntoChunks( lfnList, chunkSize ):
    progressBar.loop()
    res = fc.getReplicas( chunk, allStatus = True )
    if not res['OK']:
      gLogger.error( "Error getting file replicas:", res['Message'] )
      return 1
    replicas['Successful'].update( res['Value']['Successful'] )
    replicas['Failed'].update( res['Value']['Failed'] )

  progressBar.endLoop()
  repsDict = {}
  transDict = {}
  notFound = []
  bkToggle = []
  notFoundAtSE = []
  transNotSet = {}
  gLogger.notice( 'Checking with FC' )
  targetSEs = set( targetSEs )
  for lfn in lfnList:
    if lfn in replicas['Failed']:
      notFound.append( lfn )
    elif lfn in replicas['Successful']:
      reps = replicas['Successful'][lfn]
      repsSet = set( reps )
      overlapSEs = repsSet if not targetSEs else repsSet & targetSEs
      if not overlapSEs:
        notFoundAtSE.append( lfn )
        continue
      # Set the file problematic in the LFC
      repsDict[lfn] = dict( ( se, reps[se] ) for se in overlapSEs )
      # Now see if the file is present in a transformation
      otherSEs = repsSet - overlapSEs
      if not otherSEs or reset:
        bkToggle.append( lfn )

  if bkToggle:
    chunkSize = max( 10, min( 100, len( bkToggle ) / 10 ) )
    transStatusOK = { True:( 'Problematic', 'MissingLFC', 'MissingInFC', 'ProbInFC', 'MaxReset' ), False:( 'Unused', 'MaxReset', 'Assigned' )}
    progressBar = ProgressBar( len( bkToggle ), title = 'Checking with Transformation system', chunk = chunkSize )
    for chunk in breakListIntoChunks( bkToggle, chunkSize ):
      progressBar.loop()
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
    progressBar.endLoop()

  # Now take actions and print results
  savedLevel = gLogger.getLevel()
  gLogger.setLevel( 'INFO' if fullInfo else 'WARNING' )
  if notFound:
    gLogger.notice( "\n%d files not found in FC" % len( notFound ) )
    for lfn in notFound:
      gLogger.info( '\t%s' % lfn )

  if notFoundAtSE:
    gLogger.notice( "%d files not found in FC at any of %s" % ( len( notFoundAtSE ), targetSEs ) )
    for lfn in notFoundAtSE:
      gLogger.info( '\t%s' % lfn )

  status = 'problematic' if not reset else 'OK'
  if repsDict:
    nreps = 0
    toSet = len( repsDict )
    chunkSize = max( 10, min( 100, toSet / 10 ) )
    progressBar = ProgressBar( toSet,
                               title = "Setting replicas %s for %d files" % ( status, toSet ),
                               chunk = chunkSize )
    errors = {}
    for lfnChunk in breakListIntoChunks( repsDict, chunkSize ):
      progressBar.loop()
      chunkDict = dict( ( lfn, repsDict[lfn] ) for lfn in lfnChunk )
      res = fc.setReplicaProblematic( chunkDict, revert = reset ) if action else {'OK':True}
      if not res['OK']:
        errors[res['Message']] = errors.setdefault( res['Message'], 0 ) + len( lfnChunk )
      else:
        nreps += sum( len( reps ) for reps in chunkDict.itervalues() )
    progressBar.endLoop( "%d replicas set %s in FC" % ( nreps, status ) )
    for error, nb in errors.iteritems():
      gLogger.error( "Error setting replica %s in FC for %d files" % ( status, nb ), error )

  if bkToggle:
    toSet = len( bkToggle )
    status = 'set' if reset else 'removed'
    chunkSize = max( 10, min( 100, toSet / 10 ) )
    progressBar = ProgressBar( toSet,
                               title = "Replica flag being %s for %d files" % ( status, toSet ),
                               chunk = chunkSize )
    errors = {}
    success = 0
    for lfnChunk in breakListIntoChunks( bkToggle, chunkSize ):
      progressBar.loop()
      if reset:
        res = bk.addFiles( lfnChunk ) if action else {'OK':True}
      else:
        res = bk.removeFiles( lfnChunk ) if action else {'OK':True}
      if not res['OK']:
        errors[res['Message']] = errors.setdefault( res['Message'], 0 ) + len( lfnChunk )
      elif 'Value' in res:
        success += len( res['Value']['Successful'] )
      else:
        success += len( lfnChunk )
    progressBar.endLoop( "Replica flag %s in BK for %d files" % ( status, success ) )
    for error, nb in errors.iteritems():
      gLogger.error( "Replica flag not %s in BK for %d files:" % ( status, nb ), error )

  if transDict:
    n = sum( len( lfns ) for lfns in transDict.itervalues() )
    status = 'Unused' if reset else 'Problematic'
    gLogger.notice( "\n%d files were set %s in the transformation system" % ( n, status ) )
    for transID in sorted( transDict ):
      lfns = sorted( transDict[transID] )
      res = tr.setFileStatusForTransformation( transID, status, lfns, force = True ) if action else {'OK':True}
      if not res['OK']:
        gLogger.error( "\tError setting %d files %s for transformation %s" % ( len( lfns ), status, transID ), res['Message'] )
      else:
        gLogger.notice( "\t%d files set %s for transformation %s" % ( len( lfns ), status, transID ) )
      for lfn in lfns:
        gLogger.info( '\t\t%s' % lfn )

  gLogger.setLevel( savedLevel )
  if transNotSet:
    n = sum( len( lfns ) for lfns in transNotSet.itervalues() )
    status = "Unused" if reset else "Problematic"
    gLogger.notice( "\n%d files could not be set %s a they were not in an acceptable status:" % ( n, status ) )
    for status in sorted( transNotSet ):
      transDict = {}
      for lfn, transID in transNotSet[status]:
        transDict.setdefault( transID, [] ).append( lfn )
      for transID in transDict:
        gLogger.notice( "\t%d files were in status %s in transformation %s" % ( len( transDict[transID] ), status, str( transID ) ) )
        for lfn in transDict[transID]:
          gLogger.verbose( '\t\t%s' % lfn )

  gLogger.notice( "Execution completed in %.2f seconds" % ( time.time() - startTime ) )
  return 0

def __dfcGetDirectoryMetadata( catalog, dirList ):
  success = {}
  failed = {}
  for d in dirList:
    sup = os.path.dirname( d )
    res = catalog.listDirectory( sup, True )
    if res['OK']:
      metadata = res['Value']['Successful'].get( sup, {} ).get( 'SubDirs', {} ).get( d )
      if metadata:
        metadata['isDirectory'] = True
        success[d] = metadata
      else:
        failed[d] = 'No such file or directory'
    else:
      failed[d] = res['Message']
  return S_OK( { 'Successful': success, 'Failed':failed} )

def executeLfnMetadata( dmScript ):
  """
  Print out the FC metadata of a list of LFNs
  """
  lfnList, _ses = parseArguments( dmScript )
  if not lfnList:
    gLogger.fatal( "No list of LFNs provided" )
    Script.showHelp()
    return 0

  catalog = FileCatalog()
  savedLevel = gLogger.getLevel()
  gLogger.setLevel( "FATAL" )
  filesList = [lfn for lfn in lfnList if
               catalog.isFile( lfn ).get( 'Value', {} ).get( 'Successful', {} ).get( lfn )]
  dirList = list( set( lfnList ) - set( filesList ) )
  success = {}
  failed = {}
  if filesList:
    res = catalog.getFileMetadata( filesList )
    if res['OK']:
      success.update( res['Value']['Successful'] )
      failed.update( res['Value']['Failed'] )
    else:
      failed.update( dict.fromkeys( filesList, res['Message'] ) )
  if dirList:
    res = catalog.getDirectoryMetadata( dirList )
    if res['OK']:
      success.update( res['Value']['Successful'] )
      failed.update( res['Value']['Failed'] )
    else:
      res = __dfcGetDirectoryMetadata( catalog, dirList )
      success.update( res['Value']['Successful'] )
      failed.update( res['Value']['Failed'] )
  for metadata in success.itervalues():
    if 'Mode' in metadata:
      metadata['Mode'] = '%o' % metadata['Mode']
  gLogger.setLevel( savedLevel )
  return printDMResult( S_OK( {'Successful':success, 'Failed':failed} ), empty = "File not in FC", script = "dirac-dms-lfn-metadata" )

def executeGetFile( dmScript ):
  """
  get files to a local storage
  """
  lfnList, _ses = parseArguments( dmScript )

  dirList = dmScript.getOption( 'Directory', ['.'] )
  if len( dirList ) > 1:
    gLogger.fatal( "Not allowed to specify more than one destination directory" )
    return 2

  nLfns = len( lfnList )
  gLogger.notice( 'Downloading %s to %s' % ( ( '%d files' % nLfns ) if nLfns > 1 else 'file', dirList[0] ) )
  result = DataManager().getFile( lfnList, destinationDir = dirList[0] )

  # Prepare popularity report
  if result['OK']:
    popReport = {}
    for lfn in result['Value']['Successful']:
      dirName = os.path.join( os.path.dirname( lfn ), '' )
      popReport[dirName] = popReport.setdefault( dirName, 0 ) + 1
    if popReport:
      from LHCbDIRAC.DataManagementSystem.Client.DataUsageClient import DataUsageClient
      localSite = gConfig.getValue( '/LocalSite/Site', 'UNKNOWN' )
      try:
        localSite = localSite.split( '.' )[1]
      except:
        pass
      res = DataUsageClient().sendDataUsageReport( localSite, popReport )
      if not res['OK']:
        gLogger.error( 'Error reporting popularity', res['Message'] )
      else:
        gLogger.info( 'Successfully reported data usage', '(site: %s, usage: %s)' % ( localSite, str( popReport ) ) )

  return printDMResult( result, empty = "No allowed replica found", script = "dirac-dms-get-file" )

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
    return 1
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
    return 2

  exitCode = 0

  dm = DataManager()
  logLevel = gLogger.getLevel()
  for lfnDict in lfnList:
    localFile = lfnDict['localfile']
    remoteFile = None
    if localFile.startswith( '/castor/cern.ch/user' ) or localFile.startswith( '/eos/lhcb/user' ):
      remoteFile = localFile
    elif os.path.exists( localFile ):
      if not os.path.isfile( localFile ):
        gLogger.error( "%s is not a file" % localFile )
        continue
    else:
      gLogger.error( "File %s doesn't exist locally" % localFile )
      continue

    if remoteFile:
      eos = '/afs/cern.ch/project/eos/installation/lhcb/bin/eos.select'
      if not os.path.exists( eos ):
        gLogger.fatal( 'Impossible to download locally file', remoteFile )
        continue
      # fist get the file locally
      if remoteFile.startswith( '/castor' ):
        prefix = 'root://castorpublic.cern.ch/'
      elif remoteFile.startswith( '/eos' ):
        prefix = 'root://eoslhcb.cern.ch/'
      localFile = os.path.join( os.environ.get( 'TMPDIR', os.environ.get( 'TMP', '/tmp' ) ), os.path.basename( remoteFile ) )
      gLogger.notice( 'Attempting to download file to', localFile )
      from subprocess import Popen, PIPE
      process = Popen( [eos, 'cp', '%s%s' % ( prefix, remoteFile ), localFile], stdout = PIPE, stderr = PIPE )
      out = process.stdout.read()
      err = process.stderr.read()
      process.stdout.close()
      process.stderr.close()
      rc = process.wait()
      if rc:
        gLogger.error( "Error downloading file", err )
        continue
      else:
        gLogger.notice( "Download successful", out )

    if not lfnDict['guid']:
      from LHCbDIRAC.Core.Utilities.File import makeGuid
      lfnDict['guid'] = makeGuid( localFile )[localFile]
    lfn = lfnDict['lfn']
    gLogger.notice( "\nUploading %s as %s" % ( localFile, lfn ) )
    gLogger.setLevel( 'FATAL' )
    res = dm.putAndRegister( lfn, localFile, lfnDict['SE'], lfnDict['guid'] )
    gLogger.setLevel( logLevel )
    if not res['OK']:
      exitCode = 3
      gLogger.error( 'Error: failed to upload %s to %s' % ( localFile, lfnDict['SE'] ), res['Message'] )
    else:
      if lfn in res['Value']['Successful']:
        gLogger.notice( 'Successfully uploaded %s to %s (%.1f seconds)' % ( localFile, lfnDict['SE'],
                                                                            res['Value']['Successful'][lfn]['put'] ) )
      else:
        gLogger.error( 'Error: failed to upload %s to %s' % ( lfn, lfnDict['SE'] ), res['Value']['Failed'][lfn] )
    if remoteFile:
      os.remove( localFile )

  return exitCode

def __isOlderThan( cTimeStruct, days ):
  from datetime import datetime, timedelta
  return cTimeStruct < ( datetime.utcnow() - timedelta( days = days ) )

def executeListDirectory( dmScript, days = 0, months = 0, years = 0, wildcard = None, depth = 0 ):
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
    elif switch[0] == 'Depth':
      depth = int( switch[1] )
    elif switch[0] in ( 'r', 'Recursive' ):
      depth = sys.maxint


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

  wildcardSplit = wildcard.split( '/' )
  for baseDir in baseDirs:
    if baseDir[-1] == '/':
      baseDir = baseDir[:-1]
    gLogger.info( 'Will search for files in %s' % baseDir )
    activeDirs = [baseDir]

    allFiles = set()
    emptyDirs = set()
    while len( activeDirs ) > 0:
      currentDir = activeDirs.pop( 0 )
      # Does this directory match tthe wildcard?
      matchLen = len( currentDir.split( '/' ) )
      if not fnmatch.fnmatch( currentDir, '/'.join( wildcardSplit[:matchLen] ) ):
        continue
      res = fc.listDirectory( currentDir, verbose )
      if not res['OK']:
        gLogger.error( "Error retrieving directory contents", "%s %s" % ( currentDir, res['Message'] ) )
      elif currentDir in res['Value']['Failed']:
        gLogger.error( "Error retrieving directory contents", "%s %s" % ( currentDir, res['Value']['Failed'][currentDir] ) )
      else:
        dirContents = res['Value']['Successful'][currentDir]
        empty = True
        for subdir in sorted( dirContents['SubDirs'] ):
          metadata = dirContents['SubDirs'][subdir]
          d = len( subdir.replace( baseDir, '' ).split( '/' ) )
          # print subdir, baseDir, subdir.replace( baseDir, '' ).split( '/' ), d
          if ( d < depth + 2 ) and ( not verbose or __isOlderThan( metadata['CreationDate'], totalDays ) ):
            activeDirs.append( subdir )
          empty = False
        for filename in sorted( dirContents['Files'] ):
          fileInfo = dirContents['Files'][filename]
          metadata = fileInfo['MetaData']
          if ( not verbose ) or __isOlderThan( metadata['CreationDate'], totalDays ):
            if fnmatch.fnmatch( filename, wildcard ):
              allFiles.add( filename )
          empty = False
        gLogger.notice( "%s/: %d files, %d sub-directories" % ( currentDir, len( dirContents['Files'] ), len( dirContents['SubDirs'] ) ) )
        if empty:
          emptyDirs.add( currentDir )

    if outputFlag:
      outputFileName = '%s.lfns' % baseDir[1:].replace( '/', '-' )
      outputFile = open( outputFileName, 'w' )
      outputFile.write( '\n'.join( sorted( allFiles ) ) )
      outputFile.close()
      gLogger.notice( '%d matched files have been put in %s' % ( len( allFiles ), outputFileName ) )
    else:
      gLogger.notice( '\n'.join( sorted( allFiles ) ) )

    if emptyDirsFlag:
      outputFileName = '%s.emptydirs' % baseDir[1:].replace( '/', '-' )
      outputFile = open( outputFileName, 'w' )
      outputFile.write( '\n'.join( sorted( emptyDirs ) ) )
      outputFile.close()
      gLogger.notice( '%d empty directories have been put in %s' % ( len( emptyDirs ), outputFileName ) )

  return 0
