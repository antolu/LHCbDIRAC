#!/usr/bin/env python
########################################################################
# $HeadURL$
########################################################################
"""
Remove replicas of a (list of) LFNs at a list of sites. It is possible to request a minimum of remaining replicas
"""

__RCSID__ = "$Id$"
from DIRAC.Core.Base import Script
from LHCbDIRAC.DataManagementSystem.Client.DMScript import DMScript
from DIRAC import gConfig, gLogger

def __checkSEs( args ):
  """ Extract SE names from a list of arguments """
  seList = []
  res = gConfig.getSections( '/Resources/StorageElements' )
  if res['OK']:
    for se in list( args ):
      if se in res['Value']:
        seList.append( se )
        args.remove( se )
  return seList, args

def execute():
  verbose = False
  checkFC = True
  minReplicas = 1

  from DIRAC.Core.Utilities.List                        import breakListIntoChunks, randomize
  from DIRAC.DataManagementSystem.Client.DataManager import DataManager
  from DIRAC.Resources.Catalog.FileCatalog           import FileCatalog
  from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient  import BookkeepingClient
  import DIRAC
  dm = DataManager()
  bk = BookkeepingClient()

  seList = dmScript.getOption( 'SEs', [] )

  if not seList:
    sites = dmScript.getOption( 'Sites', [] )
    for site in sites:
      res = gConfig.getOptionsDict( '/Resources/Sites/LCG/%s' % site )
      if not res['OK']:
        gLogger.fatal( 'Site %s not known' % site )
        Script.showHelp()
      seList.extend( res['Value']['SE'].replace( ' ', '' ).split( ',' ) )
  args = Script.getPositionalArgs()
  args = [aa for arg in args for aa in arg.split( ',' )]
  if not seList:
    seList, args = __checkSEs( args )

  for lfn in args:
    dmScript.setLFNsFromFile( lfn )
  lfnList = sorted( dmScript.getOption( 'LFNs', [] ) )


  for switch in Script.getUnprocessedSwitches():
    if switch[0] == "v" or switch[0].lower() == "verbose":
      verbose = True
    if switch[0] == "n" or switch[0].lower() == "nolfc":
      checkFC = False
    elif switch[0] == 'ReduceReplicas':
      try:
        minReplicas = max( 1, int( switch[1] ) )
        # Set a default for Users
        if not seList:
          dmScript.setSEs( 'Tier1-USER' )
          seList = dmScript.getOption( 'SEs', [] )
      except:
        print "Invalid number of replicas:", switch[1]
        DIRAC.exit( 1 )

  # This should be improved, with disk SEs first...
  if not seList:
    gLogger.fatal( "Give SE name as last argument or with --SE option" )
    Script.showHelp()
  seList.sort()
  dmScript.setSEs( 'Tier1-ARCHIVE' )
  archiveSEs = set( dmScript.getOption( 'SEs', [] ) )

  errorReasons = {}
  successfullyRemoved = {}
  notExisting = {}

  if not checkFC:
    ##################################
    # Try and remove PFNs if not in FC
    ##################################
    gLogger.always( 'Removing %d physical replica from %s, for replicas not in the FC' \
      % ( len( lfnList ), str( seList ) ) )
    from DIRAC.Resources.Storage.StorageElement         import StorageElement
    # Remove the replica flag in BK just in case
    gLogger.verbose( 'Removing replica flag in BK' )
    notInFC = []
    notInBK = {}
    bkOK = 0
    chunkSize = 500
    showProgress = len( lfnList ) > 3 * chunkSize
    if showProgress:
      import sys
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
          if not res['OK']:
            reason = res['Message']
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

  else:
    #########################
    # Normal removal using FC
    #########################
    gLogger.setLevel( 'FATAL' )
    seList = set( seList )
    for lfnChunk in breakListIntoChunks( sorted( lfnList ), 500 ):
      res = dm.getReplicas( lfnChunk )
      if not res['OK']:
        gLogger.fatal( "Failed to get replicas", res['Message'] )
        DIRAC.exit( -2 )
      if res['Value']['Failed']:
        lfnChunk = list( set( lfnChunk ) - set( res['Value']['Failed'] ) )
      seReps = {}
      for lfn in res['Value']['Successful']:
        rep = set( res['Value']['Successful'][lfn] ) - archiveSEs
        if not seList & rep:
          errorReasons.setdefault( 'No replicas at requested sites (%d existing)' % ( len( rep ) ), {} ).setdefault( 'anywhere', [] ).append( lfn )
        elif len( rep ) <= minReplicas:
          seString = ','.join( sorted( seList & rep ) )
          errorReasons.setdefault( 'No replicas to remove (%d existing/%d requested)' % ( len( rep ), minReplicas ), {} ).setdefault( seString, [] ).append( lfn )
        else:
          seString = ','.join( sorted( rep ) )
          seReps.setdefault( seString, [] ).append( lfn )
      for seString in seReps:
        ses = set( seString.split( ',' ) )
        lfns = seReps[seString]
        removeSEs = list( seList & ses )
        remaining = len( ses - seList )
        if remaining < minReplicas:
          # Not enough replicas outside seList, remove only part, otherwisae remove all
          removeSEs = randomize( removeSEs )[0:remaining - minReplicas]
        for seName in sorted( removeSEs ):
          res = dm.removeReplica( seName, lfns )
          if not res['OK']:
            gLogger.fatal( "Failed to remove replica", res['Message'] )
            DIRAC.exit( -2 )
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

  if successfullyRemoved:
    for se in successfullyRemoved:
      gLogger.always( "Successfully removed %d replicas from %s" % ( len( successfullyRemoved[se] ), se ) )
  for reason, seDict in errorReasons.items():
    for se, lfns in seDict.items():
      gLogger.always( "Failed to remove %d replicas from %s with error: %s" % ( len( lfns ), se, reason ) )
  if not successfullyRemoved and not errorReasons and not checkFC:
    gLogger.always( "Replicas were found at no SE in %s" % str( seList ) )
  DIRAC.exit( 0 )

if __name__ == "__main__":
  dmScript = DMScript()
  dmScript.registerFileSwitches()
  dmScript.registerSiteSwitches()

  Script.registerSwitch( "v", "Verbose", " use this option for verbose output [False]" )
  Script.registerSwitch( "n", "NoLFC", " use this option to force the removal from storage of replicas not in FC" )
  Script.registerSwitch( '', 'ReduceReplicas=', '  specify the number of replicas you want to keep (default SE: Tier1-USER)' )
  Script.setUsageMessage( '\n'.join( __doc__.split( '\n' ) + [
                                       'Usage:',
                                       '  %s [option|cfgfile] ... [LFN[,LFN2[,LFN3...]]] SE[,SE2...]' % Script.scriptName,
                                       'Arguments:',
                                       '  LFN:      Logical File Name or file containing LFNs',
                                       '  SE:       Valid DIRAC SE' ] ) )
  Script.parseCommandLine()
  execute()
