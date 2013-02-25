#!/usr/bin/env python
########################################################################
# $HeadURL$
########################################################################
__RCSID__ = "$Id$"
from DIRAC.Core.Base import Script
from LHCbDIRAC.DataManagementSystem.Client.DMScript import DMScript
from DIRAC import DIRAC, gConfig, gLogger

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

if __name__ == "__main__":
  dmScript = DMScript()
  dmScript.registerFileSwitches()
  dmScript.registerSiteSwitches()

  Script.registerSwitch( "v", "Verbose", " use this option for verbose output [False]" )
  Script.registerSwitch( "n", "NoLFC", " use this option to force the removal from storage of replicas not in FC" )
  Script.setUsageMessage( """
  Remove the given file replica or a list of file replicas from the File Catalog
  and from the storage.

  Usage:
     %s <LFN | fileContainingLFNs> SE [SE]
  """ % Script.scriptName )

  verbose = False
  checkFC = True
  Script.parseCommandLine()

  from DIRAC.Core.Utilities.List                        import breakListIntoChunks
  from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager
  rm = ReplicaManager()

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
  if not seList:
    seList, args = __checkSEs( args )
  # This should be improved, with disk SEs first...
  if not seList:
    gLogger.fatal( "Give SE name as last argument or with --SE option" )
    Script.showHelp()
  seList.sort()

  for lfn in args:
    dmScript.setLFNsFromFile( lfn )
  lfnList = dmScript.getOption( 'LFNs', [] )


  for switch in Script.getUnprocessedSwitches():
    if switch[0] == "v" or switch[0].lower() == "verbose":
      verbose = True
    if switch[0] == "n" or switch[0].lower() == "nolfc":
      checkFC = False


  errorReasons = {}
  successfullyRemoved = {}
  notExisting = {}
  gLogger.setLevel( 'ERROR' )

  if not checkFC:
    ##################################
    # Try and remove PFNs if not in FC
    ##################################
    gLogger.always( 'Removing %d physical replica from %s, without removing entry in the FC' \
      % ( len( lfnList ), str( seList ) ) )
    from DIRAC.Resources.Storage.StorageElement         import StorageElement
    for seName in seList:
      se = None
      for lfnChunk in breakListIntoChunks( sorted( lfnList ), 500 ):
        gLogger.verbose( 'Check if replica is registered in FC at %s..' % seName )
        res = rm.getReplicaIsFile( lfnChunk, seName )
        lfnsToRemove = []
        if not res['OK']:
          lfnsToRemove = lfnChunk
        else:
          if res['Value']['Successful']:
            gLogger.always( "%d files are in the FC, they will not be removed from %s" \
              % ( len( res['Value']['Successful'] ), seName ) )
          lfnsToRemove += res['Value']['Failed'].keys()
        if not lfnsToRemove:
          continue
        if not se:
          se = StorageElement( seName )
        pfnsToRemove = {}
        gLogger.verbose( '%d replicas NOT registered in FC: removing physical file at %s.. Get the PFNs' \
          % ( len( lfnsToRemove ), seName ) )
        for lfn in lfnsToRemove:
          res = se.getPfnForLfn( lfn )
          if not res['OK']:
            gLogger.error( 'ERROR getting LFN:', lfn, res['Message'] )
          else:
            pfnsToRemove[res['Value']] = lfn
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
        res = se.removeFile( pfns )
        if not res['OK']:
          gLogger.error( 'ERROR removing storage file: ', res['Message'] )
        else:
          gLogger.verbose( "ReplicaManager.removeStorageFile returned: ", res )
          failed = res['Value']['Failed']
          for pfn, reason in failed.items():
            lfn = pfnsToRemove[pfn]
            if 'No such file or directory' in str( reason ):
              successfullyRemoved.setdefault( seName, [] ).append( lfn )
            else:
              errorReasons.setdefault( str( reason ), {} ).setdefault( seName, [] ).append( lfn )
          successfullyRemoved.setdefault( seName, [] ).extend( [pfnsToRemove[pfn] for pfn in res['Value']['Successful']] )

  else:
    #########################
    # Normal removal using FC
    #########################
    gLogger.setLevel( 'FATAL' )
    for lfnChunk in breakListIntoChunks( sorted( lfnList ), 500 ):
      for seName in sorted( seList ):
        res = rm.removeReplica( lfnChunk, seName )
        if not res['OK']:
          gLogger.fatal( "Failed to remove replica", res['Message'] )
          DIRAC.exit( -2 )
        for lfn, reason in res['Value']['Failed'].items():
          reason = str( reason )
          if 'No such file or directory' in reason:
            notExisting.setdefault( lfn, [] ).append( se )
          else:
            errorReasons.setdefault( reason, {} ).setdefault( se, [] ).append( lfn )
        successfullyRemoved.setdefault( se, [] ).extend( res['Value']['Successful'].keys() )

    gLogger.setLevel( 'ERROR' )
    if notExisting:
      res = rm.getReplicas( notExisting.keys() )
      if not res['OK']:
        gLogger.error( "Error getting replicas of %d non-existing files" % len( notExisting ), res['Message'] )
        errorReasons.setdefault( str( res['Message'] ), {} ).setdefault( 'getReplicas', [] ).extend( notExisting.keys() )
      else:
        for lfn, reason in res['Value']['Failed'].items():
          errorReasons.setdefault( str( reason ), {} ).setdefault( se, [] ).append( lfn )
          notExisting.pop( lfn, None )
        replicas = res['Value']['Successful']
        for lfn in notExisting:
          for se in [se for se in notExisting[lfn] if se in replicas.get( lfn, [] )]:
            res = rm.removeCatalogReplica( {lfn:{'SE':se, 'PFN':replicas[lfn][se]}} )
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
      gLogger.always( "Successfully removed %d files from %s" % ( len( successfullyRemoved[se] ), se ) )
  for reason, seDict in errorReasons.items():
    for se, lfns in seDict.items():
      gLogger.always( "Failed to remove %d files from %s with error: %s" % ( len( lfns ), se, reason ) )
  if not successfullyRemoved and not errorReasons and not checkFC:
    gLogger.always( "Files were found at no SE in %s" % str( seList ) )
  DIRAC.exit( 0 )
