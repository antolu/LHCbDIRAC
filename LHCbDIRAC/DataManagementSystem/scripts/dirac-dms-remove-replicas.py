#!/usr/bin/env python
########################################################################
# $HeadURL$
########################################################################
__RCSID__ = "$Id$"
from DIRAC.Core.Base import Script
from LHCbDIRAC.DataManagementSystem.Client.DMScript import DMScript, printDMResult
from DIRAC import DIRAC, gConfig, gLogger

def __checkSEs( args ):
  seList = []
  res = gConfig.getSections( '/Resources/StorageElements' )
  if res['OK']:
    for se in args:
      if se in res['Value']:
        seList.append( se )
        args.remove( se )
    return seList

if __name__ == "__main__":
  dmScript = DMScript()
  dmScript.registerFileSwitches()

  Script.setUsageMessage( """
  Remove the given file replica or a list of file replicas from the File Catalog
  and from the storage.

  Usage:
     %s <LFN | fileContainingLFNs> SE [SE]
  """ % Script.scriptName )

  Script.parseCommandLine()

  from DIRAC.Core.Utilities.List                        import sortList, breakListIntoChunks
  from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager
  rm = ReplicaManager()
  import os, sys

  lfns = dmScript.getOption( 'LFNs', [] )
  args = Script.getPositionalArgs()
  storageElementNames = __checkSEs( args )
  lfns += args

  lfnList = []
  for lfn in lfns:
    try:
      f = open( lfn, 'r' )
      lfnList += [l.split( 'LFN:' )[-1].strip().split()[0].replace( '"', '' ).replace( ',', '' ) for l in f.read().splitlines()]
      f.close()
    except:
      lfnList.append( lfn )

  if not storageElementNames:
    Script.showHelp()
    DIRAC.exit( -1 )

  errorReasons = {}
  successfullyRemoved = {}
  notExisting = {}
  gLogger.setLevel( 'FATAL' )
  for lfnChunk in breakListIntoChunks( sorted( lfnList ), 500 ):
    for se in sorted( storageElementNames ):
      res = rm.removeReplica( se, lfnChunk )
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
        notExisting.pop( lfn )
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
              notExisting.remove( lfn )
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
  DIRAC.exit( 0 )
