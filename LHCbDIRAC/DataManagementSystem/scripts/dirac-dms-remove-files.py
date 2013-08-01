#!/usr/bin/env python
########################################################################
# $HeadURL$
########################################################################
"""
  Remove the given file or a list of files from the File Catalog and from the storage
"""
__RCSID__ = "$Id$"

from DIRAC.Core.Base import Script
from LHCbDIRAC.DataManagementSystem.Client.DMScript import DMScript

if __name__ == "__main__":
  dmScript = DMScript()
  dmScript.registerBKSwitches()
  dmScript.registerFileSwitches()

  Script.registerSwitch( '', 'SetProcessed', '  Forced to set Removed the files in status Processed (default:not reset)' )
  Script.setUsageMessage( '\n'.join( [ __doc__,
                                       'Usage:',
                                       '  %s [option|cfgfile] [<LFN>] [<LFN>...]' % Script.scriptName, ] ) )

  Script.parseCommandLine()

  import sys, os
  import DIRAC
  from DIRAC import gLogger

  for lfn in Script.getPositionalArgs():
    dmScript.setLFNsFromFile( lfn )
  lfnList = dmScript.getOption( 'LFNs', [] )

  bkQuery = dmScript.getBKQuery( visible = 'All' )
  if bkQuery.getQueryDict().keys() not in ( [''], ['Visible'] ):
    print "Executing BKQuery:", bkQuery
    lfnList += bkQuery.getLFNs()

  fixTrans = True
  setProcessed = False
  switches = Script.getUnprocessedSwitches()
  for switch in switches:
    if switch[0] == 'SetProcessed':
      setProcessed = True

  from DIRAC.Core.Utilities.List import sortList, breakListIntoChunks
  from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager
  rm = ReplicaManager()
  if fixTrans:
    from LHCbDIRAC.TransformationSystem.Client.TransformationClient import TransformationClient
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
    res = rm.removeFile( lfnChunk )
    if not res['OK']:
      gLogger.fatal( "Failed to remove data", res['Message'] )
      DIRAC.exit( -2 )
    for lfn, reason in res['Value']['Failed'].items():
      reason = str( reason )
      if 'No such file or directory' in reason or 'File does not exist' in reason:
        notExisting.append( lfn )
      else:
        errorReasons.setdefault( reason, [] ).append( lfn )
    successfullyRemoved += res['Value']['Successful'].keys()
  if verbose:
    print ''
  gLogger.setLevel( 'ERROR' )

  if fixTrans and successfullyRemoved + notExisting:
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

      for fileDict in [fileDict for fileDict in transFiles if fileDict['Status'] not in ignoreStatus]:
        lfnsToSet.setdefault( fileDict['TransformationID'], [] ).append( fileDict['LFN'] )
      # If required, set files Removed in transformations
      for transID, lfns in lfnsToSet.items():
        res = transClient.setFileStatusForTransformation( transID, 'Removed', lfns, force = setProcessed )
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
      res = rm.getReplicas( lfnChunk )
      if not res['OK']:
        gLogger.error( "Error getting replicas of %d non-existing files" % len( lfnChunk ), res['Message'] )
        errorReasons.setdefault( str( res['Message'] ), [] ).extend( lfnChunk )
      else:
        for lfn, reason in res['Value']['Failed'].items():
          errorReasons.setdefault( str( reason ), [] ).append( lfn )
          lfnChunk.remove( lfn )
        replicas = res['Value']['Successful']
        for lfn in replicas:
          for se in replicas[lfn]:
            res = rm.removeCatalogReplica( {lfn:{'SE':se, 'PFN':replicas[lfn][se]}} )
            if not res['OK']:
              gLogger.error( 'Error removing replica in the FC for a non-existing file', res['Message'] )
              errorReasons.setdefault( str( res['Message'] ), [] ).append( lfn )
            else:
              for lfn, reason in res['Value']['Failed'].items():
                errorReasons.setdefault( str( reason ), [] ).append( lfn )
                lfnChunk.remove( lfn )
        if lfnChunk:
          res = rm.removeCatalogFile( lfnChunk )
          if not res['OK']:
            gLogger.error( "Error removing %d non-existing files from the FC" % len( lfnChunk ), res['Message'] )
            errorReasons.setdefault( str( res['Message'] ), [] ).extend( lfnChunk )
          else:
            for lfn, reason in res['Value']['Failed'].items():
              errorReasons.setdefault( str( reason ), [] ).append( lfn )
              lfnChunk.remove( lfn )
        notExistingRemoved += lfnChunk
    if verbose:
      print ''
    if notExistingRemoved:
      successfullyRemoved += notExistingRemoved
      gLogger.always( "Removed from FC %d non-existing files" % len( notExistingRemoved ) )

  if successfullyRemoved:
    gLogger.always( "Successfully removed %d files" % len( successfullyRemoved ) )
  for reason, lfns in errorReasons.items():
    gLogger.always( "Failed to remove %d files with error: %s" % ( len( lfns ), reason ) )
  DIRAC.exit( 0 )

