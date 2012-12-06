#!/usr/bin/env python
########################################################################
"""
  Remove the given file or a list of files from the File Catalog and from the storage
"""
__RCSID__ = "$Id$"

from DIRAC.Core.Base import Script
from LHCbDIRAC.DataManagementSystem.Client.DMScript import DMScript

if __name__ == "__main__":
  dmScript = DMScript()
  dmScript.registerFileSwitches()

  Script.registerSwitch( '', 'FixTransformations', '   Allows to set the files as Removed in all transformations' )
  Script.setUsageMessage( '\n'.join( [ __doc__,
                                       'Usage:',
                                       '  %s [option|cfgfile] [<LFN>] [<LFN>...]' % Script.scriptName, ] ) )

  Script.parseCommandLine()

  lfns = dmScript.getOption( 'LFNs', [] )
  lfns += Script.getPositionalArgs()
  lfnList = []
  for lfn in lfns:
    try:
      f = open( lfn, 'r' )
      lfnList += [l.split( 'LFN:' )[-1].strip().split()[0].replace( '"', '' ).replace( ',', '' ) for l in f.read().splitlines()]
      f.close()
    except:
      lfnList.append( lfn )

  fixTrans = False
  switches = Script.getUnprocessedSwitches()
  for switch in switches:
    if switch[0] == 'FixTransformations':
      fixTrans = True
  import sys, os
  import DIRAC
  from DIRAC import gLogger

  from DIRAC.Core.Utilities.List import sortList, breakListIntoChunks
  from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager
  rm = ReplicaManager()
  if fixTrans:
    from LHCbDIRAC.TransformationSystem.Client.TransformationClient import TransformationClient
    transClient = TransformationClient()

  errorReasons = {}
  successfullyRemoved = []
  notExisting = []
  lfnsToSet = {}
  # Avoid spurious error messages
  gLogger.setLevel( 'FATAL' )
  for lfnChunk in breakListIntoChunks( lfnList, 100 ):
    res = rm.removeFile( lfnChunk )
    if not res['OK']:
      gLogger.fatal( "Failed to remove data", res['Message'] )
      DIRAC.exit( -2 )
    for lfn, reason in res['Value']['Failed'].items():
      reason = str( reason )
      if 'No such file or directory' in reason:
        notExisting.append( lfn )
      else:
        errorReasons.setdefault( reason, [] ).append( lfn )
    successfullyRemoved += res['Value']['Successful'].keys()
  gLogger.setLevel( 'ERROR' )

  if fixTrans and successfullyRemoved + notExisting:
    res = transClient.getTransformationFiles( {'LFN':successfullyRemoved + notExisting } )
      if not res['OK']:
        gLogger.error( "Error getting transformation files", res['Message'] )
      else:
        for fileDict in [fileDict for fileDict in res['Value'] if fileDict['Status'] not in ( 'Processed', 'Removed' )]:
          lfnsToSet.setdefault( fileDict['TransformationID'], [] ).append( fileDict['LFN'] )
  # If required, set files Removed in transformations
  for transID, lfns in lfnsToSet.items():
    res = transClient.setFileStatusForTransformation( transID, 'Removed', lfns )
    if not res['OK']:
      gLogger.error( 'Error setting %d files to Removed' % len( lfns ), res['Message'] )
    else:
      gLogger.always( 'Successfully set %d files as Removed in transformation %d' % ( len( lfns ), transID ) )

  if notExisting:
    # The files are not yet removed from the catalog!! :(((
    res = rm.removeCatalogFile( notExisting )
    if not res['OK']:
      gLogger.error( "Error removing %d non-existing files from the FC" % len( notExisting ), res['Message'] )
    else:
      for lfn, reason in res['Value']['Failed'].items():
        errorReasons.setdefault( reason, [] ).append( lfn )
        notExisting.remove( lfn )
    gLogger.always( "Already removed: %d files" % len( notExisting ) )

  if successfullyRemoved:
    gLogger.always( "Successfully removed %d files" % len( successfullyRemoved ) )
  for reason, lfns in errorReasons.items():
    gLogger.always( "Failed to remove %d files with error: %s" % ( len( lfns ), reason ) )
  DIRAC.exit( 0 )

