#!/usr/bin/env python
'''
    Check if the files are in the BK, the LFC and the SEs they are supposed to be in.

    Uses the DM script switches, and, unless a list of LFNs is provided:
    1) If --Directory is used: get files in FC directories
    2) If --Production or --BK options is used get files in the FC directories from the BK

    If --FixIt is set, takes actions:
      Missing files: remove from SE and FC
      No replica flag: set it (in the BK)
      Not existing in SE: remove replica or file from the catalog
      Bad checksum: remove replica or file from SE and catalogs if no good replica
'''
__RCSID__ = "$Id: dirac-dms-check-fc2se.py 69455 2013-08-14 08:42:29Z phicharp $"

def __removeFile( lfns ):
  if type( lfns ) == type( {} ):
    lfns = lfns.keys()
  elif type( lfns ) == type( '' ):
    lfns = [lfns]
  success = 0
  failures = 0
  errors = {}
  chunkSize = 1000
  from DIRAC.Core.Utilities.List import breakListIntoChunks
  import sys
  writeDots = len( lfns ) > 3 * chunkSize
  if writeDots:
    sys.stdout.write( "Removing %d files (chunks of %d): " % ( len( lfns ), chunkSize ) )
  for lfnChunk in breakListIntoChunks( lfns, chunkSize ):
    if writeDots:
      sys.stdout.write( '.' )
      sys.stdout.flush()
    res = dm.removeFile( lfnChunk )
    if res['OK']:
      success += len( res['Value']['Successful'] )
      for reason in res['Value']['Failed'].values():
        reason = str( reason )
        if reason != "{'BookkeepingDB': 'File does not exist'}":
          errors[reason] = errors.setdefault( reason, 0 ) + 1
          failures += 1
    else:
      failures += len( lfnChunk )
      reason = res['Message']
      errors[reason] = errors.setdefault( reason, 0 ) + len( lfnChunk )
  if writeDots:
    gLogger.always( '' )
  gLogger.always( "\t%d success, %d failures%s" % ( success, failures, ':' if failures else '' ) )
  for reason in errors:
    gLogger.always( '\tError %s : %d files' % ( reason, errors[reason] ) )

def __removeReplica( lfnDict ):
  seLFNs = {}
  for lfn in lfnDict:
    for se in lfnDict[lfn]:
      seLFNs.setdefault( se, [] ).append( lfn )
  for se, lfns in seLFNs.items():
    res = dm.removeReplica( se, lfns )
    if res['OK']:
      success = len( res['Value']['Successful'] )
      failures = 0
      errors = {}
      for lfn, reason in res['Value']['Failed'].items():
        reason = str( reason )
        if reason != "{'BookkeepingDB': 'File does not exist'}":
          seLFNs[se].remove( lfn )
          errors[reason] = errors.setdefault( reason, 0 ) + 1
          failures += 1
      gLogger.always( "\t%d success, %d failures%s" % ( success, failures, ':' if failures else '' ) )
      for reason in errors:
        gLogger.always( '\tError %s : %d files' % ( reason, errors[reason] ) )
    else:
      gLogger.fatal( 'Fully failed removing replicas', res['Message'] )
      return {}
  return seLFNs

def __replaceReplica( lfnDict ):
  seLFNs = __removeReplica( lfnDict )
  if seLFNs:
    gLogger.always( "Now replicating bad replicas..." )
  for se, lfns in seLFNs.items():
    res = dm.replicateAndRegister( lfns, se )
    if res['OK']:
      success = len( res['Value']['Successful'] )
      failures = 0
      errors = {}
      for lfn, reason in res['Value']['Failed'].items():
        reason = str( reason )
        errors[reason] = errors.setdefault( reason, 0 ) + 1
        failures += 1
      gLogger.always( "\t%d success, %d failures%s" % ( success, failures, ':' if failures else '' ) )
      for reason in errors:
        gLogger.always( '\tError %s : %d files' % ( reason, errors[reason] ) )
    else:
      gLogger.fatal( 'Fully failed replacing replicas', res['Message'] )


def doCheck( bkCheck ):
  """
  Method actually calling for the the check using ConsistencyChecks module
  It prints out results and calls corrective actions if required
  """
  cc.checkFC2SE( bkCheck )

  maxFiles = 20
  if cc.existLFNsBKRepNo:
    affectedRuns = []
    for run in cc.existLFNsBKRepNo.values():
      if run not in affectedRuns:
        affectedRuns.append( str( run ) )
    if len( cc.existLFNsBKRepNo ) > maxFiles:
      prStr = ' (first %d)' % maxFiles
    else:
      prStr = ''
    gLogger.error( "%d files are in the FC but have replica = NO in BK%s:\nAffected runs: %s\n%s" %
                   ( len( cc.existLFNsBKRepNo ), prStr, ','.join( affectedRuns ),
                     '\n'.join( sorted( cc.existLFNsBKRepNo )[0:maxFiles] ) ) )
    if fixIt:
      gLogger.always( "Going to fix them, setting the replica flag" )
      res = bk.addFiles( cc.existLFNsBKRepNo.keys() )
      if res['OK']:
        gLogger.always( "\tSuccessfully added replica flag" )
      else:
        gLogger.error( 'Failed to set the replica flag', res['Message'] )
    else:
      gLogger.always( "Use --FixIt to fix it (set the replica flag)" )
  else:
    gLogger.always( "No files in FC with replica = NO in BK -> OK!" )

  if cc.existLFNsNotInBK:
    if len( cc.existLFNsNotInBK ) > maxFiles:
      prStr = ' (first %d)' % maxFiles
    else:
      prStr = ''
    gLogger.error( "%d files are in the FC but are NOT in BK%s:\n%s" %
                   ( len( cc.existLFNsNotInBK ), prStr,
                     '\n'.join( sorted( cc.existLFNsNotInBK[0:maxFiles] ) ) ) )
    if fixIt:
      gLogger.always( "Going to fix them, by removing from the FC and storage" )
      __removeFile( cc.existLFNsNotInBK )
    else:
      gLogger.always( "Use --FixIt to fix it (remove from FC and storage)" )
  else:
    gLogger.always( "No files in FC not in BK -> OK!" )

  seOK = True
  if cc.existLFNsNoSE:
    seOK = False
    gLogger.error( "%d files are in the BK and FC but do not exist on at least one SE" % len( cc.existLFNsNoSE ) )
    if fixIt:
      gLogger.always( "Going to fix them, removing from catalogs" )
      removeLfns = []
      removeReplicas = {}
      for lfn, ses in cc.existLFNsNoSE.items():
        if ses == 'All':
          removeLfns.append( lfn )
        else:
          removeReplicas.setdefault( lfn, [] ).extend( ses )
      if removeLfns:
        __removeFile( removeLfns )
      if removeReplicas:
        __replaceReplica( removeReplicas )
    else:
      gLogger.always( "Use --FixIt to fix it (remove from catalogs)" )
  else:
    gLogger.always( "No missing replicas at sites -> OK!" )

  if cc.existLFNsBadFiles:
    seOK = False
    gLogger.error( "%d files have a bad checksum" % len( cc.existLFNsBadFiles ) )
    if fixIt:
      gLogger.always( "Going to fix them, removing from catalogs and storage" )
      __removeFile( cc.existLFNsBadFiles )
    else:
      gLogger.always( "Use --FixIt to fix it (remove from SE and catalogs)" )

  if cc.existLFNsBadReplicas:
    seOK = False
    gLogger.error( "%d files have a bad checksum" % len( cc.existLFNsBadReplicas ) )
    if fixIt:
      gLogger.always( "Going to fix them, removing from catalogs and storage" )
      __replaceReplica( cc.existLFNsBadReplicas )
    else:
      gLogger.always( "Use --FixIt to fix it (remove from SE and FC)" )

  if not cc.existLFNsBadFiles and not cc.existLFNsBadReplicas:
    gLogger.always( "No replicas have a bad checksum -> OK!" )
  if seOK:
    gLogger.always( "All files exist and have a correct checksum -> OK!" )


if __name__ == '__main__':

  # Script initialization
  from DIRAC.Core.Base import Script
  from LHCbDIRAC.DataManagementSystem.Client.DMScript import DMScript

  Script.setUsageMessage( '\n'.join( [ __doc__,
                                       'Usage:',
                                       '  %s [option|cfgfile] [values]' % Script.scriptName, ] ) )
  dmScript = DMScript()
  dmScript.registerNamespaceSwitches()  # Directory
  dmScript.registerFileSwitches()  # File, LFNs
  dmScript.registerBKSwitches()
  Script.registerSwitch( '', 'FixIt', '   Take action to fix the catalogs and storage' )
  Script.registerSwitch( '', 'NoBK', '   Do not check with BK' )
  Script.parseCommandLine( ignoreErrors = True )

  # imports
  from DIRAC import gLogger
  from DIRAC.DataManagementSystem.Client.DataManager import DataManager
  from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
  from LHCbDIRAC.DataManagementSystem.Client.ConsistencyChecks import ConsistencyChecks

  fixIt = False
  bkCheck = True
  for switch in Script.getUnprocessedSwitches():
    if switch[0] == 'FixIt':
      fixIt = True
    elif switch[0] == 'NoBK':
      bkCheck = False

  dm = DataManager()
  bk = BookkeepingClient()

  cc = ConsistencyChecks( dm = dm, bkClient = bk )
  cc.directories = dmScript.getOption( 'Directory', [] )
  cc.lfns = dmScript.getOption( 'LFNs', [] ) + [lfn for arg in Script.getPositionalArgs() for lfn in arg.split( ',' )]
  bkQuery = dmScript.getBKQuery( visible = 'All' )
  if bkQuery.getQueryDict() != {'Visible': 'All'}:
    bkQuery.setOption( 'ReplicaFlag', 'All' )
    cc.bkQuery = bkQuery

  doCheck( bkCheck )
