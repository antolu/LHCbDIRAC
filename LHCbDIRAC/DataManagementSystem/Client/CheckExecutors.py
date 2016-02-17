"""
Set of functions used by the DMS checking scripts
"""

__RCSID__ = "$Id$"

from DIRAC import gLogger
from DIRAC.Core.Utilities.List import breakListIntoChunks

def __removeFile( lfns ):
  if isinstance( lfns, dict ):
    lfns = lfns.keys()
  elif isinstance( lfns, basestring ):
    lfns = [lfns]
  success = 0
  failures = 0
  errors = {}
  chunkSize = 1000
  from DIRAC.Core.Utilities.List import breakListIntoChunks
  import sys
  writeDots = len( lfns ) > 3 * chunkSize
  if writeDots:
    sys.stdout.write( "Removing completely %d files (chunks of %d): " % ( len( lfns ), chunkSize ) )
  else:
    gLogger.always( "Removing completely %d files" % len( lfns ) )

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
  chunkSize = 1000
  from DIRAC.Core.Utilities.List import breakListIntoChunks
  import sys
  for lfn in lfnDict:
    for se in lfnDict[lfn]:
      seLFNs.setdefault( se, [] ).append( lfn )
  success = 0
  failures = 0
  errors = {}
  writeDots = len( lfnDict ) > 3 * chunkSize
  if writeDots:
    sys.stdout.write( "Removing  %d replicas (chunks of %d) at %d SEs: " % ( len( lfnDict ), chunkSize, len( seLFNs ) ) )
  else:
    gLogger.always( "Removing  %d replicas at %d SEs" % ( len( lfnDict ), len( seLFNs ) ) )
  for se, lfns in seLFNs.items():
    for lfnChunk in breakListIntoChunks( lfns, chunkSize ):
      if writeDots:
        sys.stdout.write( '.' )
        sys.stdout.flush()
      res = dm.removeReplica( se, lfnChunk )
      if res['OK']:
        success += len( res['Value']['Successful'] )
        for lfn, reason in res['Value']['Failed'].items():
          reason = str( reason )
          if reason != "{'BookkeepingDB': 'File does not exist'}":
            seLFNs[se].remove( lfn )
            errors[reason] = errors.setdefault( reason, 0 ) + 1
            failures += 1
      else:
        failures += len( lfnChunk )
        for lfn in lfnChunk:
          seLFNs[se].remove( lfn )
        reason = res['Message']
        errors[reason] = errors.setdefault( reason, 0 ) + len( lfnChunk )
  if writeDots:
    gLogger.always( '' )

  gLogger.always( "\t%d success, %d failures%s" % ( success, failures, ':' if failures else '' ) )
  for reason in errors:
    gLogger.always( '\tError %s : %d files' % ( reason, errors[reason] ) )
  return seLFNs

def __replaceReplica( seLFNs ):
  if seLFNs:
    gLogger.always( "Now replicating bad replicas..." )
    success = {}
    failed = {}
    for se, lfns in seLFNs.items():
      for lfn in lfns:
        res = dm.replicateAndRegister( lfn, se )
        if res['OK']:
          success.update( res['Value']['Successful'] )
          failed.update( res['Value']['Failed'] )
        else:
          failed[lfn] = res['Message']

    failures = 0
    errors = {}
    for lfn, reason in failed.items():
      reason = str( reason )
      errors[reason] = errors.setdefault( reason, 0 ) + 1
      failures += 1
    gLogger.always( "\t%d success, %d failures%s" % ( len( success ), failures, ':' if failures else '' ) )
    for reason in errors:
      gLogger.always( '\tError %s : %d files' % ( reason, errors[reason] ) )


def doCheckFC2SE( cc, bkCheck = True, fixIt = False, noReplace = True ):
  """
  Method actually calling for the the check using ConsistencyChecks module
  It prints out results and calls corrective actions if required
  """
  global dm, bk
  dm = cc.dm
  bk = cc.bkClient
  cc.checkFC2SE( bkCheck )

  maxFiles = 20
  if cc.existLFNsBKRepNo:
    gLogger.always( '>>>>' )
    affectedRuns = set( [str( run ) for run in cc.existLFNsBKRepNo.values() if run] )
    if len( cc.existLFNsBKRepNo ) > maxFiles:
      prStr = ' (first %d)' % maxFiles
    else:
      prStr = ''
    gLogger.error( "%d files are in the FC but have replica = NO in BK%s:\nAffected runs: %s\n%s" %
                   ( len( cc.existLFNsBKRepNo ), prStr, ','.join( sorted( affectedRuns ) if affectedRuns else 'None' ),
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
    gLogger.always( '<<<<' )

  elif bkCheck:
    gLogger.always( "No files in FC with replica = NO in BK -> OK!" )

  if cc.existLFNsNotInBK:
    gLogger.always( '>>>>' )

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
    gLogger.always( '<<<<' )

  else:
    gLogger.always( "No files in FC not in BK -> OK!" )

  seOK = True
  if cc.existLFNsNoSE:
    gLogger.always( '>>>>' )

    seOK = False
    gLogger.error( "%d files are in the BK and FC but do not exist on at least one SE" % len( cc.existLFNsNoSE ) )
    fixStr = "removing them from catalogs" if noReplace else "re-replicating them"
    if fixIt:
      gLogger.always( "Going to fix, " + fixStr )
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
        seLFNs = __removeReplica( removeReplicas )
        if not noReplace:
          __replaceReplica( seLFNs )
    else:
      if not noReplace:
        fixStr += "; use --NoReplace if you want to only remove them from catalogs"
      gLogger.always( "Use --FixIt to fix it (%s)" % fixStr )
    gLogger.always( '<<<<' )

  else:
    gLogger.always( "No missing replicas at sites -> OK!" )

  if cc.existLFNsBadFiles:
    gLogger.always( '>>>>' )

    seOK = False
    gLogger.error( "%d files have a bad checksum" % len( cc.existLFNsBadFiles ) )
    if fixIt:
      gLogger.always( "Going to fix them, removing from catalogs and storage" )
      __removeFile( cc.existLFNsBadFiles )
    else:
      gLogger.always( "Use --FixIt to fix it (remove from SE and catalogs)" )
    gLogger.always( '<<<<' )


  if cc.existLFNsBadReplicas:
    gLogger.always( '>>>>' )

    seOK = False
    gLogger.error( "%d replicas have a bad checksum" % len( cc.existLFNsBadReplicas ) )
    fixStr = "removing them from catalogs and SE" if noReplace else "re-replicating them"
    if fixIt:
      gLogger.always( "Going to fix, " + fixStr )
      seLFNs = __removeReplica( cc.existLFNsBadReplicas )
      if not noReplace:
        __replaceReplica( seLFNs )
    else:
      if not noReplace:
        fixStr += "; use --NoReplace if you want to only remove them from catalogs and SE"
      gLogger.always( "Use --FixIt to fix it (%s)" % fixStr )
    gLogger.always( '<<<<' )


  if not cc.existLFNsBadFiles and not cc.existLFNsBadReplicas:
    gLogger.always( "No replicas have a bad checksum -> OK!" )
  if seOK:
    gLogger.always( "All files exist and have a correct checksum -> OK!" )


def doCheckFC2BK( cc, fixIt = False, listAffectedRuns = False ):
  """
  Method actually calling for the the check using ConsistencyChecks module
  It prints out results and calls corrective actions if required
  """
  global dm, bk
  dm = cc.dm
  bk = cc.bkClient
  cc.checkFC2BK()

  maxFiles = 20
  if cc.existLFNsBKRepNo:
    gLogger.always( '>>>>' )

    affectedRuns = list( set( [str( run ) for run in cc.existLFNsBKRepNo.values()] ) )
    gLogger.error( "%d files are in the FC but have replica = NO in BK" % len( cc.existLFNsBKRepNo ) )
    from LHCbDIRAC.DataManagementSystem.Client.ConsistencyChecks import ConsistencyChecks
    ccAux = ConsistencyChecks()
    gLogger.always( "====== Now checking %d files from FC to SE ======" % len( cc.existLFNsBKRepNo ) )
    ccAux.lfns = cc.existLFNsBKRepNo.keys()
    doCheckFC2SE( ccAux, False, fixIt )
    cc.existLFNsBKRepNo = [lfn for lfn in cc.existLFNsBKRepNo if lfn not in ccAux.existLFNsNoSE and lfn not in ccAux.existLFNsBadFiles]
    if cc.existLFNsBKRepNo:
      if len( cc.existLFNsBKRepNo ) > maxFiles:
        prStr = ' (first %d)' % maxFiles
      else:
        prStr = ''
      gLogger.always( "====== Completed, %d files still in the FC but have replica = NO in BK%s ======" % ( len( cc.existLFNsBKRepNo ), prStr ) )
      gLogger.error( '\n'.join( sorted( cc.existLFNsBKRepNo )[0:maxFiles] ) )
      if listAffectedRuns:
        gLogger.always( 'Affected runs: %s' % ','.join( affectedRuns ) )
      if fixIt:
        gLogger.always( "Going to fix them, setting the replica flag" )
        res = bk.addFiles( cc.existLFNsBKRepNo )
        if res['OK']:
          gLogger.always( "\tSuccessfully added replica flag" )
        else:
          gLogger.error( 'Failed to set the replica flag', res['Message'] )
      else:
        gLogger.always( "Use --FixIt to fix it (set the replica flag)" )
    else:
      gLogger.always( "====== Completed, no files in the FC with replica = NO in BK%s ======" )
    gLogger.always( '<<<<' )

  else:
    gLogger.always( "No files in FC with replica = NO in BK -> OK!" )

  if cc.existLFNsNotInBK:
    gLogger.always( '>>>>' )

    if len( cc.existLFNsNotInBK ) > maxFiles:
      prStr = ' (first %d)' % maxFiles
    else:
      prStr = ''
    gLogger.error( "%d files are in the FC but are NOT in BK%s:\n%s" %
                   ( len( cc.existLFNsNotInBK ), prStr,
                     '\n'.join( sorted( cc.existLFNsNotInBK[0:maxFiles] ) ) ) )
    if fixIt:
      gLogger.always( "Going to fix them, by removing from the FC and storage" )
      errors = {}
      success = 0
      failures = 0
      maxRemove = 100
      import sys
      chunkSize = min( maxRemove, max( 1, len( cc.existLFNsNotInBK ) / 2 ) )
      if len( cc.existLFNsNotInBK ) > maxRemove:
        sys.stdout.write( 'Remove by chunks of %d files ' % chunkSize )
        dots = True
      else:
        dots = False
      for lfns in breakListIntoChunks( cc.existLFNsNotInBK, chunkSize ):
        if dots:
          sys.stdout.write( '.' )
          sys.stdout.flush()
        res = dm.removeFile( lfns )
        if res['OK']:
          success += len( res['Value']['Successful'] )
          for lfn, reason in res['Value']['Failed'].items():
            reason = str( reason )
            if reason != "{'BookkeepingDB': 'File does not exist'}":
              errors[reason] = errors.setdefault( reason, 0 ) + 1
              failures += 1
            elif lfn not in res['Value']['Successful']:
              success += 1
        else:
          reason = res['Message']
          failures += len( lfns )
          errors[reason] = errors.setdefault( reason, 0 ) + len( lfns )
      gLogger.always( "\t%d success, %d failures%s" % ( success, failures, ':' if errors else '' ) )
      for reason in errors:
        gLogger.always( '\tError %s : %d files' % ( reason, errors[reason] ) )
    else:
      gLogger.always( "Use --FixIt to fix it (remove from FC and storage)" )
    gLogger.always( '<<<<' )

  else:
    gLogger.always( "No files in FC not in BK -> OK!" )

def doCheckBK2FC( cc, checkAll = False, fixIt = False ):
  """
  Method actually calling for the the check using ConsistencyChecks module
  It prints out results and calls corrective actions if required
  """
  global bk
  bk = cc.bkClient
  cc.checkBK2FC( checkAll )
  maxPrint = 20
  chunkSize = 10000

  if checkAll:
    if cc.existLFNsBKRepNo:
      gLogger.always( '>>>>' )

      nFiles = len( cc.existLFNsBKRepNo )
      comment = "%d files are in the FC but have replica = NO in BK" % nFiles
      if nFiles <= maxPrint:
        comment = ''
      else:
        comment = ' (first %d LFNs) : %s' % maxPrint
      comment += '\n'.join( cc.existLFNsBKRepNo[:maxPrint] )
      gLogger.error( comment )
      if fixIt:
        gLogger.always( "Setting the replica flag..." )
        nFiles = 0
        for lfnChunk in breakListIntoChunks( cc.existLFNsBKRepNo, chunkSize ):
          res = bk.addFiles( lfnChunk )
          if not res['OK']:
            gLogger.error( "Something wrong: %s" % res['Message'] )
          else:
            nFiles += len( lfnChunk )
        gLogger.always( "Successfully set replica flag to %d files" % nFiles )
      else:
        gLogger.always( "Use option --FixIt to fix it (set the replica flag)" )
      gLogger.always( '<<<<' )

    else:
      gLogger.always( "No LFNs exist in the FC but have replicaFlag = No in the BK -> OK!" )

  if cc.absentLFNsBKRepYes:
    gLogger.always( '>>>>' )

    nFiles = len( cc.absentLFNsBKRepYes )
    if nFiles <= maxPrint:
      comment = str( cc.absentLFNsBKRepYes )
    else:
      comment = ' (first %d LFNs) : %s' % ( maxPrint, str( cc.absentLFNsBKRepYes[:maxPrint] ) )
    if fixIt:
      gLogger.always( "Removing the replica flag to %d files: %s" % ( nFiles, comment ) )
      nFiles = 0
      for lfnChunk in breakListIntoChunks( cc.absentLFNsBKRepYes, chunkSize ):
        res = bk.removeFiles( lfnChunk )
        if not res['OK']:
          gLogger.error( "Something wrong:", res['Message'] )
        else:
          nFiles += len( lfnChunk )
      gLogger.always( "Successfully removed replica flag to %d files" % nFiles )
    else:
      gLogger.error( "%d files have replicaFlag = Yes but are not in FC: %s" % ( nFiles, comment ) )
      gLogger.always( "Use option --FixIt to fix it (remove the replica flag)" )
    gLogger.always( '<<<<' )

  else:
    gLogger.always( "No LFNs have replicaFlag = Yes but are not in the FC -> OK!" )

def doCheckSE( cc, seList, fixIt = False ):
  cc.checkSE( seList )
  dm = cc.dm

  if cc.absentLFNsInFC:
    gLogger.always( '>>>>' )
    gLogger.always( '%d files are not in the FC' % len( cc.absentLFNsInFC ) )
    if fixIt:
      __removeFile( cc.absentLFNsInFC )
    else:
      gLogger.always( "Use --FixIt to fix it (remove from catalogs" )
    gLogger.always( '<<<<' )

  if cc.existLFNsNoSE:
    gLogger.always( '<<<<' )
    gLogger.always( '%d files are not present at %s' % ( len( cc.existLFNsNoSE ), ', '.join( sorted( seList ) ) ) )
    gLogger.always( '\n'.join( sorted( cc.existLFNsNoSE ) ) )
  else:
    gLogger.always( 'No LFNs missing at %s' % ', '.join( sorted( seList ) ) )

