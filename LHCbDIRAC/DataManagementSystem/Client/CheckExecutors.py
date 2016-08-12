"""
Set of functions used by the DMS checking scripts
"""

__RCSID__ = "$Id$"

from DIRAC import gLogger
from DIRAC.Core.Utilities.List import breakListIntoChunks

def __removeFile( lfns ):
  if type( lfns ) == type( {} ):
    lfns = lfns.keys()
  elif type( lfns ) == type( '' ):
    lfns = [lfns]
  from LHCbDIRAC.DataManagementSystem.Client.ScriptExecutors import removeFiles
  removeFiles( lfns )

def __removeReplica( lfnDict ):
  seLFNs = {}
  for lfn in lfnDict:
    for se in lfnDict[lfn]:
      seLFNs.setdefault( se, [] ).append( lfn )
  from LHCbDIRAC.DataManagementSystem.Client.ScriptExecutors import removeReplicas
  for se, lfns in seLFNs.iteritems():
    removeReplicas( lfns, [se] )
  return seLFNs

def __replaceReplica( seLFNs ):
  if seLFNs:
    gLogger.notice( "Now replicating bad replicas..." )
    success = {}
    failed = {}
    for se, lfns in seLFNs.iteritems():
      for lfn in lfns:
        res = dm.replicateAndRegister( lfn, se )
        if res['OK']:
          success.update( res['Value']['Successful'] )
          failed.update( res['Value']['Failed'] )
        else:
          failed[lfn] = res['Message']

    failures = 0
    errors = {}
    for lfn, reason in failed.iteritems():
      reason = str( reason )
      errors[reason] = errors.setdefault( reason, 0 ) + 1
      failures += 1
    gLogger.notice( "\t%d success, %d failures%s" % ( len( success ), failures, ':' if failures else '' ) )
    for reason in errors:
      gLogger.notice( '\tError %s : %d files' % ( reason, errors[reason] ) )


def doCheckFC2SE( cc, bkCheck = True, fixIt = False, replace = False ):
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
    gLogger.notice( '>>>>' )
    affectedRuns = set( str( run ) for run in cc.existLFNsBKRepNo.itervalues() if run )
    if len( cc.existLFNsBKRepNo ) > maxFiles:
      prStr = ' (first %d)' % maxFiles
    else:
      prStr = ''
    gLogger.error( "%d files are in the FC but have replica = NO in BK%s:\nAffected runs: %s\n%s" %
                   ( len( cc.existLFNsBKRepNo ), prStr, ','.join( sorted( affectedRuns ) if affectedRuns else 'None' ),
                     '\n'.join( sorted( cc.existLFNsBKRepNo )[0:maxFiles] ) ) )
    if fixIt:
      gLogger.notice( "Going to fix them, setting the replica flag" )
      res = bk.addFiles( cc.existLFNsBKRepNo.keys() )
      if res['OK']:
        gLogger.notice( "\tSuccessfully added replica flag" )
      else:
        gLogger.error( 'Failed to set the replica flag', res['Message'] )
    else:
      gLogger.notice( "Use --FixIt to fix it (set the replica flag)" )
    gLogger.notice( '<<<<' )

  elif bkCheck:
    gLogger.notice( "No files in FC with replica = NO in BK -> OK!" )

  if cc.existLFNsNotInBK:
    gLogger.notice( '>>>>' )

    if len( cc.existLFNsNotInBK ) > maxFiles:
      prStr = ' (first %d)' % maxFiles
    else:
      prStr = ''
    gLogger.error( "%d files are in the FC but are NOT in BK%s:\n%s" %
                   ( len( cc.existLFNsNotInBK ), prStr,
                     '\n'.join( sorted( cc.existLFNsNotInBK[0:maxFiles] ) ) ) )
    if fixIt:
      gLogger.notice( "Going to fix them, by removing from the FC and storage" )
      __removeFile( cc.existLFNsNotInBK )
    else:
      gLogger.notice( "Use --FixIt to fix it (remove from FC and storage)" )
    gLogger.notice( '<<<<' )

  else:
    gLogger.notice( "No files in FC not in BK -> OK!" )

  seOK = True
  if cc.existLFNsNoSE:
    gLogger.notice( '>>>>' )

    seOK = False
    gLogger.error( "%d files are in the BK and FC but are missing at some SEs" % len( cc.existLFNsNoSE ) )
    fixStr = "removing them from catalogs" if not replace else "re-replicating them"
    for lfn in cc.existLFNsNoSE:
      gLogger.info( lfn )
    if fixIt:
      gLogger.notice( "Going to fix, " + fixStr )
      removeLfns = []
      removeReplicas = {}
      for lfn, ses in cc.existLFNsNoSE.iteritems():
        if ses == 'All':
          removeLfns.append( lfn )
        else:
          removeReplicas.setdefault( lfn, [] ).extend( ses )
      if removeLfns:
        __removeFile( removeLfns )
      if removeReplicas:
        seLFNs = __removeReplica( removeReplicas )
        if replace:
          __replaceReplica( seLFNs )
    else:
      if not replace:
        fixStr += "; use --Replace if you want to re-replicate them"
      gLogger.notice( "Use --FixIt to fix it (%s)" % fixStr )
    gLogger.notice( '<<<<' )

  else:
    gLogger.notice( "No missing replicas at sites -> OK!" )

  if cc.existLFNsBadFiles or cc.existLFNsNotExisting:
    gLogger.notice( '>>>>' )

    seOK = False
    if cc.existLFNsBadFiles:
      gLogger.error( "%d files have a bad checksum" % len( cc.existLFNsBadFiles ) )
    if cc.existLFNsNotExisting:
      gLogger.error( "%d files don't exist at any SE" % len( cc.existLFNsNotExisting ) )
    toRemove = sorted( set( cc.existLFNsBadFiles ) | set( cc.existLFNsNotExisting ) )
    for lfn in toRemove:
      gLogger.info( lfn )
    if fixIt:
      gLogger.notice( "Going to fix them, removing files from catalogs and storage" )
      __removeFile( toRemove )
    else:
      gLogger.notice( "Use --FixIt to fix it (remove files from SE and catalogs)" )
    gLogger.notice( '<<<<' )


  if cc.existLFNsBadReplicas:
    gLogger.notice( '>>>>' )

    seOK = False
    gLogger.error( "%d replicas have a bad checksum" % len( cc.existLFNsBadReplicas ) )
    for lfn, se in cc.existLFNsBadReplicas.iteritems():
      gLogger.info( '%s @ %s' % ( lfn, ','.join( sorted( se ) ) ) )
    fixStr = "remove replicas from SE and catalogs" if not replace else "re-replicating them"
    if fixIt:
      gLogger.notice( "Going to fix, " + fixStr )
      seLFNs = __removeReplica( cc.existLFNsBadReplicas )
      if replace:
        __replaceReplica( seLFNs )
    else:
      if not replace:
        fixStr += "; use --Replace if you want to re-replicate them"
      gLogger.notice( "Use --FixIt to fix it (%s)" % fixStr )
    gLogger.notice( '<<<<' )


  if not cc.existLFNsBadFiles and not cc.existLFNsBadReplicas:
    gLogger.notice( "No replicas have a bad checksum -> OK!" )
  if seOK:
    gLogger.notice( "All files exist and have a correct checksum -> OK!" )


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
    gLogger.notice( '>>>>' )

    affectedRuns = list( set( str( run ) for run in cc.existLFNsBKRepNo.itervalues() ) )
    gLogger.error( "%d files are in the FC but have replica = NO in BK" % len( cc.existLFNsBKRepNo ) )
    from LHCbDIRAC.DataManagementSystem.Client.ConsistencyChecks import ConsistencyChecks
    ccAux = ConsistencyChecks()
    gLogger.notice( "====== Now checking %d files from FC to SE ======" % len( cc.existLFNsBKRepNo ) )
    ccAux.lfns = cc.existLFNsBKRepNo.keys()
    doCheckFC2SE( ccAux, False, fixIt )
    cc.existLFNsBKRepNo = [lfn for lfn in cc.existLFNsBKRepNo if lfn not in ccAux.existLFNsNoSE and lfn not in ccAux.existLFNsBadFiles]
    if cc.existLFNsBKRepNo:
      if len( cc.existLFNsBKRepNo ) > maxFiles:
        prStr = ' (first %d)' % maxFiles
      else:
        prStr = ''
      gLogger.notice( "====== Completed, %d files still in the FC but have replica = NO in BK%s ======" % ( len( cc.existLFNsBKRepNo ), prStr ) )
      gLogger.error( '\n'.join( sorted( cc.existLFNsBKRepNo )[0:maxFiles] ) )
      if listAffectedRuns:
        gLogger.notice( 'Affected runs: %s' % ','.join( affectedRuns ) )
      if fixIt:
        gLogger.notice( "Going to fix them, setting the replica flag" )
        res = bk.addFiles( cc.existLFNsBKRepNo )
        if res['OK']:
          gLogger.notice( "\tSuccessfully added replica flag" )
        else:
          gLogger.error( 'Failed to set the replica flag', res['Message'] )
      else:
        gLogger.notice( "Use --FixIt to fix it (set the replica flag)" )
    else:
      gLogger.notice( "====== Completed, no files in the FC with replica = NO in BK%s ======" )
    gLogger.notice( '<<<<' )

  else:
    gLogger.notice( "No files in FC with replica = NO in BK -> OK!" )

  if cc.existLFNsNotInBK:
    gLogger.notice( '>>>>' )

    if len( cc.existLFNsNotInBK ) > maxFiles:
      prStr = ' (first %d)' % maxFiles
    else:
      prStr = ''
    gLogger.error( "%d files are in the FC but are NOT in BK%s:\n%s" %
                   ( len( cc.existLFNsNotInBK ), prStr,
                     '\n'.join( sorted( cc.existLFNsNotInBK[0:maxFiles] ) ) ) )
    if fixIt:
      gLogger.notice( "Going to fix them, by removing from the FC and storage" )
      __removeFile( cc.existLFNsNotInBK )
    else:
      gLogger.notice( "Use --FixIt to fix it (remove from FC and storage)" )
    gLogger.notice( '<<<<' )

  else:
    gLogger.notice( "No files in FC not in BK -> OK!" )

def doCheckBK2FC( cc, checkAll = False, fixIt = False ):
  """
  Method actually calling for the the check using ConsistencyChecks module
  It prints out results and calls corrective actions if required
  """
  global bk
  bk = cc.bkClient
  cc.checkBK2FC( checkAll )
  maxPrint = 20
  chunkSize = 100

  if checkAll:
    if cc.existLFNsBKRepNo:
      gLogger.notice( '>>>>' )

      nFiles = len( cc.existLFNsBKRepNo )
      comment = "%d files are in the FC but have replica = NO in BK" % nFiles
      if nFiles > maxPrint:
        comment += ' (first %d LFNs) : \n' % maxPrint
      comment += '\n'.join( cc.existLFNsBKRepNo[:maxPrint] )
      gLogger.error( comment )
      if fixIt:
        gLogger.notice( "Setting the replica flag..." )
        nFiles = 0
        for lfnChunk in breakListIntoChunks( cc.existLFNsBKRepNo, chunkSize ):
          res = bk.addFiles( lfnChunk )
          if not res['OK']:
            gLogger.error( "Something wrong: %s" % res['Message'] )
          else:
            nFiles += len( lfnChunk )
        gLogger.notice( "Successfully set replica flag to %d files" % nFiles )
      else:
        gLogger.notice( "Use option --FixIt to fix it (set the replica flag)" )
      gLogger.notice( '<<<<' )

    else:
      gLogger.notice( "No LFNs exist in the FC but have replicaFlag = No in the BK -> OK!" )

  if cc.absentLFNsBKRepYes:
    gLogger.notice( '>>>>' )

    nFiles = len( cc.absentLFNsBKRepYes )
    comment = "%d files have replicaFlag = Yes but are not in FC:" % nFiles
    if nFiles > maxPrint:
      comment += ' (first %d LFNs) : \n' % maxPrint
    comment += '\n'.join( cc.absentLFNsBKRepYes[:maxPrint] )
    gLogger.error( comment )
    if fixIt:
      gLogger.notice( "Removing the replica flag..." )
      nFiles = 0
      for lfnChunk in breakListIntoChunks( cc.absentLFNsBKRepYes, chunkSize ):
        res = bk.removeFiles( lfnChunk )
        if not res['OK']:
          gLogger.error( "Something wrong:", res['Message'] )
        else:
          nFiles += len( lfnChunk )
      gLogger.notice( "Successfully removed replica flag to %d files" % nFiles )
    else:
      gLogger.notice( "Use option --FixIt to fix it (remove the replica flag)" )
    gLogger.notice( '<<<<' )

  else:
    gLogger.notice( "No LFNs have replicaFlag = Yes but are not in the FC -> OK!" )

def doCheckSE( cc, seList, fixIt = False ):
  cc.checkSE( seList )
  dm = cc.dm

  if cc.absentLFNsInFC:
    gLogger.notice( '>>>>' )
    gLogger.notice( '%d files are not in the FC' % len( cc.absentLFNsInFC ) )
    if fixIt:
      __removeFile( cc.absentLFNsInFC )
    else:
      gLogger.notice( "Use --FixIt to fix it (remove from catalogs" )
    gLogger.notice( '<<<<' )

  if cc.existLFNsNoSE:
    gLogger.notice( '<<<<' )
    gLogger.notice( '%d files are not present at %s' % ( len( cc.existLFNsNoSE ), ', '.join( sorted( seList ) ) ) )
    gLogger.notice( '\n'.join( sorted( cc.existLFNsNoSE ) ) )
  else:
    gLogger.notice( 'No LFNs missing at %s' % ', '.join( sorted( seList ) ) )

