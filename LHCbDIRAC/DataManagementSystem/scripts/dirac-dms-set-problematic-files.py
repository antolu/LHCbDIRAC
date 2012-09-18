#!/usr/bin/env python
"""
    Set a (set of) LFNs as problematic in the LFC and in the BK and transformation system if only one replica
"""
__RCSID__ = "$Id: dirac-dms-set-problematic-files.py 22056 2010-02-23 18:41:29Z rgracian $"
__VERSION__ = "$Revision: 1.1 $"

from DIRAC.Core.Base import Script
from LHCbDIRAC.DataManagementSystem.Client.DMScript import DMScript
import DIRAC
from DIRAC import gLogger
import sys, time

if __name__ == "__main__":

  dmScript = DMScript()
  dmScript.registerFileSwitches()
  dmScript.registerSiteSwitches()

  Script.registerSwitch( '', 'Reset', '   Reset files to OK' )
  Script.registerSwitch( '', 'Full', '   Give full list of files' )
  Script.registerSwitch( '', 'NoAction', '   No action taken, just give stats' )

  Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                       'Usage:',
                                       '  %s [option|cfgfile] [<LFN>] [<LFN>...]' % Script.scriptName, ] ) )

  Script.parseCommandLine( ignoreErrors=False )

  startTime = time.time()

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

  targetSEs = dmScript.getOption( 'SEs', [] )
  if not targetSEs:
    sites = dmScript.getOption( 'Sites', '' )
    if type( sites ) != type( [] ):
      sites = [sites]
    if not sites:
      print "Site or SE switch is mandatory"
      Script.showHelp()
      DIRAC.exit( 1 )
    from DIRAC.Core.Utilities.SiteSEMapping                                import getSEsForSite
    targetSEs = []
    for site in sites:
      res = getSEsForSite( site )
      if not res['OK']:
        print "Can't get SEs associated to %s:" % site, res['Message']
        DIRAC.exit( 1 )
      targetSEs += res['Value']
  lfns = dmScript.getOption( 'LFNs', [] )
  lfns += Script.getPositionalArgs()
  file = dmScript.getOption( 'File', '' )
  if not file:
    lfns.append( file )
  lfnList = []
  for lfn in lfns:
    try:
      f = open( lfn, 'r' )
      lfnList += [l.split( 'LFN:' )[-1].strip().split()[-1].replace( '"', '' ).replace( ',', '' ) for l in f.read().splitlines()]
      f.close()
    except:
      lfnList.append( lfn )

  lfns = []
  userFiles = 0
  for lfn in lfnList:
    if lfn.find( '/user/' ) >= 0:
      userFiles += 1
      continue
    i = lfn.find( '/lhcb' )
    if i >= 0:
      lfns.append( lfn[i:] )

  from DIRAC.DataManagementSystem.Client.ReplicaManager                  import ReplicaManager
  rm = ReplicaManager()
  from LHCbDIRAC.TransformationSystem.Client.TransformationClient        import TransformationClient
  tr = TransformationClient()
  from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient  import BookkeepingClient
  bk = BookkeepingClient()
  from DIRAC.Core.Utilities.List                                         import breakListIntoChunks

  if len( lfns ) == 0:
    gLogger.always( "There are no files to process... check parameters..." )
    DIRAC.exit( 0 )
  gLogger.always( "Now processing %d files" % len( lfns ) )
  chunkSize = 1000
  sys.stdout.write( 'Getting replicas from FC (chunks of %d): ' % chunkSize )
  replicas = {'Successful':{}, 'Failed':{}}
  for chunk in breakListIntoChunks( lfns, chunkSize ):
    sys.stdout.write( '.' )
    sys.stdout.flush()
    res = rm.getCatalogReplicas( chunk, allStatus=True )
    if not res['OK']:
      gLogger.error( "Error getting file replicas:", res['Message'] )
      DIRAC.exit( 1 )
    replicas['Successful'].update( res['Value']['Successful'] )
    replicas['Failed'].update( res['Value']['Failed'] )

  gLogger.always( '' )
  repsDict = {}
  repsMultDict = {}
  transDict = {}
  notFound = []
  bkToggle = []
  notFoundAtSE = []
  transNotSet = {}
  if userFiles:
    gLogger.always( "%d user files are not considered" % userFiles )
  gLogger.always( 'Checking with BK' )
  for lfn in lfns:
    if lfn in replicas['Failed']:
      notFound.append( lfn )
    elif lfn in replicas['Successful']:
      reps = replicas['Successful'][lfn]
      overlapSEs = [se for se in reps if se in targetSEs]
      if not overlapSEs:
        notFoundAtSE.append( lfn )
        continue
      # Set the file problematic in the LFC
      if len( overlapSEs ) == 1:
        repsDict[lfn] = {'SE': overlapSEs[0], 'Status':'-' if reset else 'P', 'PFN': reps[overlapSEs[0]]}
      else:
        repsMultDict[lfn] = [{'SE': se, 'Status':'-' if reset else 'P', 'PFN': reps[se] } for se in overlapSEs]
      # Now see if the file is present in a transformation
      otherSEs = [se for se in reps if se not in targetSEs]
      if not otherSEs or reset:
        bkToggle.append( lfn )

  if bkToggle:
    chunkSize = 100
    sys.stdout.write( 'Checking with Transformation system (chunks of %d): ' % chunkSize )
    transStatusOK = { True:( 'Problematic', 'MissingLFC', 'MaxReset' ), False:( 'Unused', 'MaxReset', 'Assigned' )}
    for chunk in breakListIntoChunks( bkToggle, chunkSize ):
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
  gLogger.always( '' )
  gLogger.setLevel( 'INFO' if fullInfo else 'WARNING' )
  if notFound:
    gLogger.always( "\n%d files not found in FC" % len( notFound ) )
    for lfn in notFound:
      gLogger.info( '\t%s' % lfn )

  if notFoundAtSE:
    gLogger.always( "\n%d files not found in FC at any of %s" % ( len( notFoundAtSE ), targetSEs ) )
    for lfn in notFoundAtSE:
      gLogger.info( '\t%s' % lfn )

  status = '-' if reset else 'P'
  if repsDict:
    res = rm.setCatalogReplicaStatus( repsDict ) if action else {'OK':True}
    if not res['OK']:
      gLogger.error( "\nError setting replica status to %s in FC for %d files" % ( status, len( repsDict ) ), res['Message'] )
    else:
      gLogger.always( "\nReplicas set (%s) in FC for %d files" % ( status, len( repsDict ) ) )
    for lfn in repsDict:
      gLogger.info( '\t%s' % lfn )
  if repsMultDict:
    nbReps = 0
    for lfn in repsMultDict:
      for repDict in repsMultDict[lfn]:
        res = rm.setCatalogReplicaStatus( {lfn:repDict} ) if action else {'OK':True}
        if not res['OK']:
          gLogger.error( "\nError setting replica status to %s in FC for %d files" % ( status, len( repsDict ) ), res['Message'] )
        else:
          nbReps += 1
    gLogger.always( "\n%d replicas set (%s) in FC for %d files" % ( nbReps, status, len( repsMultDict ) ) )
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
      gLogger.error( "\nReplica flag not %s in BK for %d files" % ( stat, len( bkToggle ) ), res['Message'] )
    else:
      gLogger.always( "\nReplica flag %s in BK for %d files" % ( stat, len( bkToggle ) ) )
      for lfn in bkToggle:
        gLogger.info( '\t%s' % lfn )

  if transDict:
    n = 0
    for lfns in transDict.values():
      n += len( lfns )
    status = 'Unused' if reset else 'Problematic'
    gLogger.always( "\n%d files were set %s in the transformation system" % ( n, status ) )
    for transID in sorted( transDict ):
      lfns = transDict[transID]
      res = tr.setFileStatusForTransformation( transID, status, lfns, force=True ) if action else {'OK':True}
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
      gLogger.verbose( "\t%d files were in status %s" % ( len( transNotSet[status] ), status ) )
      for lfn, transID in transNotSet[status]:
        gLogger.verbose( '\t\t%s (%s)' % ( lfn, transID ) )

  gLogger.always( "Execution completed in %.2f seconds" % ( time.time() - startTime ) )
