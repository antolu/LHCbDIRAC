#!/usr/bin/env python
''' Does a TS -> BK check for processed files with descendants
'''


# imports
import sys, os, time
import DIRAC
from DIRAC import gLogger
# Code
if __name__ == '__main__':

  # Script initialization
  from DIRAC.Core.Base import Script
  from LHCbDIRAC.DataManagementSystem.Client.DMScript import DMScript

  dmScript = DMScript()
  dmScript.registerFileSwitches()

  Script.setUsageMessage( '\n'.join( [ __doc__,
                                       'Usage:',
                                       '  %s [option|cfgfile] [ProdIDs]' % Script.scriptName, ] ) )
  Script.registerSwitch( '', 'Runs=', '   Specify the run range' )
  Script.registerSwitch( '', 'ActiveRunsProduction=', '   Specify the production from which the runs should be derived' )
  Script.registerSwitch( '', 'FileType=', 'S   pecify the descendants file type' )
  Script.registerSwitch( '', 'NoLFC', '   Trust the BK replica flag, no LFC check' )
  Script.registerSwitch( '', 'FixIt', '   Fix the files in transformation table' )
  Script.registerSwitch( '', 'Verbose', '   Print full list of files with error' )
  Script.registerSwitch( '', 'Status=', '   Select files with a given status in the production' )
  Script.parseCommandLine( ignoreErrors = True )
  fileType = []
  runsList = []
  fixIt = False
  fromProd = None
  verbose = False
  status = None
  noLFC = False
  for switch in Script.getUnprocessedSwitches():
    if switch[0] == 'Runs':
      runsList = switch[1].split( ',' )
    elif switch[0] == 'Status':
      status = switch[1].split( ',' )
    elif switch[0] == 'Verbose':
      verbose = True
    elif switch[0] == 'FileType':
      fileType = switch[1].split( ',' )
    elif switch[0] == 'FixIt':
      fixIt = True
    elif switch[0] == 'NoLFC':
      noLFC = True
    elif switch[0] == 'ActiveRunsProduction':
      try:
        fromProd = int( switch[1] )
      except:
        gLogger.exception( "Wrong production number: %s" % switch[1] )
        DIRAC.exit( 0 )

  args = Script.getPositionalArgs()
  if not len( args ):
    gLogger.error( "Specify transformation number..." )
    DIRAC.exit( 0 )
  else:
    ids = args[0].split( "," )
    idList = []
    for id in ids:
      r = id.split( ':' )
      if len( r ) > 1:
        for i in range( int( r[0] ), int( r[1] ) + 1 ):
          idList.append( i )
      else:
        idList.append( int( r[0] ) )
  # In case the user asked for specific LFNs
  if not status:
    lfnList = dmScript.getOption( 'LFNs', [] )

  from LHCbDIRAC.DataManagementSystem.Client.ConsistencyChecks import ConsistencyChecks
  from LHCbDIRAC.BookkeepingSystem.Client.BKQuery              import BKQuery
  from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient
  tr = TransformationClient()
  for id in idList:
    res = tr.getTransformation( id )
    if not res['OK']:
      gLogger.fatal( "Error getting info for transformation", '%d: %s' % ( id, res['Message'] ) )
      continue
    if fileType:
      if res['Value']['Type'] in ( 'Merge', 'MCMerge' ):
        gLogger.always( "It is not allowed to select file type for merging transformation", id )
        continue

    startTime = time.time()
    cc = ConsistencyChecks()
    cc.prod = id
    cc.noLFC = noLFC
    gLogger.always( "Processing %s production %d" % ( cc.transType, cc.prod ) )

    if status:
      res = tr.getTransformationFiles( {'TransformationID':id, 'Status':status} )
      if res['OK']:
        lfnList = [trFile['LFN'] for trFile in res['Value']]
        gLogger.always( 'Found %d files with status %s' % ( len( lfnList ), status ) )
      else:
        gLogger.fatal( "Error getting files %s" % status, res['Message'] )
        DIRAC.exit( 2 )
      if not lfnList:
        continue

    cc.lfns = lfnList
    if not fileType:
      bkQuery = BKQuery( {'Production':id, 'FileType':'ALL', 'Visible':'All'} )
      cc.fileType = bkQuery.getBKFileTypes()
      gLogger.always( "Looking for descendants of type %s" % str( cc.fileType ) )
    else:
      cc.fileType = fileType
      cc.fileTypesExcluded = ['LOG']
    cc.runsList = runsList
    cc.runStatus = 'Active'
    cc.fromProd = fromProd
    cc.checkTS2BK()

    # Print out the results
    gLogger.always( '\nResults:' )
    if verbose:
      nMax = sys.maxint
    else:
      nMax = 20
    suffix = ''
    n = 0
    while True:
      fileName = 'CheckDescendantsResults_%s%s.txt' % ( str( cc.prod ), suffix )
      if not os.path.exists( fileName ):
        break
      n += 1
      suffix = '-%d' % n
    fp = None
    if cc.inFCNotInBK:
      lfns = cc.inFCNotInBK
      gLogger.always( "%d descendants were found in FC but not in BK" % len( lfns ) )
      if fixIt:
        res = cc.bkClient.addFiles( lfns )
        if not res['OK']:
          gLogger.always( "Error setting replica flag", res['Message'] )
        else:
          gLogger.always( 'Replica flag set successfully' )
      else:
        if not fp:
          fp = open( fileName, 'w' )
        fp.write( '\nInFCNotInBK '.join( [''] + lfns ) )
        gLogger.always( 'First %d files:' % nMax if not verbose and len( lfns ) > nMax else 'All files:',
                       '\n'.join( [''] + lfns[0:nMax] ) )
        gLogger.always( "Use --FixIt for setting replica flag in BK (or dirac-dms-check-fc2bkk --File %s)" % fileName )

    if cc.removedFiles:
      from DIRAC.Core.Utilities.List import breakListIntoChunks
      gLogger.always( "%d input files are processed, have no descendants but are not in the FC" % len( cc.removedFiles ) )
      for lfnChunk in breakListIntoChunks( cc.removedFiles, 1000 ):
        while True:
          res = cc.transClient.setFileStatusForTransformation( cc.prod, 'Removed', lfnChunk, force = True )
          if not res['OK']:
            gLogger.always( 'Error setting files Removed, retry...', res['Message'] )
          else:
            break
      gLogger.always( "\tFiles set to status Removed" )


    gLogger.always( "%d unique daughters found with real descendants" % ( len( set( cc.descForPrcdLFNs ).union( cc.descForNonPrcdLFNs ) ) ) )

    if cc.prcdWithMultDesc:
      lfns = sorted( cc.prcdWithMultDesc )
      gLogger.always( "Processed LFNs with multiple descendants (%d) -> ERROR" % len( lfns ) )
      gLogger.always( 'First %d files:' % nMax if not verbose and len( lfns ) > nMax else 'All files:',
                      '\n'.join( [''] + lfns[0:nMax] ) )
      if not fp:
        fp = open( fileName, 'w' )
      fp.write( '\nProcMultDesc '.join( ['%s: %s' % ( lfn, str( multi ) ) \
                                        for lfn, multi in cc.prcdWithMultDesc.items()] ) )
      gLogger.always( "I'm not doing anything for them, neither with the 'FixIt' option" )
    else:
      gLogger.always( "No processed LFNs with multiple descendants found -> OK!" )

    if cc.prcdWithoutDesc:
      lfns = sorted( cc.prcdWithoutDesc )
      gLogger.always( "Processed LFNs without descendants (%d) -> ERROR!" % len( lfns ) )
      if fixIt:
        gLogger.always( "Resetting them 'Unused'" )
        res = cc.transClient.setFileStatusForTransformation( id, 'Unused', lfns, force = True )
        if not res['OK']:
          gLogger.always( "Error resetting files to Unused", res['Message'] )
        else:
          if res['Value']['Failed']:
            gLogger.always( "Those files could not be reset Unused:", '\n'.join( res['Value']['Failed'] ) )
      else:
        if not fp:
          fp = open( fileName, 'w' )
        fp.write( '\nProcNoDesc '.join( [''] + lfns ) )
        gLogger.always( 'First %d files:' % nMax if not verbose and len( lfns ) > nMax else 'All files:',
                        '\n'.join( [''] + lfns[0:nMax] ) )
        gLogger.always( "Use --FixIt for resetting files Unused in TS" )
    else:
      gLogger.always( "No processed LFNs without descendants found -> OK!" )

    if cc.nonPrcdWithMultDesc:
      lfns = sorted( cc.nonPrcdWithMultDesc )
      gLogger.always( "Non processed LFNs with multiple descendants (%d) -> ERROR" % len( lfns ) )
      if not fp:
        fp = open( fileName, 'w' )
      fp.write( '\nNotProcMultDesc '.join( [''] + lfns ) )
      gLogger.always( 'First %d files:' % nMax if not verbose and len( lfns ) > nMax else 'All files:',
                     '\n'.join( [''] + lfns[0:nMax] ) )
      gLogger.always( "I'm not doing anything for them, neither with the 'FixIt' option" )
    else:
      gLogger.always( "No non processed LFNs with multiple descendants found -> OK!" )

    # fixing, if requested
    if cc.nonPrcdWithDesc:
      lfns = sorted( cc.nonPrcdWithDesc )
      gLogger.always( "There are %d LFNs not marked Processed but that have descendants -> ERROR" % len( lfns ) )
      if fixIt:
        gLogger.always( "Marking them as 'Processed'" )
        cc.transClient.setFileStatusForTransformation( id, 'Processed', lfns, force = True )
      else:
        if not fp:
          fp = open( fileName, 'w' )
        fp.write( '\nNotProcWithDesc '.join( [''] + lfns ) )
        gLogger.always( 'First %d files:' % nMax if not verbose and len( lfns ) > nMax else 'All files:',
                        '\n'.join( [''] + lfns[0:nMax] ) )
        gLogger.always( "Use --FixIt for setting files Processed in TS" )
    else:
      gLogger.always( "No non processed LFNs with descendants found -> OK!" )
    if fp:
      fp.close()
      gLogger.always( 'Complete list of files is in %s' % fileName )
    gLogger.always( "Processed production %d in %.1f seconds" % ( cc.prod, time.time() - startTime ) )
