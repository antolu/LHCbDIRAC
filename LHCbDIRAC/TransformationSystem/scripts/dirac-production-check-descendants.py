#!/usr/bin/env python
''' Does a TS -> BKK check for processed files with descendants
'''


#imports
import sys, os, time
import DIRAC
from DIRAC import gLogger
#Code
if __name__ == '__main__':

  #Script initialization
  from DIRAC.Core.Base import Script
  from LHCbDIRAC.DataManagementSystem.Client.DMScript import DMScript

  dmScript = DMScript()
  dmScript.registerFileSwitches()

  Script.setUsageMessage( '\n'.join( [ __doc__,
                                       'Usage:',
                                       '  %s [option|cfgfile] [ProdIDs]' % Script.scriptName, ] ) )
  Script.registerSwitch( '', 'Runs=', 'Specify the run range' )
  Script.registerSwitch( '', 'ActiveRunsProduction=', 'Specify the production from which the runs should be derived' )
  Script.registerSwitch( '', 'FileType=', 'Specify the descendants file type' )
  Script.registerSwitch( '', 'FixIt', 'Fix the files in transformation table' )
  Script.parseCommandLine( ignoreErrors = True )
  fileType = []
  runsList = []
  fixIt = False
  fromProd = None
  for switch in Script.getUnprocessedSwitches():
    if switch[0] == 'Runs':
      runsList = switch[1].split( ',' )
    if switch[0] == 'FileType':
      fileType = switch[1].split( ',' )
    elif switch[0] == 'FixIt':
      fixIt = True
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
  lfnList = dmScript.getOption( 'LFNs', [] )

  from LHCbDIRAC.DataManagementSystem.Client.ConsistencyChecks import ConsistencyChecks
  from LHCbDIRAC.BookkeepingSystem.Client.BKQuery              import BKQuery
  for id in idList:

    cc = ConsistencyChecks()
    cc.lfns = lfnList
    cc.prod = id
    gLogger.always( "Processing %s production %d" % ( cc.transType, cc.prod ) )
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
    cc.checkTS2BKK()

    # Print out the results
    gLogger.always( '\nResults:' )
    if cc.inFCNotInBK:
      gLogger.always( "%d descendants were found in FC but not in BK" % len( cc.inFCNotInBK ) )
      if fixIt:
        res = cc.bkClient.addFiles( cc.inFCNotInBK )
        if not res['OK']:
          gLogger.error( "Error setting replica flag", res['Message'] )
        else:
          gLogger.always( 'Replica flag set successfully' )
      else:
        gLogger.always( '\n'.join( sorted( cc.inFCNotInBK ) ) )
        gLogger.always( "Use --FixIt for fixing it (or dirac-dms-check-fc2bkk --Term and paste the list)" )

    if cc.removedFiles:
      from DIRAC.Core.Utilities.List import breakListIntoChunks
      gLogger.always( "%d input files are processed, have no descendants but are not in the FC" % len( cc.removedFiles ) )
      for lfnChunk in breakListIntoChunks( cc.removedFiles, 1000 ):
        while True:
          res = cc.transClient.setFileStatusForTransformation( cc.prod, 'Removed', lfnChunk, force = True )
          if not res['OK']:
            gLogger.error( 'Error setting files Removed, retry...', res['Message'] )
          else:
            break
      gLogger.always( "\tFiles set to status Removed" )


    if fileType:
      gLogger.always( "%d unique daughters found" % ( len( cc.descendantsForProcessedLFNs ) + len( cc.descendantsForNonProcessedLFNs ) ) )

    if cc.processedLFNsWithMultipleDescendants:
      nMax = 20
      if len( cc.processedLFNsWithMultipleDescendants ) > nMax:
        prStr = ' (first %d files)' % nMax
      else:
        prStr = ''
      gLogger.error( "Processed LFNs with multiple descendants (%d) -> ERROR%s\n%s" \
                     % ( len( cc.processedLFNsWithMultipleDescendants ) , prStr, '\n'.join( sorted( cc.processedLFNsWithMultipleDescendants )[0:nMax] ) ) )
      suffix = ''
      n = 0
      import os
      while True:
        fileName = 'FilesMultiplyProcessed_%s%s.txt' % ( str( cc.prod ), suffix )
        if not os.path.exists( fileName ):
          break
        n += 1
        suffix = '-%d' % n
      fp = open( fileName, 'w' )
      fp.write( '\n'.join( ['%s: %s' % ( lfn, str( multi ) ) for lfn, multi in cc.processedLFNsWithMultipleDescendants.items()] ) )
      fp.close()
      gLogger.always( 'Complete list of files is in %s' % fileName )
      gLogger.error( "I'm not doing anything for them, neither with the 'FixIt' option" )
    else:
      gLogger.always( "No processed LFNs with multiple descendants found -> OK!" )

    if cc.processedLFNsWithoutDescendants:
      lfns = sorted( cc.processedLFNsWithoutDescendants )
      gLogger.always( "Processed LFNs without descendants (%d) -> ERROR!\n%s" \
                      % ( len( cc.processedLFNsWithoutDescendants ), '\n'.join( lfns ) ) )
      if fixIt:
        gLogger.always( "Resetting them 'Unused'" )
        res = cc.transClient.setFileStatusForTransformation( id, 'Unused', lfns, force = True )
        if not res['OK']:
          gLogger.error( "Error resetting files to Unused", res['Message'] )
        else:
          if res['Value']['Failed']:
            gLogger.error( "Those files could not be reset Unused:", '\n'.join( res['Value']['Failed'] ) )
      else:
        gLogger.always( "Use --FixIt for fixing" )
    else:
      gLogger.always( "No processed LFNs without descendants found -> OK!" )

    if cc.nonProcessedLFNsWithMultipleDescendants:
      gLogger.error( "Non processed LFNs with multiple descendants (%d) -> ERROR\n%s" \
                     % ( len( cc.nonProcessedLFNsWithMultipleDescendants ) , '\n'.join( sorted( cc.nonProcessedLFNsWithMultipleDescendants ) ) ) )
      gLogger.error( "I'm not doing anything for them, neither with the 'FixIt' option" )
    else:
      gLogger.always( "No non processed LFNs with multiple descendants found -> OK!" )

    #fixing, if requested
    if cc.nonProcessedLFNsWithDescendants:
      gLogger.error( "There are %d LFNs not marked Processed but that have descendants\n%s" \
                     % ( len( cc.nonProcessedLFNsWithDescendants ), '\n'.join( sorted( cc.nonProcessedLFNsWithDescendants ) ) ) )
      if fixIt:
        gLogger.always( "Marking them as 'Processed'" )
        cc.transClient.setFileStatusForTransformation( id, 'Processed', cc.nonProcessedLFNsWithDescendants )
      else:
        gLogger.always( "Use --FixIt for fixing" )
    else:
      gLogger.always( "No non processed LFNs with descendants found -> OK!" )
    gLogger.always( "Processed production %d" % cc.prod )
