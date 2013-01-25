#!/usr/bin/env python
''' Does a TS -> BKK check for processed files with descendants
'''

#Script initialization
from DIRAC.Core.Base import Script
Script.setUsageMessage( '\n'.join( [ __doc__,
                                     'Usage:',
                                     '  %s [option|cfgfile] [ProdIDs]' % Script.scriptName, ] ) )
Script.registerSwitch( '', 'Runs=', 'Specify the run range' )
Script.registerSwitch( '', 'RunStatus=', 'Specify a run status for selecting runs' )
Script.registerSwitch( '', 'FromProduction=', 'Specify the production from which the runs should be derived' )
Script.registerSwitch( '', 'FileType=', 'Specify the descendants file type' )
Script.registerSwitch( '', 'FixIt', 'Fix the files in transformation table' )
Script.parseCommandLine( ignoreErrors = True )

#imports
import sys, os, time
import DIRAC
from DIRAC import gLogger
from LHCbDIRAC.DataManagementSystem.Client.ConsistencyChecks import ConsistencyChecks
from LHCbDIRAC.BookkeepingSystem.Client.BKQuery              import BKQuery
#Code
if __name__ == '__main__':

  fileType = []
  runsList = []
  fixIt = False
  runStatus = None
  fromProd = None
  for switch in Script.getUnprocessedSwitches():
    if switch[0] == 'Runs':
      runsList = switch[1].split( ',' )
    if switch[0] == 'FileType':
      fileType = switch[1].split( ',' )
    elif switch[0] == 'FixIt':
      fixIt = True
    elif switch[0] == 'RunStatus':
      runStatus = switch[1].split( ',' )
    elif switch[0] == 'FromProduction':
      try:
        fromProd = int( switch[1] )
      except:
        gLogger.exception( "Wrong production number: %s" % switch[1] )
        DIRAC.exit( 0 )

  if runStatus and not fromProd:
    gLogger.error( "Please specify from which production the run ranges should be obtained (--FromProduction <prod>)" )
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

  for id in idList:

    cc = ConsistencyChecks()
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
    cc.runStatus = runStatus
    cc.fromProd = fromProd
    cc.checkTS2BKK()
    if fileType:
      gLogger.always( "%d unique descendants found" % ( len( cc.descendantsForProcessedLFNs ) + len( cc.descendantsForNonProcessedLFNs ) ) )
    if cc.processedLFNsWithMultipleDescendants:
      gLogger.error( "Processed LFNs with multiple descendants (%d) -> ERROR\n%s" \
                     % ( len( cc.processedLFNsWithMultipleDescendants ) , '\n'.join( cc.processedLFNsWithMultipleDescendants ) ) )
      gLogger.error( "I'm not doing anything for them, neither with the 'FixIt' option" )
    else:
      gLogger.always( "No processed LFNs with multiple descendants found -> OK!" )

    if cc.processedLFNsWithoutDescendants:
      gLogger.always( "Processed LFNs without descendants (%d) -> ERROR!\n%s" \
                      % ( len( cc.processedLFNsWithoutDescendants ), '\n'.join( cc.processedLFNsWithoutDescendants.keys() ) ) )
      if fixIt:
        gLogger.always( "Resetting them 'Unused'" )
        res = cc.transClient.setFileStatusForTransformation( id, 'Unused', cc.processedLFNsWithoutDescendants.keys(), force = True )
        if not res['OK']:
          gLogger.error( "Error resetting files to Unused", res['Message'] )
        else:
          if res['Value']['Failed']:
            gLogger.error( "Those files could not be reset Unused:", '\n'.join( res['Value']['Failed'] ) )
      else:
        gLogger.always( "use --FixIt for fixing" )
    else:
      gLogger.always( "No processed LFNs without descendants found -> OK!" )

    if cc.nonProcessedLFNsWithMultipleDescendants:
      gLogger.error( "Non processed LFNs with multiple descendants (%d) -> ERROR\n%s" \
                     % ( len( cc.nonProcessedLFNsWithMultipleDescendants ) , '\n'.join( cc.nonProcessedLFNsWithMultipleDescendants ) ) )
      gLogger.error( "I'm not doing anything for them, neither with the 'FixIt' option" )
    else:
      gLogger.always( "No non processed LFNs with multiple descendants found -> OK!" )

    #fixing, if requested
    if cc.nonProcessedLFNsWithDescendants:
      gLogger.error( "There are %d LFNs not marked Processed but that have descendants" % len( cc.nonProcessedLFNsWithDescendants ) )
      if fixIt:
        gLogger.always( "Marking them as 'Processed'" )
        cc.transClient.setFileStatusForTransformation( id, 'Processed', cc.nonProcessedLFNsWithDescendants )
      else:
        gLogger.always( "use --FixIt for fixing" )
    else:
      gLogger.always( "No non processed LFNs with descendants found -> OK!" )
    gLogger.always( "Processed production %d" % cc.prod )
