#!/usr/bin/env python
''' Does a TS -> BKK check for processed files with descendants
'''

#Script initialization
from DIRAC.Core.Base import Script
Script.setUsageMessage( '\n'.join( [ __doc__,
                                     'Usage:',
                                     '  %s [option|cfgfile] [ProdIDs]' % Script.scriptName, ] ) )
Script.registerSwitch( '', 'Runs=', 'Specify the run range' )
Script.registerSwitch( '', 'FileType=', 'Specify the descendants file type' )
Script.registerSwitch( '', 'FixIt', 'Fix the files in transformation table' )
Script.parseCommandLine( ignoreErrors = True )

#imports
import sys, os, time
import DIRAC
from DIRAC import gLogger
from LHCbDIRAC.DataManagementSystem.Client.ConsistencyChecks import ConsistencyChecks

#Code
if __name__ == '__main__':

  fileType = []
  runsList = []
  fixIt = False
  for switch in Script.getUnprocessedSwitches():
    if switch[0] == 'Runs':
      runsList = switch[1].split( ',' )
    if switch[0] == 'FileType':
      fileType = switch[1].split( ',' )
    elif switch[0] == 'FixIt':
      fixIt = True

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
    cc.fileType = fileType
    cc.fileTypesExcluded = ['LOG']
    cc.runsList = runsList
    cc.checkTS2BKK()
    if fileType:
      gLogger.always( "%d unique descendants found" % ( len( cc.descendantsForProcessedLFNs ) + len( cc.descendantsForNonProcessedLFNs ) ) )
    if cc.processedLFNsWithMultipleDescendants:
      gLogger.error( "Processed LFNs with multiple descendants:\n%s" % '\n'.join( cc.processedLFNsWithMultipleDescendants ) )
      gLogger.error( "I'm not doing anything for them, neither with the 'FixIt' option" )
    else:
      gLogger.always( "No processed LFNs with multiple descendants found -> OK!" )
    if cc.nonProcessedLFNsWithMultipleDescendants:
      gLogger.error( "Non processed LFNs with multiple descendants:\n%s" % '\n'.join( cc.nonProcessedLFNsWithMultipleDescendants ) )
      gLogger.error( "I'm not doing anything for them, neither with the 'FixIt' option" )
    else:
      gLogger.always( "No non processed LFNs with multiple descendants found -> OK!" )
    if cc.processedLFNsWithoutDescendants:
      gLogger.always( "Processed LFNs without descendants -> ERROR!\n%s" % '\n'.join( cc.processedLFNsWithoutDescendants ) )
    else:
      gLogger.always( "No processed LFNs without descendants found -> OK!" )
    #fixing, if requested
    if cc.nonProcessedLFNsWithDescendants:
      gLogger.error( "There are %d LFNs marked as not 'Processed' but that have descendants" % len( cc.nonProcessedLFNsWithDescendants ) )
      if fixIt:
        gLogger.always( "Marking them as 'Processed'" )
        cc.transClient.setFileStatusForTransformation( id, 'Processed', cc.nonProcessedLFNsWithDescendants )
      else:
        gLogger.always( "use --FixIt for fixing" )
    else:
      gLogger.always( "No non processed LFNs with descendants found -> OK!" )
    gLogger.always( "Processed production %d" % cc.prod )
