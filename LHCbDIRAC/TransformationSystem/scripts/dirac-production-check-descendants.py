#!/usr/bin/env python

from DIRAC.Core.Base import Script
Script.parseCommandLine( ignoreErrors = True )

import sys, os, time

import DIRAC
from DIRAC import gLogger

from LHCbDIRAC.DataManagementSystem.Client.ConsistencyChecks import ConsistencyChecks

if __name__ == '__main__':

  Script.registerSwitch( '', 'Runs=', 'Specify the run range' )
  Script.registerSwitch( '', 'Extension=', 'Specify the descendants file extension' )
  Script.registerSwitch( '', 'FixIt', 'Fix the files in transformation table' )

  extension = ''
  runsList = []
  fixIt = False
  for switch in Script.getUnprocessedSwitches():
    if switch[0] == 'Runs':
      runsList = switch[1].split( ',' )
    if switch[0] == 'Extension':
      extension = switch[1].lower()
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

    cc = ConsistencyChecks( id )
    gLogger.info( "Processing %s production %d" % ( cc.transType, cc.prod ) )
    cc.fileType = extension
    cc.runsList = runsList
    cc.descendantsConsistencyCheck()
    if cc.processedLFNsWithMultipleDescendants:
      gLogger.info( "Processed LFNs with multiple descendants: %s" % str( cc.processedLFNsWithMultipleDescendants ) )
    if cc.nonProcessedLFNsWithMultipleDescendants:
      gLogger.info( "Non processed LFNs with multiple descendants: %s" % str( cc.nonProcessedLFNsWithMultipleDescendants ) )
    #fixing, if requested
    if cc.processedLFNsWithoutDescendants:
      if fixIt:
        gLogger.info( "Resetting to 'Unused' the files marked as 'Processed' that does not have descendants" )
        #check if there are jobs that produced something, that is uploaded
        #TODO
      else:
        gLogger.info( "use --FixIt for fixing" )
    if cc.nonProcessedLFNsWithDescendants:
      if fixIt:
        ba
      else:
        gLogger.info( "use --FixIt for fixing" )
>>>>>>> refs / remotes / origin / master
