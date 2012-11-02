#!/usr/bin/env python

__RCSID__ = "$Id$"

import sys, os, time

import DIRAC
from DIRAC import gLogger

from DIRAC.Core.Base import Script
from LHCbDIRAC.DataManagementSystem.Utilities.BKAndCatalogs import ConsistencyChecks

def _getRunsList( fromProd, runList ):

  if fromProd:
    if runList:
      gLogger.warn( "List of runs given as parameters ignored, superseded with runs from productions" )
    runList = []
    # Get the list of Active runs in that production list
    for prod in fromProd:
      res = transClient.getTransformationRuns( {'TransformationID': prod } )
      if not res['OK']:
        gLogger.warn( "Runs %s not found for transformation" % prod )
      else:
        runList += [str( run['RunNumber'] ) for run in res['Value'] if run['Status'] in ( 'Active', 'Flush' )]
    gLogger.info( "Active runs in productions %s: %s" % ( str( fromProd ), str( runList ) ) )

  runList.sort()

  return runList

def _getTransfParams():
  res = transClient.getTransformation( id )
  if not res['OK']:
    gLogger.warn( res['Message'] )
    raise ValueError, res['Message']
  else:
    transType = res['Value']['Type']
    transStatus = res['Value']['Status']

  return transType, transStatus



if __name__ == '__main__':

  Script.parseCommandLine( ignoreErrors = True )

  extension = ''
  #Script.registerSwitch( '', 'Runs=', 'Specify the run range' )
  #Script.registerSwitch( '', 'RunsFromProduction=', 'Productions number where to get the list of Active runs' )
  Script.registerSwitch( '', 'Extension=', 'Specify the descendants file extension [%s]' % extension )
  #Script.registerSwitch( '', 'FixIt', 'Fix the files in transformation table' )
  #Script.registerSwitch( '', 'Verbose', 'Verbose mode' )

  runList = []
  fixIt = False
  fromProd = []
  verbose = False
  for switch in Script.getUnprocessedSwitches():
    if switch[0] == 'Runs':
      runList = switch[1].split( ',' )
    if switch[0] == 'RunsFromProduction':
      fromProd = switch[1].split( ',' )
    if switch[0] == 'Extension':
      extension = switch[1].lower()
    elif switch[0] == 'FixIt':
      fixIt = True
    elif switch[0] == 'Verbose':
      verbose = True

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

#  runsList = _getRunsList( fromProd, runList )

  for id in idList:

    cc = ConsistencyChecks( id )
    gLogger.info( "Processing %s production %d" % ( cc.transType, cc.prod ) )
    cc.fileType = extension
    cc.descendantsConsistencyCheck()
