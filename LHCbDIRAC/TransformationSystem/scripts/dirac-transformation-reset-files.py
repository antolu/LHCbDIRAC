#! /usr/bin/env python
"""
   Reset Unused the files in status <Status> of Transformation <TransID>
"""

import DIRAC
from DIRAC.Core.Base import Script
from DIRAC.TransformationSystem.Client.TransformationClient     import TransformationClient

import re, time, types, string, signal, sys, os, cmd
from LHCbDIRAC.DataManagementSystem.Client.DMScript import DMScript

if __name__ == "__main__":

  dmScript = DMScript()
  dmScript.registerFileSwitches()
  Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                       'Usage:',
                                       '  %s [option|cfgfile] <TransID> <Status>' % Script.scriptName, ] ) )
  Script.parseCommandLine()

  args = Script.getPositionalArgs()

  if not len( args ):
    print "Specify transformation number..."
    DIRAC.exit( 0 )
  else:
    ids = args[0].split( "," )
    idList = []
    try:
      for id in ids:
        r = id.split( ':' )
        if len( r ) > 1:
            for i in range( int( r[0] ), int( r[1] ) + 1 ):
                idList.append( i )
        else:
            idList.append( int( r[0] ) )
    except:
      print "Invalid set of transformationIDs..."
      DIRAC.exit( 1 )

  if len( args ) == 2:
    status = args[1]
  else:
    status = 'Unknown'
  lfnsExplicit = dmScript.getOption( 'LFNs' )

  transClient = TransformationClient()

  for transID in idList:
    lfns = lfnsExplicit
    if not lfns:
      res = transClient.getTransformation( transID )
      if not res['OK']:
        print "Failed to get transformation information: %s" % res['Message']
        DIRAC.exit( 2 )

      selectDict = {'TransformationID':res['Value']['TransformationID'], 'Status':status}
      res = transClient.getTransformationFiles( condDict = selectDict )
      if not res['OK']:
        print "Failed to get files: %s" % res['Message']
        DIRAC.exit( 2 )

      lfns = [d['LFN'] for d in res['Value']]
      if not lfns:
        print "No files found in transformation %s, status %s" % ( transID, status )

    if not lfns:
      print "No files to be reset in transformation", transID
    else:
      res = transClient.setFileStatusForTransformation( transID, 'Unused', lfns, force = ( status == 'MaxReset' ) or lfnsExplicit )
      if res['OK']:
        print "%d files were reset Unused in transformation %s" % ( len( lfns ), transID )
      else:
        print "Failed to reset %d files to Unused in transformation %s" % ( len( lfns ), transID )
        DIRAC.exit( 2 )
  DIRAC.exit( 0 )
