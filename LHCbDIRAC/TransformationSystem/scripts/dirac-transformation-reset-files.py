#! /usr/bin/env python
"""
   Reset Unused the files in status <Status> of Transformation <TransID>
"""

import DIRAC
from DIRAC.Core.Base import Script
from DIRAC.TransformationSystem.Client.TransformationClient     import TransformationClient

import re, time, types, string, signal, sys, os, cmd
Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] <TransID> <Status>' % Script.scriptName, ] ) )
Script.parseCommandLine()

args = Script.getPositionalArgs()

if not len( args ) == 2:
  Script.showUsage()
  DIRAC.exit( 0 )

transID = args[0]
status = args[1]

transClient = TransformationClient()

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

res = transClient.setFileStatusForTransformation( transID, 'Unused', lfns, force = ( status == 'MaxReset' ) )

if res['OK']:
  print "%d files were reset Unused in transformation %s" % ( len( lfns ), transID )
  DIRAC.exit( 0 )
else:
  print "Failed to reser %d files to Unused in transformation %s" % ( len( lfns ), transID )
  DIRAC.exit( 2 )
