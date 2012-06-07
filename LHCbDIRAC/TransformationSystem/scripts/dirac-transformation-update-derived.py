#!/usr/bin/env python

"""
Move files that are Unused or MaxReset from a parent production to its derived production
The argument is a list of productions: comma separated list of ranges (a range has the form p1:p2)
"""

__RCSID__ = "$Id: dirac-transformation-update-derived.py 52217 2012-05-07 08:18:33Z phicharp $"

import sys
from DIRAC.Core.Base import Script


Script.setUsageMessage( '\n'.join( [ __doc__,
                                    'Usage:',
                                    '  %s [option|cfgfile] [prod1[:prod2][,prod3[:prod4]]' % Script.scriptName, ] ) )
Script.registerSwitch( '', 'NoReset', "Don't reset the MaxRest files to unused (default is to reset)" )
Script.parseCommandLine( ignoreErrors=True )
import DIRAC

resetUnused = True
switches = Script.getUnprocessedSwitches()
for switch in switches:
  if switch[0] == 'NoReset':
    resetUnused = False
args = Script.getPositionalArgs()

if not len( args ):
    print "Specify transformation number..."
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

from LHCbDIRAC.TransformationSystem.Client.TransformationClient           import TransformationClient
transClient = TransformationClient()

statusToMove = [ 'Unused', 'MaxReset' ]
for prod in idList:
    res = transClient.getTransformation( prod, extraParams=True )
    if not res['OK']:
        print "Couldn't find transformation", prod
        continue
    else:
      parentProd = int( res['Value'].get( 'InheritedFrom' ) )
      if not parentProd:
        print "Transformation %d was not derived..." % prod
        continue
      selectDict = {'TransformationID': parentProd, 'Status': statusToMove}
      res = transClient.getTransformationFiles( selectDict )
      if not res['OK']:
        print "Error getting Unused files from transformation %s:" % parentProd, res['Message']
        continue
      parentFiles = res['Value']
      lfns = [lfnDict['LFN'] for lfnDict in parentFiles]
      if not lfns:
        print "No files found to be moved from transformation %d to %d" % ( parentProd, prod )
        continue
      selectDict = { 'TransformationID': prod, 'LFN': lfns}
      res = transClient.getTransformationFiles( selectDict )
      if not res['OK']:
        print "Error getting files from derived transformation %s" % prod, res['Message']
        continue
      derivedFiles = res['Value']
      suffix = '-%d' % parentProd
      movedFiles = {}
      errorFiles = {}
      for parentDict in parentFiles:
        lfn = parentDict['LFN']
        status = parentDict['Status']
        force = False
        if resetUnused and status == 'MaxReset':
          status = Unused
          force = True
        derivedStatus = None
        for derivedDict in derivedFiles:
          if derivedDict['LFN'] == lfn:
            derivedStatus = derivedDict['Status']
            break
        if derivedStatus:
          if derivedStatus.endswith( suffix ):
            res = transClient.setFileStatusForTransformation( parentProd, 'MovedTo-%d' % prod, [lfn] )
            if not res['OK']:
              print "Error setting status for %s in transformation %d to %s" % ( lfn, parentProd, 'MovedTo-%d' % prod ), res['Message']
              continue
            res = transClient.setFileStatusForTransformation( prod, status, [lfn], force=force )
            if not res['OK']:
              print "Error setting status for %s in transformation %d to %s" % ( lfn, prod, status ), res['Message']
              transClient.setFileStatusForTransformation( parentProd, status , [lfn] )
              continue
            if force:
              status = 'Unused from MaxReset'
            movedFiles[status] = movedFiles.setdefault( status, 0 ) + 1
          else:
            errorFiles[derivedStatus] = errorFiles.setdefault( derivedStatus, 0 ) + 1
      if errorFiles:
        print "Some files didn't have the expected status in derived transformation %d" % prod
        for err, val in errorFiles.items():
          print "\t%d files were in status %s" % ( val, err )
      if movedFiles:
        print "Successfully moved files from %d to %d:" % ( parentProd, prod )
        for status, val in movedFiles.items():
          print "\t%d files to status %s" % ( val, status )
