#! /usr/bin/env python
"""
   Reset Unused the files in status <Status> of Transformation <TransID>
"""

import DIRAC
from DIRAC.Core.Base import Script
from DIRAC.TransformationSystem.Client.TransformationClient     import TransformationClient

from LHCbDIRAC.DataManagementSystem.Client.DMScript import DMScript
from DIRAC.Core.Utilities.List import breakListIntoChunks

if __name__ == "__main__":

  dmScript = DMScript()
  dmScript.registerFileSwitches()
  newStatus = 'Unused'
  statusList = ( "Unused", "Assigned", "Done", "Problematic", "MissingLFC", "MissingInFC", "MaxReset",
                 "Processed", "NotProcessed", "Removed", 'ProbInFC' )
  Script.registerSwitch( '', 'Status=', "Select files with a given status from %s" % str( statusList ) )
  Script.registerSwitch( '', 'NewStatus=', "New status to be set (default: %s)" % newStatus )
  Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                       'Usage:',
                                       '  %s [option|cfgfile] <TransID> <Status>' % Script.scriptName, ] ) )
  Script.parseCommandLine()

  switches = Script.getUnprocessedSwitches()
  status = None
  for opt, val in switches:
    if opt == 'Status':
      if val not in statusList:
        print "Unknown status %s... Select in %s" % ( val, str( statusList ) )
        Script.showHelp()
        DIRAC.exit( 1 )
      status = val
    elif opt == 'NewStatus':
      if val not in statusList:
        print "Unknown status %s... Select in %s" % ( val, str( statusList ) )
        Script.showHelp()
        DIRAC.exit( 1 )
      newStatus = val

  args = Script.getPositionalArgs()

  if not len( args ):
    print "Specify transformation number..."
    DIRAC.exit( 0 )
  else:
    ids = args[0].split( "," )
    idList = []
    try:
      for id_o in ids:
        r = id_o.split( ':' )
        if len( r ) > 1:
          for i in xrange( int( r[0] ), int( r[1] ) + 1 ):
            idList.append( i )
        else:
          idList.append( int( r[0] ) )
    except:
      print "Invalid set of transformationIDs..."
      DIRAC.exit( 1 )

  if len( args ) == 2:
    status = args[1]
  elif not status:
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
      resetFiles = 0
      failed = {}
      for lfnChunk in breakListIntoChunks( lfns, 10000 ):
        res = transClient.setFileStatusForTransformation( transID, newStatus, lfnChunk,
                                             force = ( status == 'MaxReset' or status == 'Processed' ) or lfnsExplicit )
        if res['OK']:
          resetFiles += len( res['Value'].get( 'Successful', res['Value'] ) )
          for lfn, reason in res['Value'].get( 'Failed', {} ).items():
            if reason != 'File not found in the Transformation Database':
              failed.setdefault( reason, [] ).append( lfn )
        else:
          print "Failed to reset %d files to Unused in transformation %s: %s" % ( len( lfns ), transID, res['Message'] )
      print "%d files were reset Unused in transformation %s" % ( resetFiles, transID )
      if failed:
        for reason in failed:
          print 'Failed for %d files: %s' % ( len( failed[reason] ), reason )
  DIRAC.exit( 0 )
