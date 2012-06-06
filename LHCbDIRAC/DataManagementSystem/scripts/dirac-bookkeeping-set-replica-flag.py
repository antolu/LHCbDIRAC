#! /usr/bin/env python
"""
   Set the replica flag for output files of a transformation that are in the LFC and not in the BK
   <transList> is a comma-separated list of transformation ID or ranges (<t1>:<t2>)
"""
import DIRAC
from DIRAC.Core.Base import Script
from LHCbDIRAC.BookkeepingSystem.Client.BKQuery import BKQuery
from DIRAC import gLogger
import sys, os

if __name__ == "__main__":

  Script.setUsageMessage( '\n'.join( [ __doc__,
                                       'Usage:',
                                       '  %s [option|cfgfile] <transList>' % Script.scriptName, ] ) )

  Script.addDefaultOptionValue( 'LogLevel', 'error' )
  Script.parseCommandLine( ignoreErrors=False )

  args = Script.getPositionalArgs()

  if not len( args ):
    print "Specify transformation number..."
    DIRAC.exit( 0 )
  else:
    try:
      ids = args[0].split( "," )
      idList = []
      for id in ids:
          r = id.split( ':' )
          if len( r ) > 1:
              for i in range( int( r[0] ), int( r[1] ) + 1 ):
                  idList.append( i )
          else:
              idList.append( int( r[0] ) )
    except:
      print "Invalid list of transformation (ranges):", args[0]
      Script.showHelp()
      DIRAC.exit( 1 )

  from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager
  rm = ReplicaManager()
  from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient  import BookkeepingClient
  bk = BookkeepingClient()
  from DIRAC.Core.Utilities.List                                         import breakListIntoChunks
  from LHCbDIRAC.TransformationSystem.Client.TransformationClient           import TransformationClient
  transClient = TransformationClient()

  prodsWithMerge = ( 'MCSimulation', 'DataStripping', 'DataSwimming', 'WGProduction' )
  for prod in idList:
    res = transClient.getTransformation( prod, extraParams=False )
    if not res['OK']:
        print "Couldn't find transformation", prod
        continue
    else:
        transType = res['Value']['Type']
    bkQuery = BKQuery( {'Production': prod, 'ReplicaFlag':'No'}, fileTypes='ALL',
                       visible=( transType not in prodsWithMerge ) )
    print "Production %d: %s" % ( prod, transType )
    lfns = bkQuery.getLFNs( printOutput=False )
    if not lfns:
      print "\tNo files found without replica flag"
      continue
    print "\tFound %d files without replica flag" % len( lfns )
    existingLFNs = []
    if transType not in prodsWithMerge:
      # In principle few files without replica flag, check them in FC
      sys.stdout.write( '\tChecking LFC ' )
      sys.stdout.flush()
      thousands = 10
      for chunk in breakListIntoChunks( lfns, thousands * 1000 ):
        sys.stdout.write( len( chunk ) / 1000 * '.' )
        sys.stdout.flush()
        res = rm.getReplicas( chunk )
        if res['OK']:
          existingLFNs += res['Value']['Successful'].keys()
    else:
      # In principle most files have no replica flag, start from LFC files with replicas
      dirs = []
      for lfn in lfns:
        dir = os.path.dirname( lfn )
        if dir not in dirs:
          dirs.append( dir )
      dirs.sort()
      print "\tChecking LFC files from %d directories" % len( dirs )
      gLogger.setLevel( 'FATAL' )
      res = rm.getFilesFromDirectory( dirs )
      gLogger.setLevel( 'WARNING' )
      if not res['OK']:
        print "Error getting files from directories %s:" % dirs, res['Message']
        continue
      if res['Value']:
        res = rm.getReplicas( res['Value'] )
        if res['OK']:
          existingLFNs = res['Value']['Successful']


    print ""
    if len( existingLFNs ) != len( lfns ):
      print "\t%d files in BK are not in the LFC" % ( len( lfns ) - len( existingLFNs ) )
    if existingLFNs:
      print "\t%d files are in the LFC and not in BK" % len( existingLFNs )
      res = bk.addFiles( existingLFNs )
      if res['OK']:
        print "\tSuccessfully set replica flag to %d files " % len( existingLFNs )



