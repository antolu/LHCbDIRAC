#!/usr/bin/env python
"""
   1) If --Directory is used: get files in LFC directories, check if they are in BK and if the replica flag is set
   2) If a BK query is used (--BKQuery, --Production), gets files in BK and check if they are in the LFC
   If --FixIt is set, take actions:
     1) LFC->BK
       Missing files: remove from SE and LFC
       No replica flag: set it
     2) BK->LFC
       Removes the replica flag in the BK for files not in the LFC
"""

from DIRAC.Core.Base import Script
Script.parseCommandLine( ignoreErrors = False )

import os, sys

import DIRAC
from DIRAC.Core.Utilities.List import breakListIntoChunks
from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager

from LHCbDIRAC.DataManagementSystem.Client.DMScript import DMScript
from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
from LHCbDIRAC.BookkeepingSystem.Client.BKQuery import BKQuery
from LHCbDIRAC.DataManagementSystem.Client.ConsistencyChecks import ConsistencyChecks

def checkLFC2BK( lfns, fixIt ):
  bk = BookkeepingClient()
  res = bk.getFileMetadata( lfns )
  if not res['OK']: return
  metadata = res['Value']
  missingLFNs = [lfn for lfn in lfns if metadata.get( lfn, {} ).get( 'GotReplica' ) == None]
  noFlagLFNs = [lfn for lfn in lfns if metadata.get( lfn, {} ).get( 'GotReplica' ) == 'No']
  okLFNs = [lfn for lfn in lfns if metadata.get( lfn, {} ).get( 'GotReplica' ) == 'Yes']
  print "Out of %d files, %d have a replica flag in the BK, %d are not in the BK and %d don't have the flag" % ( len( lfns ), len( okLFNs ), len( missingLFNs ), len( noFlagLFNs ) )
  if missingLFNs:
    print "Files not in BK:"
    for lfn in missingLFNs:
      print lfn
  if noFlagLFNs:
    print "Files without replica flag in BK:"
    for lfn in noFlagLFNs:
      print lfn
  if fixIt:
    if missingLFNs:
      print "Attempting to remove %d files from SE and LFC:" % len( missingLFNs )
      res = rm.removeFile( missingLFNs )
      if res['OK']:
        success = len( res['Value']['Successful'] )
        failures = len( res['Value']['Failed'] )
        print "\t%d success, %d failures" % ( success, failures )

    if noFlagLFNs:
      print "Setting the replica flag in BK for %d files:" % len( noFlagLFNs )
      res = bk.addFiles( noFlagLFNs )
      if res['OK']:
        print "\tSuccessfully added replica flag"

if __name__ == "__main__":

  dmScript = DMScript()
  dmScript.registerNamespaceSwitches()
  dmScript.registerFileSwitches()
  dmScript.registerBKSwitches()

  Script.registerSwitch( '', 'FixIt', '   Take action to fix the catalogs' )

  Script.setUsageMessage( '\n'.join( [ __doc__,
                                       'Usage:',
                                       '  %s [option|cfgfile] [<LFN>] [<LFN>...]' % Script.scriptName, ] ) )

  Script.addDefaultOptionValue( 'LogLevel', 'error' )
  fixIt = False
  switches = Script.getUnprocessedSwitches()
  for opt, val in switches:
    if opt == 'FixIt':
      fixIt = True

  directories = dmScript.getOption( "Directory", [] )
  lfns = dmScript.getOption( "LFNs", [] )
  rm = ReplicaManager()
  bk = BookkeepingClient()

  bkQueries = [dmScript.getBKQuery()]
  for lfn in sorted( lfns ):
    if not lfn.startswith( '/lhcb' ):
      bkQueries.append( BKQuery( lfn ) )
      lfns.remove( lfn )

  if lfns:
    checkLFC2BK( lfns, fixIt )

  elif directories:
    for dir in directories:
      lfns = cc._getFilesFromDirectoryScan( directories )
      if lfns:
        checkLFC2BK( lfns, fixIt )
      else:
        print "No files found"

  else:
    for bkQuery in bkQueries:
      print "For BK query:", bkQuery
      if bkQuery.getQueryDict():
        lfns = bkQuery.getLFNs()
        if not lfns:
          print "No files found in BK"
          continue
        print len( lfns ), 'files found in BK'
        success = 0
        missingLFNs = []
        for chunk in breakListIntoChunks( lfns, 500 ):
          res = rm.getCatalogExists( chunk )
          if res['OK']:
            success += len ( [lfn for lfn in chunk if lfn in res['Value']['Successful'] and res['Value']['Successful'][lfn]] )
            missingLFNs += [lfn for lfn in chunk if lfn in res['Value']['Failed']] + [lfn for lfn in chunk if lfn in res['Value']['Successful'] and not res['Value']['Successful'][lfn]]
        print '\t%d are in the LFC, %d are not' % ( success, len( missingLFNs ) )
        if fixIt and missingLFNs:
          print "Attempting to remove GotReplica for %d files:" % len( missingLFNs )
          res = bk.removeFiles( missingLFNs )
          if res['OK']:
            print "\tReplica flag successfully removed in BK"


