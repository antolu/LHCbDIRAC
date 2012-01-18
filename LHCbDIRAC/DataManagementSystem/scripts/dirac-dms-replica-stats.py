#!/usr/bin/env python
########################################################################
# $HeadURL$
########################################################################

"""
   Get statistics on number of replicas for a given directory or production
"""

__RCSID__ = "$Id$"

from DIRAC.Core.Base import Script
import os, sys

def orderSEs( listSEs ):
  listSEs = sortList( listSEs )
  orderedSEs = [se for se in listSEs if se.endswith( "-ARCHIVE" )]
  orderedSEs += [se for se in listSEs if not se.endswith( "-ARCHIVE" )]
  return orderedSEs

import DIRAC
from DIRAC.Core.Base import Script
from LHCbDIRAC.DataManagementSystem.Client.DMScript import DMScript

if __name__ == "__main__":

  dmScript = DMScript()
  dmScript.registerBKSwitches()
  dmScript.registerNamespaceSwitches()

  Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                       'Usage:',
                                       '  %s [option|cfgfile] [<LFN>] [<LFN>...]' % Script.scriptName, ] ) )

  Script.registerSwitch( "S:", "Size", "   Get the LFN size [No]" )
  Script.registerSwitch( '', 'Invisible', '   Show invisible files also [No]' )
  Script.addDefaultOptionValue( 'LogLevel', 'error' )
  Script.parseCommandLine( ignoreErrors = False )

  getSize = False
  visible = True
  for switch in Script.getUnprocessedSwitches():
    if switch[0] in ( "S", "Size" ):
      getSize = True
    if switch[0] == 'Invisible':
      visible = False

  import DIRAC
  from DIRAC.Core.Utilities.List                        import sortList, breakListIntoChunks
  from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager
  from DIRAC.Core.DISET.RPCClient import RPCClient
  from DIRAC.Core.Utilities.SiteSEMapping                                import getSitesForSE, getSEsForSite
  from LHCbDIRAC.DataManagementSystem.Client.StorageUsageClient  import StorageUsageClient

  rm = ReplicaManager()

  repStats = {}
  lfnReplicas = {}
  directories = dmScript.getOption( 'Directory' )
  if directories:
    for directory in directories:
      res = rm.getReplicasFromDirectory( directory )
      if not res['OK']:
        print res['Message']
        continue
      lfnReplicas.update( res['Value'] )
  else:
    bkQuery = dmScript.getBKQuery( visible = visible )
    print "Executing BK query:", bkQuery
    lfns = bkQuery.getLFNs()
    if lfns:
      res = rm.getReplicas( lfns )
      if not res['OK']:
        print res['Message']
        DIRAC.exit( 2 )
      lfnReplicas = res['Value']['Successful']
      if res['Value']['Failed']:
        repStats[0] = len( res['Value']['Failed'] )
    else:
      lfnReplicas = None

  if not lfnReplicas:
    print "No files found that have a replica...."
    if repStats.get( 0 ):
      print "%d files found without a replica" % repStats[0]
    DIRAC.exit( 0 )

  repSEs = {}
  repSites = {}
  maxRep = 0
  maxArch = 0
  nfiles = 0
  totSize = 0
  if getSize:
    lfnSize = {}
    left = len( lfnReplicas )
    for lfns in breakListIntoChunks( lfnReplicas.keys(), 500 ):
      left -= len( lfns )
      sys.stdout.write( "... getting size for %d LFNs (%d left), be patient...    \r" % ( len( lfns ), left ) )
      sys.stdout.flush()
      r = rm.getCatalogFileSize( lfns )
      if r['OK']:
        lfnSize.update( r['Value']['Successful'] )
    for lfn, size in lfnSize.items():
      totSize += size
  for lfn, replicas in lfnReplicas.items():
    SEs = replicas.keys()
    nrep = len( replicas )
    narchive = -1
    for se in list( SEs ):
      if se.endswith( "-FAILOVER" ):
        nrep -= 1
        if not repStats.has_key( -100 ):
          repStats[-100] = 0
        repStats[-100] += 1
        SEs.remove( se )
      if se.endswith( "-ARCHIVE" ):
        nrep -= 1
        narchive -= 1
    if not repStats.has_key( nrep ):
      repStats[nrep] = 0
    repStats[nrep] += 1
    # narchive is negative ;-)
    if not repStats.has_key( narchive ):
      repStats[narchive] = 0
    repStats[narchive] += 1
    for se in replicas.keys():
      if not repSEs.has_key( se ):
        repSEs[se] = [0, 0]
      repSEs[se][0] += 1
      if getSize:
        repSEs[se][1] += lfnSize[lfn]

    if nrep > maxRep: maxRep = nrep
    if -narchive > maxArch: maxArch = -narchive
    nfiles += 1

  GB = 1000. * 1000. * 1000.
  TB = GB * 1000.
  if directories:
    dirStr = " in %s" % str( directories )
  else:
    dirStr = " with replicas"
  if totSize:
    print "%d files found (%.3f GB)%s" % ( nfiles, totSize / GB, dirStr )
  else:
    print "%d files found%s" % ( nfiles, dirStr )
  print "\nReplica statistics:"
  if repStats.has_key( -100 ):
    print "Failover replicas:", repStats[-100], "files"
  if maxArch:
    for nrep in range( 1, maxArch + 1 ):
      if not repStats.has_key( -nrep ):
        repStats[-nrep] = 0
      print nrep - 1, "archives:", repStats[-nrep], "files"
  for nrep in range( maxRep + 1 ):
    if not repStats.has_key( nrep ):
      repStats[nrep] = 0
    print nrep, "replicas:", repStats[nrep], "files"

  print "\nSE statistics:"
  for se in orderSEs( repSEs.keys() ):
    if se.endswith( "-FAILOVER" ): continue
    if not se.endswith( "-ARCHIVE" ):
      res = getSitesForSE( se, gridName = 'LCG' )
      if res['OK']:
        try:
          site = res['Value'][0]
        except:
          continue
        if not repSites.has_key( site ):
          repSites[site] = [0, 0]
        repSites[site][0] += repSEs[se][0]
        repSites[site][1] += repSEs[se][1]
    string = "%16s: %s files" % ( se, repSEs[se][0] )
    if getSize: string += " - %.3f TB" % ( repSEs[se][1] / TB )
    print string

  print "\nSites statistics:"
  for site in sortList( repSites.keys() ):
    string = "%16s: %d files" % ( site, repSites[site][0] )
    if getSize: string += " - %.3f TB" % ( repSites[site][1] / TB )
    print string


