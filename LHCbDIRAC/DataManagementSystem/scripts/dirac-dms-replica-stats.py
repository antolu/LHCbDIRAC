#!/usr/bin/env python

"""
   Get statistics on number of replicas for a given directory or production
"""

__RCSID__ = "$Id$"

from DIRAC.Core.Base import Script
import sys

def orderSEs( listSEs ):
  listSEs = sortList( listSEs )
  orderedSEs = [se for se in listSEs if se.endswith( "-ARCHIVE" )]
  orderedSEs += [se for se in listSEs if not se.endswith( "-ARCHIVE" )]
  return orderedSEs

import DIRAC
from LHCbDIRAC.DataManagementSystem.Client.DMScript import DMScript

if __name__ == "__main__":

  dmScript = DMScript()
  dmScript.registerBKSwitches()
  dmScript.registerNamespaceSwitches()
  dmScript.registerFileSwitches()

  Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                       'Usage:',
                                       '  %s [option|cfgfile] [<LFN>] [<LFN>...]' % Script.scriptName, ] ) )

  Script.registerSwitch( "", "Size", "   Get the LFN size [No]" )
  Script.registerSwitch( '', 'DumpNoReplicas', '   Print list of files without a replica [No]' )
  Script.registerSwitch( '', 'DumpWithArchives=', '   =<n>, print list of files with <n> archives' )
  Script.registerSwitch( '', 'DumpWithReplicas=', '   =<n>, print list of files with <n> replicas' )
  Script.addDefaultOptionValue( 'LogLevel', 'error' )
  Script.parseCommandLine( ignoreErrors = False )

  getSize = False
  prNoReplicas = False
  prWithArchives = False
  prWithReplicas = False
  for switch in Script.getUnprocessedSwitches():
    if switch[0] in ( "S", "Size" ):
      getSize = True
    elif switch[0] == 'DumpNoReplicas':
      prNoReplicas = True
    elif switch[0] == 'DumpWithArchives':
      prWithArchives = [int( xx ) for xx in switch[1].split( ',' )]
    elif switch[0] == 'DumpWithReplicas':
      prWithReplicas = [int( xx ) for xx in switch[1].split( ',' )]


  from DIRAC.Core.Utilities.List                        import sortList, breakListIntoChunks
  from DIRAC.DataManagementSystem.Client.DataManager import DataManager
  from DIRAC.Resources.Catalog.FileCatalog import FileCatalog
  from DIRAC.Core.DISET.RPCClient import RPCClient
  from DIRAC.Core.Utilities.SiteSEMapping                                import getSitesForSE, getSEsForSite
  from LHCbDIRAC.DataManagementSystem.Client.StorageUsageClient  import StorageUsageClient

  dm = DataManager()

  repStats = {}
  noReplicas = {}
  withReplicas = {}
  withArchives = {}
  lfns = dmScript.getOption( 'LFNs', [] )
  lfnReplicas = {}
  directories = dmScript.getOption( 'Directory' )
  if directories:
    for directory in directories:
      res = dm.getReplicasFromDirectory( directory )
      if not res['OK']:
        print res['Message']
        continue
      lfnReplicas.update( res['Value'] )
  else:
    if not lfns:
      bkQuery = dmScript.getBKQuery()
      if not set( bkQuery.getQueryDict() ) - set( ['Visible', 'ReplicaFlag'] ):
        print "Invalid BK query:", bkQuery
        DIRAC.exit( 2 )
      print "Executing BK query:", bkQuery
      lfns = bkQuery.getLFNs()
    if lfns:
      res = dm.getReplicas( lfns )
      if not res['OK']:
        print res['Message']
        DIRAC.exit( 2 )
      lfnReplicas = res['Value']['Successful']
      if res['Value']['Failed']:
        repStats[0] = len( res['Value']['Failed'] )
        withReplicas[0] = res['Value']['Failed'].keys()
        for lfn in res['Value']['Failed']:
          noReplicas[lfn] = -1
    else:
      lfnReplicas = None

  if repStats.get( 0 ):
    print "%d files found without a replica" % repStats[0]
  if not lfnReplicas:
    print "No files found that have a replica...."
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
      r = FileCatalog().getFileSize( lfns )
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
        repStats[-100] = repStats.setdefault( -100, 0 ) + 1
        if nrep == 0:
          repStats[-101] = repStats.setdefault( -101, 0 ) + 1
        SEs.remove( se )
      if se.endswith( "-ARCHIVE" ):
        nrep -= 1
        narchive -= 1
    repStats[nrep] = repStats.setdefault( nrep, 0 ) + 1
    withReplicas.setdefault( nrep, [] ).append( lfn )
    withArchives.setdefault( -narchive - 1, [] ).append( lfn )
    if nrep == 0:
      noReplicas[lfn] = -narchive - 1
    # narchive is negative ;-)
    repStats[narchive] = repStats.setdefault( narchive, 0 ) + 1
    for se in replicas:
      if se not in repSEs:
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
  if -100 in repStats:
    print "Failover replicas:", repStats[-100], "files"
    if -101 in repStats:
      print "   ...of which %d are only in Failover" % repStats[-101]
    else:
      print "   ...but all of them are also somewhere else"
  if maxArch:
    for nrep in range( 1, maxArch + 1 ):
      print nrep - 1, "archives:", repStats.setdefault( -nrep, 0 ), "files"
  for nrep in range( maxRep + 1 ):
    print nrep, "replicas:", repStats.setdefault( nrep, 0 ), "files"

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
        if site not in repSites:
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

  if prNoReplicas and noReplicas:
    print "\nFiles without a disk replica:"
    for rep in sorted( noReplicas ):
      print rep, "(%d archives)" % noReplicas[rep]

  if type( prWithArchives ) == type( [] ):
    for n in [n for n in prWithArchives if n in withArchives]:
      print '\nFiles with %d archives:' % n
      for rep in sorted( withArchives[n] ):
        print rep

  if type( prWithReplicas ) == type( [] ):
    for n in [n for n in prWithReplicas if n in withReplicas]:
      print '\nFiles with %d disk replicas:' % n
      for rep in sorted( withReplicas[n] ):
        print rep
