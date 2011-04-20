#!/usr/bin/env python
########################################################################
# $HeadURL: svn+ssh://svn.cern.ch/reps/dirac/DIRAC/tags/LHCbDIRAC/DataManagementSystem/dm_2010121301-v5r11p1/scripts/dirac-dms-replica-stats.py $
########################################################################

"""
   Get statistics on number of replicas for a given directory or production
"""

__RCSID__ = "$Id:  $"

from DIRAC.Core.Base import Script
import os, sys

def orderSEs( listSEs ):
  listSEs = sortList( listSEs )
  orderedSEs = [se for se in listSEs if se.endswith( "-ARCHIVE" )]
  orderedSEs += [se for se in listSEs if not se.endswith( "-ARCHIVE" )]
  return orderedSEs

fileType = ''
directories = []
prods = ['']
getSize = False
Script.registerSwitch( "D:", "Dir=", "   Dir to search [ALL]" )
Script.registerSwitch( "S:", "Size", "   Get the LFN size [No]" )
Script.registerSwitch( "t:", "Type=", "   File type to search [ALL]" )
Script.registerSwitch( "p:", "Prod=", "   Production ID to search [ALL] (space or comma separated list)" )
Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ...' % Script.scriptName, ] ) )
Script.addDefaultOptionValue( 'LogLevel', 'error' )
Script.parseCommandLine( ignoreErrors = False )

for switch in Script.getUnprocessedSwitches():
  if switch[0] == "D" or switch[0].lower() == "dir":
    directories = switch[1].split( ',' )
  if switch[0].lower() == "t" or switch[0].lower() == "type":
    fileType = switch[1]
  if switch[0].lower() == "p" or switch[0].lower() == "prod":
    prods = switch[1].split( ',' )
  if switch[0].lower() in ["s", "size"]:
    getSize = True


import DIRAC
from DIRAC.Core.Utilities.List                        import sortList, breakListIntoChunks
from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.Core.Utilities.SiteSEMapping                                import getSitesForSE, getSEsForSite
from LHCbDIRAC.DataManagementSystem.Client.StorageUsageClient  import StorageUsageClient

rm = ReplicaManager()

pr = []
for p in prods:
  if p.find( ":" ) > 0:
    p = p.split( ":" )
    for i in range( int( p[0] ), int( p[1] ) + 1 ):
      pr.append( str( i ) )
  else:
    pr.append( p )
prods = pr

if fileType or prods[0] != '':
  for prod in prods:
    res = StorageUsageClient().getStorageDirectoryData( '', fileType, prod, [] )
    if not res['OK']:
      print "Failed to get directories for production", prod, res['Message']
    else:
      for directory in  res['Value'].keys():
        if directory.find( "/LOG/" ) < 0:
          directories.append( directory )

if len( directories ) == 0:
  print "No directories to get statistics for"
  DIRAC.exit( 2 )

repStats = {}
repSEs = {}
repSites = {}
maxRep = 0
maxArch = 0
nfiles = 0
totSize = 0
for directory in directories:
  res = rm.getReplicasFromDirectory( directory )
  if not res['OK']:
    print res['Message']
    continue
  if getSize:
    sys.stdout.write( "... getting size for %d LFNs, be patient...\r" % len( res['Value'] ) )
    sys.stdout.flush()
    lfnSize = {}
    for lfns in breakListIntoChunks( res['Value'].keys(), 100 ):
      r = rm.getCatalogFileSize( lfns )
      if r['OK']:
        lfnSize.update( r['Value']['Successful'] )
    for lfn, size in lfnSize.items():
      totSize += size
  for lfn, replicas in res['Value'].items():
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
    if - narchive > maxArch: maxArch = -narchive
    nfiles += 1

GB = 1000. * 1000. * 1000.
TB = GB * 1000.
if totSize:
  print "%d files found (%.3f GB) in" % ( nfiles, totSize / GB ), directories
else:
  print "%d files found in" % ( nfiles ), directories
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


