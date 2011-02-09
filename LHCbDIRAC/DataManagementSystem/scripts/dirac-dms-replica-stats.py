#!/usr/bin/env python
########################################################################
# $HeadURL: svn+ssh://svn.cern.ch/reps/dirac/DIRAC/tags/LHCbDIRAC/DataManagementSystem/dm_2010121301-v5r11p1/scripts/dirac-dms-replica-stats.py $
########################################################################

"""
   Get statistics on number of replicas for a given directory or production
"""

__VERSION__ = "$ $"

import DIRAC
from DIRAC.Core.Utilities.List                        import sortList, breakListIntoChunks
from DIRAC.Core.Base import Script
from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.Core.Utilities.SiteSEMapping                                import getSitesForSE, getSEsForSite
import sys

fileType = ''
directory = ''
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
    directory = switch[1]
  if switch[0].lower() == "t" or switch[0].lower() == "type":
    fileType = switch[1]
  if switch[0].lower() == "p" or switch[0].lower() == "prod":
    prods = switch[1].split( ',' )
  if switch[0].lower() in ["s", "size"]:
    getSize = True

rm = ReplicaManager()
import os, sys


from LHCbDIRAC.DataManagementSystem.Client.StorageUsageClient  import StorageUsageClient
directories = []
if fileType or prods[0] != '':
  for prod in prods:
    res = StorageUsageClient().getStorageDirectoryData( directory, fileType, prod, [] )
    if not res['OK']:
      print "Failed to get directories for production", prod, res['Message']
    else:
      directories.extend( res['Value'].keys() )
elif directory:
  directories = [directory]

if len( directories ) == 0:
  print "No directories to get statistics for"
  DIRAC.exit( 2 )

repStats = {}
repSEs = {}
repSites = {}
maxRep = 0
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
    for se in list( SEs ):
      if se.endswith( "-FAILOVER" ):
        nrep -= 1
        if not repStats.has_key( -1 ):
          repStats[-1] = 0
        repStats[-1] += 1
    if not repStats.has_key( nrep ):
      repStats[nrep] = 0
    repStats[nrep] += 1
    for se in replicas.keys():
      if not repSEs.has_key( se ):
        repSEs[se] = [0, 0]
      repSEs[se][0] += 1
      if getSize:
        repSEs[se][1] += lfnSize[lfn]

    if nrep > maxRep: maxRep = nrep
    nfiles += 1

GB = 1000. * 1000. * 1000.
TB = GB * 1000.
if totSize:
  print "%d files found (%.3f GB) in" % ( nfiles, totSize / GB ), directories
else:
  print "%d files found in" % ( nfiles ), directories
print "\nReplica statistics:"
if repStats.has_key( -1 ):
  print "Failover replicas:", repStats[-1], "files"
for nrep in range( maxRep + 1 ):
  if not repStats.has_key( nrep ):
    repStats[nrep] = 0
  print nrep, "replicas:", repStats[nrep], "files"

print "\nSE statistics:"
for se in sortList( repSEs.keys() ):
  if se.endswith( "-FAILOVER" ): continue
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


