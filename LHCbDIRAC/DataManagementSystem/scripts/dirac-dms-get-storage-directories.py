#!/usr/bin/env python
########################################################################
__RCSID__ = "$Id$"

from DIRAC.Core.Base import Script

folder = ''
fileType = ''
ses = []
prods = []
Script.registerSwitch( "f:", "Dir=", "   Dir to search [ALL]" )
Script.registerSwitch( "t:", "Type=", "   File type to search [ALL]" )
Script.registerSwitch( "S:", "SEs=", "  SEs to consider [ALL] (space or comma separated list)" )
Script.registerSwitch( "p:", "Prod=", "   Production ID to search [ALL] (space or comma separated list)" )

Script.setUsageMessage( """
Get summary of storage directory usage

Usage:
   %s [option]
""" % Script.scriptName )

Script.parseCommandLine()

from LHCbDIRAC.DataManagementSystem.Client.StorageUsageClient import StorageUsageClient

rpc = StorageUsageClient()

from DIRAC.Core.Utilities.List import sortList

prods = [ int( x ) for x in Script.getPositionalArgs() ]

for switch in Script.getUnprocessedSwitches():
  if switch[0].lower() in ( "f", "dir" ):
    folder = switch[1]
  if switch[0].lower() in ( "t", "type" ):
    fileType = switch[1]
  if switch[0].lower() in ( "c",  "ses" ):
    ses = switch[1].replace( ',', ' ' ).split()
  if switch[0].lower() in ( "p", "prod" ):
    prods = [ int( x.strip() ) for x in switch[1].split(",") if x.strip() ]

allDirs = []
if not prods:
  res = rpc.getStorageDirectoryData( folder, fileType, '', ses )
  if not res['OK']:
    print "ERROR getting storage directories", res['Message']
  else:
    for resDir in res['Value']:
      if resDir not in allDirs:
        allDirs.append( resDir )
for prod in prods:
  res = rpc.getStorageDirectoryData( folder, fileType, prod, ses )
  if not res['OK']:
    print "ERROR getting storage directories", res['Message']
  else:
    for resDir in res['Value']:
      if resDir not in allDirs:
        allDirs.append( resDir )

for folder in sortList( allDirs ):
  print dir
