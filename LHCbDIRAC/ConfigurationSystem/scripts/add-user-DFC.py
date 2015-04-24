#!/usr/bin/env python

from DIRAC.Core.Base import Script
from DIRAC import exit, gLogger
import os, sys

Script.registerSwitch( '', 'User=', '  User name' )
Script.registerSwitch( '', 'Recursive', '  Set ownership recursively' )

Script.parseCommandLine()

user = None
recursive = False
for switch in Script.getUnprocessedSwitches():
  if switch[0] == 'User':
    user = switch[1].lower()
  elif switch[0] == 'Recursive':
    recursive = True

if not user:
  Script.showHelp()
  exit( 1 )

from DIRAC.Core.Security.ProxyInfo import getProxyInfo
res = getProxyInfo()
if not res['OK']:
  gLogger.fatal( "Can't get proxy info", res['Message'] )
  exit( 1 )
properties = res['Value'].get( 'groupProperties', [] )
if not 'FileCatalogManagement' in properties:
  gLogger.error( "You need to use a proxy from a group with FileCatalogManagement" )
  exit( 5 )

from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient
exists = False
initial = user[0]
dfc = FileCatalogClient()
baseDir = os.path.join( '/lhcb', 'user', initial, user )
if dfc.isDirectory( baseDir ).get( 'Value', {} ).get( 'Successful', {} ).get( baseDir ):
  gLogger.always( 'Directory already existing', baseDir )
  exists = True
  res = dfc.listDirectory( baseDir )
  if res['OK']:
    success = res['Value']['Successful'][baseDir]
    if success.get( 'SubDirs' ) or success.get( 'Files' ):
      gLogger.error( 'Directory is not empty:', ' %d files / %d subdirectories' % ( len( success['Files'] ), len( success['SubDirs'] ) ) )
    elif recursive:
      gLogger.always( "Empty directory, recursive is useless..." )
      recursive = False
if not exists:
  gLogger.always( 'Creating directory', baseDir )
  res = dfc.createDirectory( baseDir )
  if not res['OK']:
    gLogger.fatal( 'Error creating directory', res['Message'] )
    exit( 2 )
else:
  gLogger.always( 'Change ownership of directory', baseDir )
res = dfc.changePathOwner( {baseDir: { 'Owner' : user}}, recursive )
if not res['OK']:
  gLogger.fatal( 'Error changing directory owner', res['Message'] )
  exit( 2 )
res = dfc.changePathGroup( {baseDir: { 'Group' : 'lhcb_user'}}, recursive )
if not res['OK']:
  gLogger.fatal( 'Error changing directory group', res['Message'] )
  exit( 2 )

from LHCbDIRAC.DataManagementSystem.Client.DMScript import DMScript
from LHCbDIRAC.DataManagementSystem.Client.ScriptExecutors import executeLfnMetadata
dmScript = DMScript()
dmScript.setLFNs( baseDir )
sys.stdout.write( 'Directory metadata: ' )
sys.stdout.flush()
executeLfnMetadata( dmScript )
exit( 0 )
