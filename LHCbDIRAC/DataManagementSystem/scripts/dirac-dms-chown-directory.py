#!/usr/bin/env python

from DIRAC.Core.Base import Script
from DIRAC import exit, gLogger, S_OK
import os, sys
from time import time

Script.registerSwitch( '', 'User=', '  User name' )
Script.registerSwitch( '', 'Recursive', '  Set ownership recursively' )
Script.registerSwitch( '', 'Directory=', '   Directory to change the ownership of' )
Script.registerSwitch( '', 'Group=', '   Group name (default lhcb_user)' )
Script.registerSwitch( '', 'Mode=', '   Change permission mode (default: not changed)' )
Script.registerSwitch( '', 'Create', '   Use for creating the directory if it does not exist' )

Script.parseCommandLine()

user = None
recursive = False
group = None
baseDir = None
mode = None
create = False
for switch in Script.getUnprocessedSwitches():
  if switch[0] == 'User':
    user = switch[1].lower()
  elif switch[0] == 'Recursive':
    recursive = True
  elif switch[0] == 'Directory':
    baseDir = switch[1]
    if baseDir.endswith( '/' ):
      baseDir = baseDir[:-1]
  elif switch[0] == 'Group':
    group = switch[1]
  elif switch[0] == 'Mode':
    mode = int( switch[1], base = 8 )
  elif switch[0] == 'Create':
    create = True

args = Script.getPositionalArgs()
if baseDir is None and args:
  baseDir = args[0]

from DIRAC.Core.Security.ProxyInfo import getProxyInfo
from LHCbDIRAC.DataManagementSystem.Utilities.FCUtilities import chown

res = getProxyInfo()
if not res['OK']:
  gLogger.fatal( "Can't get proxy info", res['Message'] )
  exit( 1 )
properties = res['Value'].get( 'groupProperties', [] )
if not 'FileCatalogManagement' in properties:
  gLogger.error( "You need to use a proxy from a group with FileCatalogManagement" )
  exit( 5 )

from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient
dfc = FileCatalogClient()
if baseDir is None or ( user is None and group is None and mode is None ):
  Script.showHelp()
  exit( 1 )

if not baseDir.startswith( '/lhcb' ):
  gLogger.fatal( "Not a valid directory", baseDir )
  exit ( 1 )
if group and not group.startswith( 'lhcb_' ):
  gLogger.fatal( "This is not a valid group", group )
  exit( 1 )
if not dfc.isDirectory( baseDir ).get( 'Value', {} ).get( 'Successful', {} ).get( baseDir ):
  if create:
    res = dfc.createDirectory( baseDir )
    if not res['OK']:
      gLogger.fatal( "Error creating directory", res['Message'] )
      exit( 2 )
  else:
    gLogger.fatal( "Directory doesn't exist", baseDir )
    exit( 1 )
startTime = time()
res = chown( baseDir, user, group = group, mode = mode, recursive = recursive, fcClient = dfc )
if not res['OK']:
  gLogger.fatal( 'Error changing directory owner', res['Message'] )
  exit( 2 )
gLogger.always( 'Successfully changed owner in %d directories in %.1f seconds' % ( res['Value'], time() - startTime ) )

from LHCbDIRAC.DataManagementSystem.Client.DMScript import DMScript
from LHCbDIRAC.DataManagementSystem.Client.ScriptExecutors import executeLfnMetadata
dmScript = DMScript()
dmScript.setLFNs( baseDir )
sys.stdout.write( 'Directory metadata: ' )
sys.stdout.flush()
executeLfnMetadata( dmScript )
exit( 0 )
