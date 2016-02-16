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
dirList = None
mode = None
create = False
for switch in Script.getUnprocessedSwitches():
  if switch[0] == 'User':
    user = switch[1].lower()
  elif switch[0] == 'Recursive':
    recursive = True
  elif switch[0] == 'Directory':
    dirList = switch[1].split( ',' )
  elif switch[0] == 'Group':
    group = switch[1]
  elif switch[0] == 'Mode':
    mode = int( switch[1], base = 8 )
  elif switch[0] == 'Create':
    create = True

args = Script.getPositionalArgs()
if dirList is None and args:
  dirList = args

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

if group and not group.startswith( 'lhcb_' ):
  gLogger.fatal( "This is not a valid group", group )
  exit( 1 )

directories = set()
for baseDir in dirList:
  if os.path.exists( baseDir ):
    f = open( baseDir )
    directories.update( [os.path.dirname( line.split()[0] + '/' ) for line in f.read().splitlines()] )
    f.close()
  else:
    directories.add( baseDir if not baseDir.endswith( '/' ) else baseDir[:-1] )

if not directories or ( user is None and group is None and mode is None ):
  Script.showHelp()
  exit( 1 )

from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient
dfc = FileCatalogClient()

error = 0
success = 0
startTime = time()
if len( directories ) > 1:
  sys.stdout.write( 'Changing ownership to %d directories :' % len( directories ) )
  sys.stdout.flush()
count = 0
for baseDir in directories:
  if not baseDir.startswith( '/lhcb' ):
    gLogger.fatal( "\nNot a valid directory", baseDir )
    error = 1
    continue
  if not dfc.isDirectory( baseDir ).get( 'Value', {} ).get( 'Successful', {} ).get( baseDir ):
    if create:
      res = dfc.createDirectory( baseDir )
      if not res['OK']:
        gLogger.fatal( "\nError creating directory", res['Message'] )
        error = 1
        continue
    else:
      gLogger.fatal( "\nDirectory doesn't exist", baseDir )
      error = 1
      continue
  res = chown( baseDir, user, group = group, mode = mode, recursive = recursive, fcClient = dfc )
  if not res['OK']:
    gLogger.fatal( '\nError changing directory owner', res['Message'] )
    error = 1
    continue

  if len( directories ) == 1:
    gLogger.always( 'Successfully changed owner in %d directories in %.1f seconds' % ( res['Value'], time() - startTime ) )
    from LHCbDIRAC.DataManagementSystem.Client.DMScript import DMScript
    from LHCbDIRAC.DataManagementSystem.Client.ScriptExecutors import executeLfnMetadata
    dmScript = DMScript()
    dmScript.setLFNs( baseDir )
    sys.stdout.write( 'Directory metadata: ' )
    sys.stdout.flush()
    executeLfnMetadata( dmScript )
  elif count % 10 == 0:
    sys.stdout.write( '.' )
    sys.stdout.flush()
  count += 1
  success += res['Value']
  error = 0

if len( directories ) > 1:
  gLogger.notice( '\nSuccessfully changed owner in %d directories in %.1f seconds' % ( success, time() - startTime ) )
exit( error )
