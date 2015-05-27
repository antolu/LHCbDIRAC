#!/usr/bin/env python

from DIRAC.Core.Base import Script
from DIRAC import exit, gLogger, S_OK
import os, sys
from time import time

def chown( directories, user, group = 'lhcb_user', recursive = False, ndirs = None ):
  if isinstance( directories, basestring ):
    directories = [directories]
  if ndirs is None:
    ndirs = 0
  res = dfc.changePathOwner( dict.fromkeys( directories, user ) )
  if not res['OK']:
    res = dfc.changePathOwner( dict.fromkeys( directories, {'Owner' : user} ) )
    if not res['OK']:
      return res
  res = dfc.changePathGroup( dict.fromkeys( directories, group ) )
  if not res['OK']:
    res = dfc.changePathGroup( dict.fromkeys( directories, {'Group':group} ) )
    if not res['OK']:
      return res
  if recursive:
    for subDir in directories:
      if ndirs % 10 == 0:
        sys.stdout.write( '.' )
        sys.stdout.flush()
      ndirs += 1
      res = dfc.listDirectory( subDir )
      if res['OK']:
        subDirectories = res['Value']['Successful'][subDir]['SubDirs']
        if subDirectories:
          # print subDir, len( subDirectories ), ndirs
          res = chown( subDirectories, user, recursive = True, ndirs = ndirs )
          if not res['OK']:
            return res
          ndirs = res['Value']
  return S_OK( ndirs )


Script.registerSwitch( '', 'User=', '  User name' )
Script.registerSwitch( '', 'Recursive', '  Set ownership recursively' )
Script.registerSwitch( '', 'Directory=', '   Directory to change the ownership of' )
Script.registerSwitch( '', 'Group=', '   Group name (default lhcb_user)' )

Script.parseCommandLine()

user = None
recursive = False
group = 'lhcb_user'
baseDir = None
for switch in Script.getUnprocessedSwitches():
  if switch[0] == 'User':
    user = switch[1].lower()
  elif switch[0] == 'Recursive':
    recursive = True
  elif switch[0] == 'Directory':
    baseDir = switch[1]
  elif switch[0] == 'Group':
    group = switch[1]

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
dfc = FileCatalogClient()
if baseDir:
  if not baseDir.startswith( '/lhcb' ):
    gLogger.fatal( "This is not a valid directory", directory )
    exit ( 1 )
  if not group.startswith( 'lhcb_' ):
    gLogger.fatal( "This is not a valid group", group )
    exit( 1 )
  if not dfc.isDirectory( baseDir ).get( 'Value', {} ).get( 'Successful', {} ).get( baseDir ):
    gLogger.fatal( "Directory doesn't exist", baseDir )
    exit( 1 )
  startTime = time()
  res = chown( baseDir, user, group = group, recursive = recursive )
  if not res['OK']:
    gLogger.fatal( 'Error changing directory owner', res['Message'] )
    exit( 2 )
  gLogger.always( 'Successfully changed owner in %d directories in %.1f seconds' % ( res['Value'], time() - startTime ) )
  exit( 0 )


if user is None:
  Script.showHelp()
  exit( 1 )

exists = False
initial = user[0]
baseDir = os.path.join( '/lhcb', 'user', initial, user )
subDirectories = []
if dfc.isDirectory( baseDir ).get( 'Value', {} ).get( 'Successful', {} ).get( baseDir ):
  gLogger.always( 'Directory already existing', baseDir )
  exists = True
  res = dfc.listDirectory( baseDir )
  if res['OK']:
    success = res['Value']['Successful'][baseDir]
    subDirectories = success['SubDirs']
    if success.get( 'SubDirs' ) or success.get( 'Files' ):
      gLogger.always( 'Directory is not empty:', ' %d files / %d subdirectories' % ( len( success['Files'] ), len( success['SubDirs'] ) ) )
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
  gLogger.always( 'Change%s ownership of directory' % ' recursively' if recursive else '', baseDir )
res = chown( baseDir, user, recursive = False )
if not res['OK']:
  gLogger.fatal( 'Error changing directory owner', res['Message'] )
  exit( 2 )
if recursive:
  startTime = time()
  res = chown( subDirectories, user, recursive = True, ndirs = 1 )
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
