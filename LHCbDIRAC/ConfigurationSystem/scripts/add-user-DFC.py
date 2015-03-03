#!/usr/bin/env python

from DIRAC.Core.Base import Script
from DIRAC import exit, gLogger
import os, sys

Script.registerSwitch( '', 'User=', '  User name' )

Script.parseCommandLine()

user = None
for switch in Script.getUnprocessedSwitches():
  if switch[0] == 'User':
    user = switch[1].lower()

if not user:
  Script.showHelp()
  exit( 1 )

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
if not exists:
  gLogger.always( 'Creating directory', baseDir )
  res = dfc.createDirectory( baseDir )
  if not res['OK']:
    gLogger.fatal( 'Error creating directory', res['Message'] )
    exit( 2 )
else:
  gLogger.always( 'Change ownership of directory', baseDir )
res = dfc.changePathOwner( {baseDir: { 'Owner' : user}}, False )
if not res['OK']:
  gLogger.fatal( 'Error changing directory owner', res['Message'] )
  exit( 2 )
res = dfc.changePathGroup( {baseDir: { 'Group' : 'lhcb_user'}}, False )
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
