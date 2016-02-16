#! /usr/bin/env python
""" Show/set the pilot version. In the CS and/or in the JSON file that might be used.
"""

# FIXME: gConfig.getValue( '/DIRAC/Extensions' ) to move to Extensions().getCSExtensions() from CSGlobals as of v6r14

__RCSID__ = "$Id$"

# Usual script stuff
from DIRAC.Core.Base import Script

Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile]' % Script.scriptName ] ) )
Script.registerSwitch( 'S:', 'set=', "set the pilot version to use for the current setup. Requires write permission." )
Script.registerSwitch( 'F:', 'file=', "file location. If not set, what is found here is used" )
Script.registerSwitch( 'U:', 'update=', "Update the symbolic links on the web area to the current release scripts" )


Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()

setVersion = False
fileLocation = ''
updatePilotScript = False
global LHCbVn
LHCbVn = ''

for unprocSw in Script.getUnprocessedSwitches():
  if unprocSw[0] in ( "S", "set" ):
    setVersion = True
    version = unprocSw[1].replace( ' ', '' ).split( ',' )
    LHCbVn = version[0]
  elif unprocSw[0] in ( "F", "file" ):
    fileLocation = unprocSw[1]
  elif unprocSw[0] in ( "U", "update" ):
    updatePilotScript = True
    DiracVersion = unprocSw[1].replace( ' ', '' ).split( ',' )[0]


# Actual logic
import urllib2
import json
import os
# import re
from DIRAC import gConfig, gLogger
from DIRAC import exit as DIRACExit
from DIRAC.ConfigurationSystem.Client.CSAPI import CSAPI
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.Core.Security.ProxyInfo import getProxyInfo

# This is a standard location - can be changed with a switch
if not fileLocation:
  fileLocation = 'http://lhcbproject.web.cern.ch/lhcbproject/dist/DIRAC3/defaults/%s-pilot.json' % gConfig.getValue( '/DIRAC/Extensions' )

# functions
def showVersion():
  """ show the pilot versions set right now
  """
  # CS version
  global LHCbVn
  csVersion = Operations().getValue( "Pilot/Version", [] )
  print "Version specified in the CS: %s" % ', '.join( csVersion )
  # file version
  if not LHCbVn:
    LHCbVn = csVersion[0]
  try:
    remoteFD = urllib2.urlopen( fileLocation )
    fileV = json.load( remoteFD )
    versionsInFile = [str( x ) for x in fileV[gConfig.getValue( '/DIRAC/Setup' )]['Version']]
    print "Version specified in the file: %s" % ', '.join( versionsInFile )
  except urllib2.HTTPError, x:
    print "Can't find the file in %s" % fileLocation, x


# main
print "Versions set right now:"
showVersion()

if setVersion:
  if 'CSAdministrator' not in getProxyInfo()['Value']['groupProperties']:
    gLogger.error( "You need CSAdministrator property to modify the CS" )
    DIRACExit( 1 )
  print "Going to set the pilot version"
  # CS version
  csAPI = CSAPI()
  res = csAPI.modifyValue( "/Operations/%s/Pilot/Version" % gConfig.getValue( '/DIRAC/Setup' ), ','.join( version ) )
  if not res['OK']:
    gLogger.error( res['Message'] )
    DIRACExit( 1 )
  print res['Value']
  res = csAPI.commit()
  if not res['OK']:
    gLogger.error( res['Message'] )
    DIRACExit( 1 )
  # file version
  fp = open( "/afs/cern.ch/lhcb/distribution/Dirac_project/defaults/%s-pilot.json" % gConfig.getValue( '/DIRAC/Extensions' ), 'r' )
  fileVersion = json.load( fp )
  fp.close()
  newFileVersion = dict( fileVersion )
  newFileVersion[gConfig.getValue( '/DIRAC/Setup' )]['Version'] = version
  fp = open( "/afs/cern.ch/lhcb/distribution/Dirac_project/defaults/%s-pilot.json" % gConfig.getValue( '/DIRAC/Extensions' ), 'w' )
  json.dump( newFileVersion, fp )
  fp.close()

  print "Versions set now:"
  showVersion()

if  updatePilotScript:
  LHCbsrc = os.path.join( "/afs/cern.ch/lhcb/software/releases/LHCBDIRAC/LHCBDIRAC_" + LHCbVn, "LHCbDIRAC/WorkloadManagementSystem/PilotAgent/LHCbPilotCommands.py" )
  dst = "/afs/cern.ch/lhcb/project/web/lbdirac/Operations/VM/pilotscripts/"
  for item in os.listdir( dst ):
    os.unlink( os.path.join( dst, item ) )
  os.symlink( LHCbsrc, os.path.join ( dst, "LHCbPilotCommands.py" ) )
  # try:
  #  releases = urllib2.urlopen( "http://svnweb.cern.ch/world/wsvn/dirac/LHCbDIRAC/trunk/LHCbDIRAC/releases.cfg" )
  #  releases = re.findall( "\sDIRAC:.+", releases.read() )
  #  DiracVersion = ( releases[1].split( ':' ) )[1]
  # except urllib2.HTTPError, x:
  #  print "Can't find the DiracVersion", x
  Diracsrc = "/afs/cern.ch/lhcb/software/releases/DIRAC/DIRAC_" + DiracVersion
  DiracPath = os.path.join ( Diracsrc, "DIRAC/WorkloadManagementSystem/PilotAgent/" )
  for item in os.listdir( DiracPath ):
    dest = os.path.join( dst, item )
    if  os.path.isfile( dest ):
      os.symlink( os.path.join( DiracPath, item ), dest )
  os.symlink( os.path.join( Diracsrc, "DIRAC/Core/scripts/dirac-install.py" ), os.path.join( dst , "dirac-install.py" ) )
  print "Pilot scripts link updated"




