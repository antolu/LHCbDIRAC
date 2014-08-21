#! /usr/bin/env python
""" Show/set the pilot version. In the CS and/or in the JSON file that might be used.
"""

__RCSID__ = "$Id$"

# Usual script stuff
from DIRAC.Core.Base import Script

Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile]' % Script.scriptName ] ) )
Script.registerSwitch( 'S:', 'set=', "set the pilot version to use for the current setup. Requires write permission." )
Script.registerSwitch( 'F:', 'file=', "file location. If not set, what is found here is used" )

Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()

setVersion = False
fileLocation = ''

for unprocSw in Script.getUnprocessedSwitches():
  if unprocSw[0] in ( "S", "set" ):
    setVersion = True
    version = unprocSw[1].replace( ' ', '' ).split( ',' )
  elif unprocSw[0] in ( "F", "file" ):
    fileLocation = unprocSw[1]


# Actual logic
import urllib2
import json
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
  csVersion = Operations().getValue( "Pilot/Version", [] )
  print "Version specified in the CS: %s" % ', '.join( csVersion )
  # file version
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
