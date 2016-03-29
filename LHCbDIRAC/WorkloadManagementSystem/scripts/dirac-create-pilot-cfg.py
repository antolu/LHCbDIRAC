#!/usr/bin/env python
"""
  Create pilot.cfg
"""
__RCSID__ = "$Id$"

import  DIRAC.Core.Base.Script as Script
from DIRAC.ConfigurationSystem.Client.Config import gConfig


Script.setUsageMessage( '\n'.join( ['Get the parameters (Memory and Number of processors) of a worker node',
                                    'Usage:',
                                    '%s [option]... [cfgfile]' % Script.scriptName,
                                    'Arguments:',
                                    ' cfgfile: DIRAC Cfg with description of the configuration (optional)'] ) )

ceName = ''
ceType = ''

def setCSAddress( args ):
  global csAddress
  csAddress = args

def setcfgName( args ):
  global cfgName
  cfgName = args

def setReleaseVersion( args ):
  global releaseVersion
  releaseVersion = args

Script.registerSwitch( "C:", "ConfigurationServer=", "ConfigurationServer address (Mandatory)", setCSAddress )
Script.registerSwitch( "N:", "cfgFileName=", "Cfg file name (Mandatory)", setcfgName )
Script.registerSwitch( "N:", "ReleaseVersion=", "ReleaseVersion (Mandatory)", setReleaseVersion )
Script.parseCommandLine( ignoreErrors = True )

gConfig.setOptionValue( '/DIRAC/Configuration/Servers', csAddress )
gConfig.setOptionValue( '/LocalInstallation/ConfigurationServer', csAddress )
gConfig.setOptionValue( '/LocalSite/ReleaseVersion', releaseVersion )
gConfig.dumpLocalCFGToFile( cfgName )

