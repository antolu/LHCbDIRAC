#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :    dirac-lhcb-manage-software
# Author :  Stuart Paterson
########################################################################
__RCSID__ = "$Id$"
import sys
import DIRAC
from DIRAC.Core.Base import Script
from DIRAC.ConfigurationSystem.Client.LocalConfiguration import LocalConfiguration
from DIRAC.Core.Base.AgentReactor                        import AgentReactor
from DIRAC import gConfig

Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()

def usage():
  print 'Usage: %s [<PATH TO SOFTWARE CACHE>]' % ( Script.scriptName )
  print 'If not specified, path to software cache taken from /LocalSite/SharedArea'
  DIRAC.exit( 2 )

if len( args ) > 2:
  usage()

if not args:
  sharedArea = gConfig.getValue( '/LocalSite/SharedArea' )
else:
  sharedArea = args[0]

exitCode = 0
agentName = 'LHCb/SoftwareManagementAgent'
localCfg = LocalConfiguration()
localCfg.addDefaultEntry( '/LocalSite/SharedArea', sharedArea )
localCfg.setConfigurationForAgent( agentName )
resultDict = localCfg.loadUserData()
if not resultDict[ 'OK' ]:
  print "There were errors when loading configuration", resultDict['Message']
  sys.exit( 1 )

agent = AgentReactor( agentName )
result = agent.runNumCycles( numCycles = 1 )
if not result['OK']:
  print 'ERROR %s' % result['Message']
  exitCode = 2

DIRAC.exit( exitCode )
