#!/usr/bin/env python
""" update local cfg
"""

from DIRAC.Core.Base import Script

Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgFile] ... DB ...' % Script.scriptName,
                                     'Arguments:',
                                     '  setup: Name of the build setup (mandatory)'] ) )

Script.parseCommandLine()

args = Script.getPositionalArgs()

if len( args ) < 1:
  Script.showHelp()
  exit( -1 )

setupName = args[0]

import os

from DIRAC.Core.Utilities.CFG import CFG

localCfg = CFG()

localConfigFile = os.path.join( '.', 'etc', 'Production.cfg' )
localCfg.loadFromFile( localConfigFile )

os.makedirs( '/scratch/workspace/%s/sandboxes' % setupName )
# localCfg.createNewSection( 'Systems/WorkloadManagement/Production/Services/SandboxStore/' )
localCfg.setOption( 'Systems/WorkloadManagement/Production/Services/SandboxStore/BasePath',
                    '/scratch/workspace/%s/sandboxes' % setupName )
localCfg.writeToFile( localConfigFile )

