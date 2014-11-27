#!/usr/bin/env python
""" update local cfg
"""

from DIRAC.Core.Base import Script
Script.parseCommandLine()

import os

from DIRAC.Core.Utilities.CFG import CFG

localCfg = CFG()

localConfigFile = os.path.join( '.', 'etc', 'Production.cfg' )
localCfg.loadFromFile( localConfigFile )

os.makedirs( '/scratch/workspace/lhcbdirac_certification/sandboxes' )
# localCfg.createNewSection( 'Systems/WorkloadManagement/Production/Services/SandboxStore/' )
localCfg.setOption( 'Systems/WorkloadManagement/Production/Services/SandboxStore/BasePath',
                    '/scratch/workspace/lhcbdirac_certification/sandboxes' )
localCfg.writeToFile( localConfigFile )

