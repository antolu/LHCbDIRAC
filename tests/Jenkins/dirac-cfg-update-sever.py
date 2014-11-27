#!/usr/bin/env python
""" update local cfg
"""

from DIRAC.Core.Base import Script
Script.parseCommandLine()

import os

from DIRAC.Core.Utilities.CFG import CFG

localCfg = CFG()

os.makedirs( '/scratch/workspace/lhcbdirac_certification/sandboxes' )
localCfg.setOption( 'WorkloadManagement/Production/Databases/SandboxStore/BasePath',
                    '/scratch/workspace/lhcbdirac_certification/sandboxes' )

