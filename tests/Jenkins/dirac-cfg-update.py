#!/usr/bin/env python
""" update local cfg
"""

from DIRAC.Core.Base import Script
Script.parseCommandLine()

import os

from DIRAC.Core.Utilities.CFG import CFG

localCfg = CFG()
localConfigFile = os.path.join( '.', 'pilot.cfg' )
localCfg.loadFromFile( localConfigFile )
if not localCfg.isSection( '/LocalSite' ):
  localCfg.createNewSection( '/LocalSite' )
localCfg.setOption( '/LocalSite/CPUTimeLeft', 5000 )
localCfg.writeToFile( localConfigFile )
