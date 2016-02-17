#!/usr/bin/env python
"""
    Set a (set of) LFNs as problematic in the FC and in the BK and transformation system if all replicas are problematic
"""
__RCSID__ = "$Id$"
__VERSION__ = "$Revision: 87258 $"

from DIRAC.Core.Base import Script
from LHCbDIRAC.DataManagementSystem.Client.DMScript import DMScript

if __name__ == "__main__":

  dmScript = DMScript()
  dmScript.registerFileSwitches()
  dmScript.registerSiteSwitches()

  Script.registerSwitch( '', 'Reset', '   Reset files to OK' )
  Script.registerSwitch( '', 'Full', '   Give full list of files' )
  Script.registerSwitch( '', 'NoAction', '   No action taken, just give stats' )

  Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                       'Usage:',
                                       '  %s [option|cfgfile] [<LFN>] [<LFN>...]' % Script.scriptName, ] ) )

  Script.parseCommandLine( ignoreErrors = False )

  from LHCbDIRAC.DataManagementSystem.Client.ScriptExecutors import executeSetProblematicFiles
  executeSetProblematicFiles( dmScript )

