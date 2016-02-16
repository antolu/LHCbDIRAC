#!/usr/bin/env python
"""
    Set a (set of) LFNs as problematic in the LFC and in the BK and transformation system if only one replica
"""
__RCSID__ = "$Id: dirac-dms-set-problematic-files.py 77175 2014-08-11 13:32:45Z phicharp $"
__VERSION__ = "$Revision: 77175 $"

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

