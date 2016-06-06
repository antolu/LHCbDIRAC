#!/usr/bin/env python
########################################################################
# File :    dirac-bookkeeping-get-file-sisters
# Author :  Zoltan Mathe
########################################################################
"""
  Report sisters or cousins (i.e. descendant of a parent or ancestor) for a (list of) LFN(s)
"""
__RCSID__ = "$Id$"

from LHCbDIRAC.DataManagementSystem.Client.DMScript import DMScript, Script

if __name__ == "__main__":

  dmScript = DMScript()
  dmScript.registerFileSwitches()
  Script.registerSwitch( '', 'All', 'Do not restrict to sisters with replicas' )
  Script.registerSwitch( '', 'Full', 'Get full metadata information on sisters' )
  level = 1
  Script.registerSwitch( '', 'Depth=', 'Number of ancestor levels (default:%d), 2 would be cousins, 3 grand-cousins etc...' % level )
  Script.registerSwitch( '', 'Production=', 'Production to check for sisters (default=same production)' )
  Script.registerSwitch( '', 'AllFileTypes', 'Consider also files with a different type' )
  Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                       'Usage:',
                                       '  %s [option|cfgfile] ... [LFN|File] [Level]' % Script.scriptName ] ) )

  Script.parseCommandLine( ignoreErrors = True )

  from LHCbDIRAC.BookkeepingSystem.Client.ScriptExecutors import executeFileSisters
  executeFileSisters( dmScript )