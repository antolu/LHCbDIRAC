#!/usr/bin/env python
########################################################################
# File :    dirac-bookkeeping-get-file-descendants
# Author :  Zoltan Mathe
########################################################################
"""
  Returns descendants for a (list of) LFN(s)
"""
__RCSID__ = "$Id$"

from LHCbDIRAC.DataManagementSystem.Client.DMScript import DMScript, Script

if __name__ == "__main__":
  dmScript = DMScript()
  dmScript.registerFileSwitches()
  Script.registerSwitch( '', 'All', 'Do not restrict to descendants with replicas' )
  Script.registerSwitch( '', 'Full', 'Get full metadata information on descendants' )
  level = 1
  Script.registerSwitch( '', 'Depth=', 'Number of processing levels (default:%d)' % level )
  Script.registerSwitch( '', 'Production=', 'Restrict to descendants in a given production (at any depth)' )
  Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                       'Usage:',
                                       '  %s [option|cfgfile] ... [LFN|File] [Depth]' % Script.scriptName,
                                       'Arguments:',
                                       '  LFN:      Logical File Name',
                                       '  File:     Name of the file with a list of LFNs',
                                       '  Depth:    Number of levels to search (default: %d)' % level ] ) )

  Script.parseCommandLine( ignoreErrors = True )

  from LHCbDIRAC.BookkeepingSystem.Client.ScriptExecutors import executeFileDescendants
  executeFileDescendants( dmScript, level )
