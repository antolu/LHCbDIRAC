#!/usr/bin/env python
########################################################################
# $HeadURL$
########################################################################
"""
Get the list of all the user files.
"""
__RCSID__ = "$Id$"

from LHCbDIRAC.DataManagementSystem.Client.DMScript import DMScript, Script

if __name__ == "__main__":

  days = 0
  months = 0
  years = 0
  depth = 1
  wildcard = '*'
  dmScript = DMScript()
  dmScript.registerNamespaceSwitches()
  Script.registerSwitch("", "Days=", "Match files older than number of days [%s]" % days)
  Script.registerSwitch("", "Months=", "Match files older than number of months [%s]" % months)
  Script.registerSwitch("", "Years=", "Match files older than number of years [%s]" % years)
  Script.registerSwitch("", "Wildcard=", "Wildcard for matching filenames [%s]" % wildcard)
  Script.registerSwitch('', 'Output', 'Write list to an output file')
  Script.registerSwitch("", "EmptyDirs", "Create a list of empty directories")
  Script.registerSwitch("", "Depth=", "Depth to which recursively browse (default = %d)" % depth)
  Script.registerSwitch("r", "Recursive", "Set depth to infinite")
  Script.registerSwitch('', 'NoDirectories', 'Only print out only files, not subdirectories')
  dmScript.registerBKSwitches()

  Script.setUsageMessage('\n'.join([__doc__.split('\n')[1],
                                    'Usage:',
                                    '  %s [option|cfgfile] ...' % Script.scriptName, ]))

  Script.parseCommandLine(ignoreErrors=False)
  from LHCbDIRAC.DataManagementSystem.Client.ScriptExecutors import executeListDirectory
  from DIRAC import exit

  exit(executeListDirectory(dmScript, days, months, years, wildcard, depth))
