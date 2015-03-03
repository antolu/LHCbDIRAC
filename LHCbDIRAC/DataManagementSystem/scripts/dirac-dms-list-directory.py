#!/usr/bin/env python
########################################################################
# $HeadURL$
########################################################################
"""
Get the list of all the user files.
"""
__RCSID__ = "$Id$"

if __name__ == "__main__":

  from LHCbDIRAC.DataManagementSystem.Client.DMScript import DMScript
  from DIRAC.Core.Base import Script
  days = 0
  months = 0
  years = 0
  wildcard = '*'
  dmScript = DMScript()
  dmScript.registerNamespaceSwitches()
  Script.registerSwitch( "", "Days=", "Match files older than number of days [%s]" % days )
  Script.registerSwitch( "", "Months=", "Match files older than number of months [%s]" % months )
  Script.registerSwitch( "", "Years=", "Match files older than number of years [%s]" % years )
  Script.registerSwitch( "", "Wildcard=", "Wildcard for matching filenames [%s]" % wildcard )
  Script.registerSwitch( '', 'Output', 'Write list to an output file' )
  Script.registerSwitch( "", "EmptyDirs", "Create a list of empty directories" )
  Script.registerSwitch( "", "Depth=", "Depth to which recursively browse (default = no)" )

  Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                       'Usage:',
                                       '  %s [option|cfgfile] ...' % Script.scriptName, ] ) )

  Script.parseCommandLine( ignoreErrors = False )
  from LHCbDIRAC.DataManagementSystem.Client.ScriptExecutors import executeListDirectory

  executeListDirectory( dmScript, days, months, years, wildcard )
