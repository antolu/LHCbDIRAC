#! /usr/bin/env python
"""
   Get statistical information on a dataset
"""

__RCSID__ = "$Id$"

from LHCbDIRAC.DataManagementSystem.Client.DMScript import DMScript, Script

if __name__ == "__main__":

  dmScript = DMScript()
  dmScript.registerBKSwitches()
  dmScript.registerFileSwitches()

  Script.registerSwitch( '', 'TriggerRate', '   For RAW files, returns the trigger rate' )
  Script.registerSwitch( '', 'ListRuns', '   Give a list of runs (to be used with --Trigger)' )
  Script.registerSwitch( '', 'ListFills', '   Give a list of fills (to be used with --Trigger)' )
  Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                       'Usage:',
                                       '  %s [option|cfgfile]' % Script.scriptName, ] ) )

  Script.addDefaultOptionValue( 'LogLevel', 'error' )
  Script.parseCommandLine( ignoreErrors = False )

  from LHCbDIRAC.BookkeepingSystem.Client.ScriptExecutors import executeGetStats
  executeGetStats( dmScript )
