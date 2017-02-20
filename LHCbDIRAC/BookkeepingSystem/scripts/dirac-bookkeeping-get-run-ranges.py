#!/usr/bin/env python
########################################################################
# File :    dirac-bookkeeping-get-run-ranges
# Author :  Zoltan Mathe
########################################################################
"""
  Returns list of TCKs for a run range, by default only if there is a FULL stream
"""
__RCSID__ = "$Id$"

from LHCbDIRAC.DataManagementSystem.Client.DMScript import Script

if __name__ == "__main__":
  Script.registerSwitch( '', 'Activity=', 'Specify the BK activity (e.g. Collision15)' )
  Script.registerSwitch( '', 'Runs=', 'Run range or list (can be used with --Activity to reduce the run range)' )
  Script.registerSwitch( '', 'Fast', 'Include runs even if no FULL stream is present (much faster)' )
  Script.registerSwitch( '', 'DQFlag=', 'Specify the DQ flag (default: all)' )
  Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                       'Usage:',
                                       '  %s [option|cfgfile] ... ' % Script.scriptName ] ) )

  Script.parseCommandLine( ignoreErrors = True )

  from LHCbDIRAC.BookkeepingSystem.Client.ScriptExecutors import executeRunInfo
  executeRunInfo( 'Ranges' )
