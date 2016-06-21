#!/usr/bin/env python
########################################################################
# File :    dirac-bookkeeping-get-tck
# Author :  Zoltan Mathe
########################################################################
"""
  Returns list of TCKs for a run range, by default only if there is a FULL stream
"""
__RCSID__ = "$Id$"

from LHCbDIRAC.DataManagementSystem.Client.DMScript import Script

if __name__ == "__main__":
  Script.registerSwitch( '', 'Runs=', 'Run range or list' )
  Script.registerSwitch( '', 'Force', 'Force getting TCK even if no FULL stream is present' )
  Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                       'Usage:',
                                       '  %s [option|cfgfile] ... ' % Script.scriptName ] ) )

  Script.parseCommandLine( ignoreErrors = True )

  from LHCbDIRAC.BookkeepingSystem.Client.ScriptExecutors import executeRunTCK
  executeRunTCK()
