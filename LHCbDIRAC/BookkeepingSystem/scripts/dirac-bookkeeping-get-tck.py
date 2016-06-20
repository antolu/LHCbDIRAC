#!/usr/bin/env python
########################################################################
# File :    dirac-bookkeeping-get-tck
# Author :  Zoltan Mathe
########################################################################
"""
  Returns list of TCKs for a run range
"""
__RCSID__ = "$Id$"

from LHCbDIRAC.DataManagementSystem.Client.DMScript import Script

if __name__ == "__main__":
  Script.registerSwitch( '', 'Runs=', 'Run range or list' )
  Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                       'Usage:',
                                       '  %s [option|cfgfile] ... ' % Script.scriptName ] ) )

  Script.parseCommandLine( ignoreErrors = True )

  from LHCbDIRAC.BookkeepingSystem.Client.ScriptExecutors import executeRunTCK
  executeRunTCK()
