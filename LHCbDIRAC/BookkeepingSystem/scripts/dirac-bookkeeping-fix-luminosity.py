#! /usr/bin/env python
"""
   Fix the luminosity of all descendants of a set of RAW files, if hte run is Finished
"""

__RCSID__ = "$Id$"

from LHCbDIRAC.DataManagementSystem.Client.DMScript import DMScript, Script

if __name__ == "__main__":

  dmScript = DMScript()
  dmScript.registerBKSwitches()

  Script.registerSwitch( '', 'DoIt', '   Fix the BK database (default No)' )
  Script.registerSwitch( '', 'Force', '   Force checking all descendants and not only those of files with bad lumi (default No)' )
  Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                       'Usage:',
                                       '  %s [option|cfgfile]' % Script.scriptName, ] ) )

  Script.addDefaultOptionValue( 'LogLevel', 'error' )
  Script.parseCommandLine( ignoreErrors = False )

  execute()

