#!/usr/bin/env python

"""
   Get statistics on number of replicas for a given directory or production
"""

__RCSID__ = "$Id$"

from DIRAC.Core.Base import Script
from LHCbDIRAC.DataManagementSystem.Client.DMScript import DMScript

if __name__ == "__main__":

  dmScript = DMScript()
  dmScript.registerBKSwitches()
  dmScript.registerNamespaceSwitches()
  dmScript.registerFileSwitches()

  Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                       'Usage:',
                                       '  %s [option|cfgfile] [<LFN>] [<LFN>...]' % Script.scriptName, ] ) )

  Script.registerSwitch( "", "Size", "   Get the LFN size [No]" )
  Script.registerSwitch( '', 'DumpNoReplicas', '   Print list of files without a replica [No]' )
  Script.registerSwitch( '', 'DumpWithArchives=', '   =<n>, print list of files with <n> archives' )
  Script.registerSwitch( '', 'DumpWithReplicas=', '   =<n>, print list of files with <n> replicas' )
  Script.addDefaultOptionValue( 'LogLevel', 'error' )

  Script.parseCommandLine( ignoreErrors = False )

  from LHCbDIRAC.DataManagementSystem.Client.ScriptExecutors import executeReplicaStats
  executeReplicaStats( dmScript )

