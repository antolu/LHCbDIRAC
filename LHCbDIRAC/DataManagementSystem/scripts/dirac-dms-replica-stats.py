#!/usr/bin/env python

"""
   Get statistics on number of replicas for a given directory or production
"""

__RCSID__ = "$Id$"

from LHCbDIRAC.DataManagementSystem.Client.DMScript import DMScript, Script

if __name__ == "__main__":

  dmScript = DMScript()
  dmScript.registerBKSwitches()
  dmScript.registerNamespaceSwitches()
  dmScript.registerFileSwitches()

  Script.setUsageMessage('\n'.join([__doc__.split('\n')[1],
                                    'Usage:',
                                    '  %s [option|cfgfile] [<LFN>] [<LFN>...]' % Script.scriptName, ]))

  Script.registerSwitch("", "Size", "   Get the LFN size [No]")
  Script.registerSwitch('', 'DumpNoReplicas', '   Print list of files without a replica [No]')
  Script.registerSwitch('', 'DumpWithArchives=', '   =<n>, print files with <n> archives')
  Script.registerSwitch('', 'DumpWithReplicas=', '   =<n>, print files with <n> replicas')
  Script.registerSwitch('', 'DumpFailover',
                        '   print files with failover replica (can be used with Dump[With/No]Replicas)')
  Script.registerSwitch('', 'DumpAtSE=', '   print files present at a (list of) SE')
  Script.registerSwitch('', 'DumpNotAtSE=', '   print files absent at a (list of) SE')
  Script.registerSwitch('', 'DumpAtSite=', '   print files present at a (list of) sites')
  Script.registerSwitch('', 'Summary', '   do not print stats per SE nor site')

  Script.parseCommandLine(ignoreErrors=False)

  from LHCbDIRAC.DataManagementSystem.Client.ScriptExecutors import executeReplicaStats
  from DIRAC import exit
  exit(executeReplicaStats(dmScript))
