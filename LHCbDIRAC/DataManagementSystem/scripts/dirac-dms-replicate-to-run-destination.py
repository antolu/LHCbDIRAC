#!/usr/bin/env python
########################################################################
# File :    dirac-dms-replicate-to-run-destination
# Author  : Philippe Charpentier
########################################################################
"""
  Replicate a (list of) existing LFN(s) to Ses defined by the run destination
"""
__RCSID__ = "$Id$"

from LHCbDIRAC.DataManagementSystem.Client.DMScript import DMScript, Script

if __name__ == "__main__":
  dmScript = DMScript()
  dmScript.registerFileSwitches()
  dmScript.registerSiteSwitches()

  Script.registerSwitch('', 'RemoveSource', '   If set, the source replica(s) will be removed')

  Script.setUsageMessage('\n'.join([__doc__,
                                    'Usage:',
                                    '  %s [option|cfgfile] ...  [LFN1[,LFN2,[...]]] Dest[,Dest2[,...]] [Source [Cache]]' % Script.scriptName,
                                    'Arguments:',
                                    '  Dest:     Valid DIRAC SE(s)']))
  Script.parseCommandLine(ignoreErrors=True)

  from LHCbDIRAC.DataManagementSystem.Client.ScriptExecutors import executeReplicateToRunDestination
  from DIRAC import exit
  exit(executeReplicateToRunDestination(dmScript))
