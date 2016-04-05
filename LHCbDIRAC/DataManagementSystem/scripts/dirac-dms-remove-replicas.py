#!/usr/bin/env python
"""
Remove replicas of a (list of) LFNs at a list of sites. It is possible to request a minimum of remaining replicas
"""

__RCSID__ = "$Id$"
from DIRAC.Core.Base import Script
from LHCbDIRAC.DataManagementSystem.Client.DMScript import DMScript

if __name__ == "__main__":
  dmScript = DMScript()
  dmScript.registerFileSwitches()
  dmScript.registerSiteSwitches()

  Script.registerSwitch( "n", "NoLFC", " use this option to force the removal from storage of replicas not in FC" )
  Script.registerSwitch( '', 'ReduceReplicas=', '  specify the number of replicas you want to keep (default SE: Tier1-USER)' )
  Script.registerSwitch( "", "Force", " use this option for force the removal of replicas even if last one" )
  Script.setUsageMessage( '\n'.join( __doc__.split( '\n' ) + [
                                       'Usage:',
                                       '  %s [option|cfgfile] ... [LFN[,LFN2[,LFN3...]]] SE[,SE2...]' % Script.scriptName,
                                       'Arguments:',
                                       '  LFN:      Logical File Name or file containing LFNs',
                                       '  SE:       Valid DIRAC SE' ] ) )
  Script.parseCommandLine()

  from LHCbDIRAC.DataManagementSystem.Client.ScriptExecutors import executeRemoveReplicas
  from DIRAC import exit
  exit( executeRemoveReplicas( dmScript ) )
