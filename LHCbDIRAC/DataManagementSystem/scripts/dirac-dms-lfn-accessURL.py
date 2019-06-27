#!/usr/bin/env python

"""
  Retrieve an access URL for an LFN replica given a valid DIRAC SE.
"""

__RCSID__ = "$Id$"

from LHCbDIRAC.DataManagementSystem.Client.DMScript import DMScript, Script

if __name__ == "__main__":

  dmScript = DMScript()
  dmScript.registerFileSwitches()
  dmScript.registerSiteSwitches()
  Script.registerSwitch('', 'Protocol=',
                        '   Define the protocol for which a tURL is requested (default:root)')
  Script.setUsageMessage('\n'.join(__doc__.split('\n') + [
      'Usage:',
      '  %s [option|cfgfile] ... [LFN[,LFN2[,LFN3...]]] SE[,SE2...]' % Script.scriptName,
      'Arguments:',
      '  LFN:      Logical File Name or file containing LFNs',
      '  SE:       Valid DIRAC SE']))
  Script.parseCommandLine(ignoreErrors=True)

  from LHCbDIRAC.DataManagementSystem.Client.ScriptExecutors import executeAccessURL
  from DIRAC import exit
  exit(executeAccessURL(dmScript))
