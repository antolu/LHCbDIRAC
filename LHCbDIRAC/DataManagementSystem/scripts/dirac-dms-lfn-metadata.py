#!/usr/bin/env python
########################################################################
# File :    dirac-dms-lfn-metadata
# Author :  Philippe Charpentier
########################################################################
"""
  Get the metadata of a (list of) LFNs from the FC
"""
__RCSID__ = "$Id$"
from LHCbDIRAC.DataManagementSystem.Client.DMScript import DMScript, Script

if __name__ == "__main__":

  dmScript = DMScript()
  dmScript.registerFileSwitches()
  dmScript.registerSiteSwitches()
  Script.setUsageMessage('\n'.join([__doc__.split('\n')[1],
                                    'Usage:',
                                    '  %s [option|cfgfile] ... [LFN[,LFN2[,LFN3...]]]' % Script.scriptName,
                                    'Arguments:',
                                    '  LFN:      Logical File Name or file containing LFNs']))
  Script.parseCommandLine(ignoreErrors=True)

  from LHCbDIRAC.DataManagementSystem.Client.ScriptExecutors import executeLfnMetadata
  from DIRAC import exit
  exit(executeLfnMetadata(dmScript))
