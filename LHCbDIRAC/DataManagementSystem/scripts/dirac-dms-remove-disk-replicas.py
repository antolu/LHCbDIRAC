#!/usr/bin/env python
########################################################################
# $HeadURL: svn+ssh://svn.cern.ch/reps/dirac/LHCbDIRAC/branches/LHCbDIRAC_v7r16_branch/DataManagementSystem/scripts/dirac-dms-remove-replicas.py $
########################################################################
"""
Remove replicas of a (list of) LFNs from all non-ARCHIVE storage elements
"""

__RCSID__ = "$Id$"
from DIRAC.Core.Base import Script
from LHCbDIRAC.DataManagementSystem.Client.DMScript import DMScript

if __name__ == "__main__":
  dmScript = DMScript()
  dmScript.registerFileSwitches()
  dmScript.registerBKSwitches()

  Script.registerSwitch( "", "Force", " use this option for force the removal of files without ARCHIVE" )
  Script.setUsageMessage( '\n'.join( __doc__.split( '\n' ) + [
                                       'Usage:',
                                       '  %s [option|cfgfile] ... [LFN[,LFN2[,LFN3...]]] SE[,SE2...]' % Script.scriptName ] ) )
  Script.parseCommandLine()

  from LHCbDIRAC.DataManagementSystem.Client.ScriptExecutors import executeRemoveReplicas
  executeRemoveReplicas( dmScript, allDisk = True )


