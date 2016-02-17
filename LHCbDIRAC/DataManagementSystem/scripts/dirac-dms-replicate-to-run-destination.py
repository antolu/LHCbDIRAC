#!/usr/bin/env python
########################################################################
# $HeadURL: svn+ssh://svn.cern.ch/reps/dirac/LHCbDIRAC/branches/LHCbDIRAC_v8r1_branch/DataManagementSystem/scripts/dirac-dms-replicate-lfn.py $
# File :    dirac-dms-replicate-to-run-destination
# Author  : Philippe Charpentier
########################################################################
"""
  Replicate a (list of) existing LFN(s) to Ses defined by the run destination
"""
__RCSID__ = "$Id$"
from DIRAC.Core.Base import Script
from LHCbDIRAC.DataManagementSystem.Client.DMScript import DMScript

if __name__ == "__main__":
  dmScript = DMScript()
  dmScript.registerFileSwitches()
  dmScript.registerSiteSwitches()

  Script.setUsageMessage( '\n'.join( [ __doc__,
                                       'Usage:',
                                       '  %s [option|cfgfile] ...  [LFN1[,LFN2,[...]]] Dest[,Dest2[,...]] [Source [Cache]]' % Script.scriptName,
                                       'Arguments:',
                                       '  Dest:     Valid DIRAC SE(s)'] ) )
  Script.parseCommandLine( ignoreErrors = True )

  from LHCbDIRAC.DataManagementSystem.Client.ScriptExecutors import executeReplicateToRunDestination
  executeReplicateToRunDestination( dmScript )

