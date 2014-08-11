#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :    dirac-dms-replicate-lfn
# Author  : Stuart Paterson
########################################################################
"""
  Replicate a (list of) existing LFN(s) to (set of) Storage Element(s)
"""
__RCSID__ = "$Id$"
from DIRAC.Core.Base import Script
from LHCbDIRAC.DataManagementSystem.Client.DMScript import DMScript

if __name__ == "__main__":
  dmScript = DMScript()
  dmScript.registerFileSwitches()

  Script.setUsageMessage( '\n'.join( [ __doc__,
                                       'Usage:',
                                       '  %s [option|cfgfile] ...  [LFN1[,LFN2,[...]]] Dest[,Dest2[,...]] [Source [Cache]]' % Script.scriptName,
                                       'Arguments:',
                                       '  Dest:     Valid DIRAC SE(s)',
                                       '  Source:   Valid DIRAC SE',
                                       '  Cache:    Local directory to be used as cache' ] ) )
  Script.parseCommandLine( ignoreErrors = True )

  from LHCbDIRAC.DataManagementSystem.Client.ScriptExecutors import executeReplicateLfn
  executeReplicateLfn( dmScript )

