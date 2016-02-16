#!/usr/bin/env python
########################################################################
# $HeadURL: http://svn.cern.ch/guest/dirac/LHCbDIRAC/tags/LHCbDIRAC/v8r2/DataManagementSystem/scripts/dirac-dms-replicate-lfn.py $
# File :    dirac-dms-replicate-lfn
# Author  : Stuart Paterson
########################################################################
"""
  Replicate a (list of) existing LFN(s) to (set of) Storage Element(s)
"""
__RCSID__ = "$Id: dirac-dms-replicate-lfn.py 79816 2014-12-03 09:11:02Z phicharp $"
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
                                       '  Dest:     Valid DIRAC SE(s)',
                                       '  Source:   Valid DIRAC SE',
                                       '  Cache:    Local directory to be used as cache' ] ) )
  Script.parseCommandLine( ignoreErrors = True )

  from LHCbDIRAC.DataManagementSystem.Client.ScriptExecutors import executeReplicateLfn
  executeReplicateLfn( dmScript )

