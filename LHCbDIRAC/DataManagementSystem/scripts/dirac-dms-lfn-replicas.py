#!/usr/bin/env python
########################################################################
# File :   dirac-admin-lfn-replicas
# Author : Stuart Paterson
########################################################################
"""
    Show replicas for a (set of) LFNs
"""
__RCSID__ = "$Id$"
__VERSION__ = "$Revision$"

from DIRAC.Core.Base import Script
from LHCbDIRAC.DataManagementSystem.Client.DMScript import DMScript

if __name__ == "__main__":

  dmScript = DMScript()
  dmScript.registerFileSwitches()
  Script.registerSwitch( 'a', "All", "  Also show inactive replicas" )

  Script.setUsageMessage( '\n'.join( [ __doc__,
                                       'Usage:',
                                       '  %s [option|cfgfile] [<LFN>] [<LFN>...]' % Script.scriptName, ] ) )

  Script.parseCommandLine( ignoreErrors = False )

  from LHCbDIRAC.DataManagementSystem.Client.ScriptExecutors import executeLfnReplicas
  executeLfnReplicas( dmScript )
