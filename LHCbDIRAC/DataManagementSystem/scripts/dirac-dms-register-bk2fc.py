#!/usr/bin/env python
########################################################################
# File :    dirac-dms-register-bk2fc
# Author  : Philippe Charpentier
########################################################################
"""
  Given a (list of) LFNs and SEs, check for existence of a file and register in the FC if the file exists
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
                                       '  %s [option|cfgfile] ...  [LFN1[,LFN2,[...]]] [--SE] Dest[,Dest2[,...]] ' % Script.scriptName,
                                       'Arguments:',
                                       '  Dest:     Valid DIRAC SE(s)' ] ) )
  Script.parseCommandLine( ignoreErrors = True )

  from LHCbDIRAC.DataManagementSystem.Client.ScriptExecutors import executeRegisterBK2FC
  from DIRAC import exit
  exit( executeRegisterBK2FC( dmScript ) )
