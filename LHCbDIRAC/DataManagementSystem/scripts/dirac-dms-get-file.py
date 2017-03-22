#!/usr/bin/env python
########################################################################
# File :    dirac-dms-get-file
# Author :  Stuart Paterson
########################################################################
"""
  Retrieve a single file or list of files from Grid storage to the current directory.
"""
__RCSID__ = "$Id$"
from LHCbDIRAC.DataManagementSystem.Client.DMScript import DMScript
from DIRAC.Core.Base import Script
import os

if __name__ == "__main__":

  dmScript = DMScript()
  dmScript.registerFileSwitches()
  dmScript.registerNamespaceSwitches( 'download to (default = %s)' % os.path.realpath( '.' ) )

  Script.setUsageMessage( '\n'.join( [ __doc__,
                                       'Usage:',
                                       '  %s [option|cfgfile] [<LFN>] [<LFN>...] [SourceSE]' % Script.scriptName, ] ) )

  Script.parseCommandLine( ignoreErrors = False )

  from LHCbDIRAC.DataManagementSystem.Client.ScriptExecutors import executeGetFile
  from DIRAC import exit
  exit( executeGetFile( dmScript ) )
