#!/usr/bin/env python
"""
  Remove the given file or a list of files from the File Catalog and from the storage
"""
__RCSID__ = "$Id$"

from DIRAC.Core.Base import Script
from LHCbDIRAC.DataManagementSystem.Client.DMScript import DMScript

if __name__ == "__main__":
  dmScript = DMScript()
  dmScript.registerBKSwitches()
  dmScript.registerFileSwitches()

  Script.registerSwitch( '', 'IncludeProcessedFiles', '  Forced to set Removed the files in status Processed (default:not reset)' )
  Script.setUsageMessage( '\n'.join( [ __doc__,
                                       'Usage:',
                                       '  %s [option|cfgfile] [<LFN>] [<LFN>...]' % Script.scriptName, ] ) )

  Script.parseCommandLine()

  from LHCbDIRAC.DataManagementSystem.Client.ScriptExecutors import executeRemoveFiles
  executeRemoveFiles( dmScript )
