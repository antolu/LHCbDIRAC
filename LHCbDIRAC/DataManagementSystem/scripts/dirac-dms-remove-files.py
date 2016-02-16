#!/usr/bin/env python
########################################################################
# $HeadURL: http://svn.cern.ch/guest/dirac/LHCbDIRAC/tags/LHCbDIRAC/v8r2p5/DataManagementSystem/scripts/dirac-dms-remove-files.py $
########################################################################
"""
  Remove the given file or a list of files from the File Catalog and from the storage
"""
__RCSID__ = "$Id: dirac-dms-remove-files.py 77175 2014-08-11 13:32:45Z phicharp $"

from DIRAC.Core.Base import Script
from LHCbDIRAC.DataManagementSystem.Client.DMScript import DMScript

if __name__ == "__main__":
  dmScript = DMScript()
  dmScript.registerBKSwitches()
  dmScript.registerFileSwitches()

  Script.registerSwitch( '', 'SetProcessed', '  Forced to set Removed the files in status Processed (default:not reset)' )
  Script.setUsageMessage( '\n'.join( [ __doc__,
                                       'Usage:',
                                       '  %s [option|cfgfile] [<LFN>] [<LFN>...]' % Script.scriptName, ] ) )

  Script.parseCommandLine()

  from LHCbDIRAC.DataManagementSystem.Client.ScriptExecutors import executeRemoveFiles
  executeRemoveFiles( dmScript )
