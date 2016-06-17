#!/usr/bin/env python
########################################################################
# File :    dirac-bookkeeping-eventtype-mgt-update
# Author :  Zoltan Mathe
########################################################################
"""
  Retrieve metadata from the Bookkeeping for the given files
"""
__RCSID__ = "$Id$"
import  DIRAC.Core.Base.Script as Script
from LHCbDIRAC.DataManagementSystem.Client.DMScript import DMScript

if __name__ == "__main__":
  dmScript = DMScript()
  dmScript.registerFileSwitches()
  Script.registerSwitch( '', 'Full', '   Print out all metadata' )
  Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                       'Usage:',
                                       '  %s [option|cfgfile] ... LFN|File' % Script.scriptName,
                                       'Arguments:',
                                       '  LFN:      Logical File Name',
                                       '  File:     Name of the file with a list of LFNs' ] ) )
  Script.parseCommandLine()

  from LHCbDIRAC.BookkeepingSystem.Client.ScriptExecutors import executeFileMetadata
  executeFileMetadata( dmScript )
