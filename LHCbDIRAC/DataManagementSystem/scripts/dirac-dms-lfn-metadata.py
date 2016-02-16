#!/usr/bin/env python
########################################################################
# File :    dirac-dms-lfn-metadata
# Author :  Philippe Charpentier
########################################################################
"""
  Get the metadata of a (list of) LFNs from the FC
"""
__RCSID__ = "$Id: dirac-dms-lfn-metadata.py 77175 2014-08-11 13:32:45Z phicharp $"
from DIRAC.Core.Base import Script
from LHCbDIRAC.DataManagementSystem.Client.DMScript import DMScript

if __name__ == "__main__":

  dmScript = DMScript()
  dmScript.registerFileSwitches()
  dmScript.registerSiteSwitches()
  Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                       'Usage:',
                                       '  %s [option|cfgfile] ... [LFN[,LFN2[,LFN3...]]]' % Script.scriptName,
                                       'Arguments:',
                                       '  LFN:      Logical File Name or file containing LFNs'] ) )
  Script.parseCommandLine( ignoreErrors = True )

  from LHCbDIRAC.DataManagementSystem.Client.ScriptExecutors import executeLfnMetadata
  executeLfnMetadata( dmScript )

