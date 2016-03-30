  from DIRAC import exit
#!/usr/bin/env python
########################################################################
# File :    dirac-dms-pfn-metadata.py
# Author :  Ph. Charpentier
########################################################################
"""
  Gets the metadata of a (list of) LHCb LFNs/PFNs given a valid DIRAC SE.
  Only the LFN contained in the PFN is considered, unlike the DIRAC similar script
"""
__RCSID__ = "$Id$"
from DIRAC.Core.Base import Script
from LHCbDIRAC.DataManagementSystem.Client.DMScript import DMScript, printDMResult

if __name__ == "__main__":

  dmScript = DMScript()
  dmScript.registerFileSwitches()
  dmScript.registerSiteSwitches()
  Script.registerSwitch( '', 'Check', '   Checks the PFN metadata vs LFN metadata' )
  Script.registerSwitch( '', 'Exists', '   Only reports if the file exists (and checks the checksum)' )
  Script.registerSwitch( '', 'Summary', '   Only prints a summary on existing files' )
  Script.setUsageMessage( '\n'.join( [ __doc__,
                                       'Usage:',
                                       '  %s [option|cfgfile] ... [URL[,URL2[,URL3...]]] SE[ SE2...]' % Script.scriptName,
                                       'Arguments:',
                                       '  URL:      Logical/Physical File Name or file containing URLs',
                                       '  SE:       Valid DIRAC SE' ] ) )
  Script.parseCommandLine( ignoreErrors = True )

  from LHCbDIRAC.DataManagementSystem.Client.ScriptExecutors import executePfnMetadata
  from DIRAC import exit
  exit( executePfnMetadata( dmScript ) )

