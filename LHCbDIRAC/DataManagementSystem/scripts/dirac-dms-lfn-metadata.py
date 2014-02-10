#!/usr/bin/env python
########################################################################
# File :    dirac-dms-lfn-metadata
# Author :  Philippe Charpentier
########################################################################
"""
  Get the metadata of a (list of) LFNs from the FC
"""
__RCSID__ = "$Id$"
import DIRAC
from LHCbDIRAC.DataManagementSystem.Client.DMScript import DMScript, printDMResult
from DIRAC.Core.Base import Script

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


  args = Script.getPositionalArgs()

  for lfn in args:
    dmScript.setLFNsFromFile( lfn )
  lfnList = dmScript.getOption( 'LFNs', [] )
  if not lfnList:
    Script.showHelp()
    DIRAC.exit( 0 )

  from DIRAC.Resources.Catalog.FileCatalog import FileCatalog
  from DIRAC import gLogger
  gLogger.setLevel( "FATAL" )
  res = FileCatalog().getFileMetadata( lfnList )
  if res['OK']:
    printDMResult( res, empty = "File not in FC" )
  else:
    gLogger.fatal( "Error getting metadata for %s" % ( lfnList ) )
    printDMResult( res, empty = "File not in FC", script = "dirac-dms-lfn-metadata" )
  DIRAC.exit( 0 )
