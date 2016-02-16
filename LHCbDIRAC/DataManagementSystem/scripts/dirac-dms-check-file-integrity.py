#!/usr/bin/env python
########################################################################
__RCSID__ = "$Id: dirac-dms-check-file-integrity.py 69359 2013-08-08 13:57:13Z phicharp $"

import DIRAC
from LHCbDIRAC.DataManagementSystem.Client.DMScript import DMScript
from DIRAC.Core.Base import Script

if __name__ == "__main__":

  dmScript = DMScript()
  dmScript.registerBKSwitches()
  dmScript.registerFileSwitches()

  fixIt = False
  Script.registerSwitch( '', 'FixIt', 'Set replicas problematic if needed' )
  Script.setUsageMessage( """
  Check the integrity of the state of the storages and information in the File Catalogs
  for a given file or a collection of files.

  Usage:
     %s <lfn | fileContainingLfns> <SE> <status>
  """ % Script.scriptName )

  Script.parseCommandLine()

  for opt, val in Script.getUnprocessedSwitches():
    if opt == 'FixIt':
      fixIt = True

  from DIRAC import gLogger
  gLogger.setLevel( 'INFO' )
  from LHCbDIRAC.DataManagementSystem.Client.DataIntegrityClient import DataIntegrityClient

  for lfn in Script.getPositionalArgs():
    dmScript.setLFNsFromFile( lfn )
  lfns = dmScript.getOption( 'LFNs' )
  if not lfns:
    print "No LFNs given..."
    Script.showHelp()
    DIRAC.exit( 0 )

  integrityClient = DataIntegrityClient()
  res = integrityClient.catalogFileToBK( lfns )
  if not res['OK']:
    gLogger.error( res['Message'] )
    DIRAC.exit( 1 )
  replicas = res['Value']['CatalogReplicas']
  metadata = res['Value']['CatalogMetadata']
  res = integrityClient.checkPhysicalFiles( replicas, metadata, fixIt = fixIt )
  if not res['OK']:
    gLogger.error( res['Message'] )
    DIRAC.exit( 1 )
  DIRAC.exit( 0 )
