#! /usr/bin/env python
"""
script to get the GUID of a ROOT file
"""
__RCSID__ = "$Id$"

from DIRAC.Core.Base                                      import Script
Script.parseCommandLine( ignoreErrors = True )
localFiles = Script.getPositionalArgs()
import DIRAC
from DIRAC                                                import gLogger
from LHCbDIRAC.Core.Utilities.File                        import getRootFileGUIDs
from LHCbDIRAC.DataManagementSystem.Client.DMScript       import printDMResult
import os

if not localFiles:
  gLogger.info( "No files suppied" )
  gLogger.info( "Usage: dirac-lhcb-get-root-guid file1 [file2 ...]" )
  gLogger.info( "Try dirac-lhcb-get-root-guid --help for options" )
  DIRAC.exit( 0 )
existFiles = []

for localFile in localFiles:
  if os.path.exists( localFile ):
    existFiles.append( os.path.realpath( localFile ) )

fileGUIDs = getRootFileGUIDs( existFiles )
nonExisting = sorted( set( localFiles ) - set( existFiles ) )
if nonExisting:
  fileGUIDs['Value']['Failed'].update( dict.fromkeys( nonExisting, 'Non existing file' ) )
printDMResult( fileGUIDs )

DIRAC.exit( 0 )
