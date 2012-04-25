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
from DIRAC.Core.Utilities.List                            import sortList
from LHCbDIRAC.Core.Utilities.ClientTools                 import getRootFilesGUIDs
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
  else:
    gLogger.info( "The supplied file %s does not exist" % localFile )
res = getRootFilesGUIDs( existFiles )
if not res['OK']:
  gLogger.error( "Failed to obtain file GUIDs", res['Message'] )
  DIRAC.exit( -1 )
fileGUIDs = res['Value']
for filename in sortList( fileGUIDs.keys() ):
  if fileGUIDs[file]:
    gLogger.info( "%s GUID: %s" % ( filename, fileGUIDs[filename] ) )
  else:
    gLogger.info( "%s GUID: Failed to get GUID" % filename )
DIRAC.exit( 0 )
