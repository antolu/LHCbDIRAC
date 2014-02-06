#! /usr/bin/env python
"""
Get the GUID of a (set of) ROOT file
The file can be either local, an LFN or an xrootd URL (root:...)
"""
__RCSID__ = "$Id$"

from DIRAC.Core.Base                                      import Script
Script.setUsageMessage( '\n'.join( [ __doc__,
                                     'Usage:',
                                     '  %s [option|cfgfile] file1 [file2 ...]' % Script.scriptName ] ) )
Script.parseCommandLine( ignoreErrors = True )
files = []
for file in Script.getPositionalArgs():
  files += file.split( ',' )
import DIRAC
from DIRAC                                                import gLogger
from LHCbDIRAC.Core.Utilities.File                        import getRootFileGUIDs
from LHCbDIRAC.DataManagementSystem.Client.DMScript       import printDMResult
from DIRAC.Interfaces.API.Dirac import Dirac
import os

if not files:
  Script.showHelp()
  DIRAC.exit( 0 )
existFiles = {}
nonExisting = []
dirac = Dirac()

for localFile in files:
  if os.path.exists( localFile ):
    existFiles[os.path.realpath( localFile ) ] = localFile
  elif localFile.startswith( '/lhcb' ):
    res = dirac.getReplicas( localFile, True )
    if res['OK'] and localFile in res['Value']['Successful']:
      ses = res['Value']['Successful'][localFile].keys()
      for se in ses:
        res = dirac.getAccessURL( localFile, se )
        if res['OK'] and localFile in res['Value']['Successful']:
          existFiles[ res['Value']['Successful'][localFile] ] = "%s @ %s" % ( localFile, se )
    else:
      nonExisting.append( localFile )
  elif localFile.startswith( 'root:' ):
    existFiles[ localFile ] = localFile
  else:
    nonExisting.append( localFile )

fileGUIDs = getRootFileGUIDs( existFiles.keys() )
for status in ( 'Successful', 'Failed' ):
  for file in fileGUIDs.get( 'Value', {} ).get( status, {} ):
    fileGUIDs['Value'][status][existFiles.get( file, file )] = fileGUIDs['Value'][status].pop( file )
if nonExisting:
  fileGUIDs['Value']['Failed'].update( dict.fromkeys( nonExisting, 'Non existing file' ) )
printDMResult( fileGUIDs )

DIRAC.exit( 0 )
