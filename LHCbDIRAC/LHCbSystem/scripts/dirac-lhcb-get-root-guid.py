#! /usr/bin/env python
from DIRAC.Core.Base                                      import Script
Script.parseCommandLine(ignoreErrors = True)
localFiles = Script.getPositionalArgs()
import DIRAC
from DIRAC                                                import gLogger
from DIRAC.LHCbSystem.Utilities.ClientTools               import getRootFilesGUIDs
from DIRAC.Core.Utilities.List                            import sortList
import os

if not localFiles:
  gLogger.info("No files suppied")
  gLogger.info("Usage: dirac-lhcb-get-root-guid file1 [file2 ...]")
  gLogger.info("Try dirac-lhcb-get-root-guid --help for options")
  DIRAC.exit(0)
existFiles = []
for localFile in localFiles:
  if os.path.exists(localFile):
    existFiles.append(os.path.realpath(localFile))
  else:
    gLogger.info("The supplied file %s does not exist" % localFile)  
res = getRootFilesGUIDs(existFiles)
if not res['OK']:
  gLogger.error("Failed to obtain file GUIDs",res['Message'])
  DIRAC.exit(-1)
fileGUIDs = res['Value']
for file in sortList(fileGUIDs.keys()):
  if fileGUIDs[file]:
    gLogger.info("%s GUID: %s" % (file,fileGUIDs[file]))
  else:
    gLogger.info("%s GUID: Failed to get GUID" % file)
DIRAC.exit(0)
