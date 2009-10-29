#! /usr/bin/env python
from DIRAC.Core.Base                                      import Script
Script.parseCommandLine(ignoreErrors = True)
localFiles = Script.getPositionalArgs()
import DIRAC
from DIRAC                                                import gLogger
from DIRAC.LHCbSystem.Utilities.ClientTools               import getRootFileGUID
import os

if not localFiles:
  gLogger.info("No files suppied")
  gLogger.info("Usage: dirac-lhcb-get-root-guid file1 [file2 ...]")
  gLogger.info("Try dirac-lhcb-get-root-guid --help for options")
  DIRAC.exit(0)
for file in localFiles:
  if not os.path.exists(file):
    gLogger.info("The supplied file %s does not exist" % file)
    continue
  res = getRootFileGUID(file)
  if not res['OK']:
    gLogger.info("Failed to obtain GUID for %s: %s" % (file,res['Message']))
    continue
  gLogger.info("%s GUID: %s" % (file,res['Value']))
DIRAC.exit(0)
