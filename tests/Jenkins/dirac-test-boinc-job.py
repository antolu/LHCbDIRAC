#!/usr/bin/env python
""" Submission of test jobs for use by Jenkins
"""

# pylint: disable=wrong-import-position,unused-wildcard-import,wildcard-import

import os.path
import time

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from DIRAC import gLogger


from LHCbDIRAC.Interfaces.API.LHCbJob import LHCbJob
from LHCbDIRAC.Interfaces.API.DiracLHCb import DiracLHCb

gLogger.setLevel('DEBUG')

dirac = DiracLHCb()

now = time.time()

# Simple Hello Word job with output to BOINCCert.World.org
gLogger.info("\n Submitting hello world job targeting BOINCCert.World.org")
helloJMP = LHCbJob()
helloJMP.setName("helloWorld-TEST-TO-BOINC")
helloJMP.setExecutable("boinc-exec-script.py", "%s" % now, "helloWorld.log")
helloJMP.setType('Test')
helloJMP.setCPUTime(17800)
helloJMP.setDestination('BOINCCert.World.org')
helloJMP.setOutputData(["%s_toto.txt" % now])
result = dirac.submit(helloJMP)
gLogger.info("Result: ", result)
if not result['OK']:
  gLogger.error("Problem submitting job", result['Message'])
  exit(1)


gLogger.info("JOB SUBMITTED CORRECTLY\n")
