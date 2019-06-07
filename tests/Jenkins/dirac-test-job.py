#!/usr/bin/env python
###############################################################################
# (c) Copyright 2019 CERN for the benefit of the LHCb Collaboration           #
#                                                                             #
# This software is distributed under the terms of the GNU General Public      #
# Licence version 3 (GPL Version 3), copied verbatim in the file "LICENSE".   #
#                                                                             #
# In applying this licence, CERN does not waive the privileges and immunities #
# granted to it by virtue of its status as an Intergovernmental Organization  #
# or submit itself to any jurisdiction.                                       #
###############################################################################
""" Submission of test jobs for use by Jenkins
"""

# pylint: disable=wrong-import-position,unused-wildcard-import,wildcard-import

import os

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from DIRAC import gLogger, rootPath

from DIRAC.tests.Utilities.utils import find_all

from LHCbDIRAC.Interfaces.API.LHCbJob import LHCbJob
from LHCbDIRAC.Interfaces.API.DiracLHCb import DiracLHCb
#from tests.Workflow.Integration.Test_UserJobs import createJob

gLogger.setLevel('DEBUG')

cwd = os.path.realpath('.')

dirac = DiracLHCb()

# Simple Hello Word job to DIRAC.Jenkins.ch
gLogger.info("\n Submitting hello world job targeting DIRAC.Jenkins.ch")
helloJ = LHCbJob()
helloJ.setName("helloWorld-TEST-TO-Jenkins")
try:
  helloJ.setInputSandbox([find_all('exe-script.py', rootPath, '/DIRAC/tests/Workflow/')[0]])
except IndexError:
  helloJ.setInputSandbox([find_all('exe-script.py', os.environ['WORKSPACE'], '/DIRAC/tests/Workflow/')[0]])
helloJ.setExecutable("exe-script.py", "", "helloWorld.log")
helloJ.setCPUTime(17800)
helloJ.setDestination('DIRAC.Jenkins.ch')
result = dirac.submitJob(helloJ)
gLogger.info("Hello world job: ", result)
if not result['OK']:
  gLogger.error("Problem submitting job", result['Message'])
  exit(1)


# Simple Hello Word job to DIRAC.Jenkins.ch, with an input file
gLogger.info("\n Submitting hello world job, with input, targeting DIRAC.Jenkins.ch")
inputJ = LHCbJob()
inputJ.setName("helloWorld-TEST-INPUT-TO-Jenkins")
try:
  inputJ.setInputSandbox([find_all('exe-script-with-input-jenkins.py',
                                   rootPath,
                                   'tests/System/GridTestSubmission')[0]])
except BaseException:
  inputJ.setInputSandbox([find_all('exe-script-with-input-jenkins.py',
                                   os.environ['WORKSPACE'],
                                   'tests/System/GridTestSubmission')[0]])
inputJ.setExecutable("exe-script-with-input-jenkins.py", "", "exeWithInput.log")
inputJ.setInputData('/lhcb/test/DIRAC/Jenkins/jenkinsInputTestFile.txt')  # this file should be at CERN-SWTEST only
inputJ.setInputDataPolicy('download')
inputJ.setCPUTime(17800)
inputJ.setDestination('DIRAC.Jenkins.ch')
result = dirac.submitJob(inputJ)
gLogger.info("Hello world job with input: ", result)
if not result['OK']:
  gLogger.error("Problem submitting job", result['Message'])
  exit(1)


# Simple Hello Word job to DIRAC.Jenkins.ch, that needs to be matched by a MP WN
gLogger.info("\n Submitting hello world job targeting DIRAC.Jenkins.ch and a MP WN")
helloJMP = LHCbJob()
helloJMP.setName("helloWorld-TEST-TO-Jenkins-MP")
try:
  helloJMP.setInputSandbox([find_all('exe-script.py', rootPath, '/DIRAC/tests/Workflow/')[0]])
except IndexError:
  helloJMP.setInputSandbox([find_all('exe-script.py', os.environ['WORKSPACE'], '/DIRAC/tests/Workflow/')[0]])
helloJMP.setExecutable("exe-script.py", "", "helloWorld.log")
helloJMP.setCPUTime(17800)
helloJMP.setDestination('DIRAC.Jenkins.ch')
helloJMP.setTag('MultiProcessor')  # this should make the difference!
result = dirac.submitJob(helloJMP)
gLogger.info("Hello world job MP: ", result)
if not result['OK']:
  gLogger.error("Problem submitting job", result['Message'])
  exit(1)


# Simple GaudiApplication job to DIRAC.Jenkins.ch
gLogger.info("\n Submitting gaudi application job targeting DIRAC.Jenkins.ch")
gaudiJ = LHCbJob()
gaudiJ.setName("GaudiJob-TO-Jenkins")
gaudiJ.setApplication('Gauss', 'v49r5', '$APPCONFIGOPTS/Gauss/DataType-2012.py',
                      extraPackages='AppConfig.v3r277;Gen/DecFiles.v29r10',
                      events=1)
gaudiJ.setCPUTime(17800)
gaudiJ.setDestination('DIRAC.Jenkins.ch')
result = dirac.submitJob(gaudiJ)
gLogger.info("Gaudi job: ", result)
if not result['OK']:
  gLogger.error("Problem submitting job", result['Message'])
  exit(1)

gLogger.info("\nALL JOBS SUBMITTED CORRECTLY\n")
