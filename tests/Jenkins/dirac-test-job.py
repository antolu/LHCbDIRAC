#!/usr/bin/env python
""" Submission of test jobs for use by Jenkins
"""

import os.path

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from DIRAC import gLogger

from DIRAC.tests.Utilities.utils import find_all

from LHCbDIRAC.Interfaces.API.LHCbJob import LHCbJob
from LHCbDIRAC.Interfaces.API.DiracLHCb import DiracLHCb
#from tests.Workflow.Integration.Test_UserJobs import createJob

gLogger.setLevel( 'DEBUG' )

cwd = os.path.realpath( '.' )

dirac = DiracLHCb()

# Simple Hello Word job to DIRAC.Jenkins.ch

gLogger.info( "\n Submitting hello world job targeting DIRAC.Jenkins.ch" )
helloJ = LHCbJob()
helloJ.setName( "helloWorld-TEST-TO-Jenkins" )
helloJ.setInputSandbox( [find_all( 'exe-script.py', '..', 'GridTestSubmission' )[0]] )
helloJ.setExecutable( "exe-script.py", "", "helloWorld.log" )
helloJ.setCPUTime( 17800 )
helloJ.setDestination( 'DIRAC.Jenkins.ch' )
result = dirac.submit( helloJ )
gLogger.info( "Hello world job: ", result )
if not result['OK']:
  gLogger.error( "Problem submitting job", result['Message'] )
  exit( 1 )

# Simple Hello Word job to DIRAC.Jenkins.ch, with an input file

gLogger.info( "\n Submitting hello world job, with input, targeting DIRAC.Jenkins.ch" )
inputJ = LHCbJob()
inputJ.setName( "helloWorld-TEST-INPUT-TO-Jenkins" )
inputJ.setInputSandbox( [find_all( 'exe-script-with-input.py', '..', 'GridTestSubmission' )[0]] )
inputJ.setExecutable( "exe-script-with-input-jenkins.py", "", "exeWithInput.log" )
inputJ.setInputData( '/lhcb/test/DIRAC/Jenkins/jenkinsInputTestFile.txt' )  # this file should be at CERN-SWTEST only
inputJ.setInputDataPolicy( 'download' )
inputJ.setCPUTime( 17800 )
inputJ.setDestination( 'DIRAC.Jenkins.ch' )
result = dirac.submit( inputJ )
gLogger.info( "Hello world job with input: ", result )
if not result['OK']:
  gLogger.error( "Problem submitting job", result['Message'] )
  exit( 1 )

# Simple Hello Word job to DIRAC.Jenkins.ch, that needs to be matched by a MP WN
gLogger.info( "\n Submitting hello world job targeting DIRAC.Jenkins.ch and a MP WN" )
helloJ = LHCbJob()
helloJ.setName( "helloWorld-TEST-TO-Jenkins-MP" )
helloJ.setInputSandbox( [find_all( 'exe-script.py', '..', 'GridTestSubmission' )[0]] )
helloJ.setExecutable( "exe-script.py", "", "helloWorld.log" )
helloJ.setCPUTime( 17800 )
helloJ.setDestination( 'DIRAC.Jenkins.ch' )
helloJ.setTag('MultiProcessor')
result = dirac.submit( helloJ ) # this should make the difference!
gLogger.info( "Hello world job MP: ", result )
if not result['OK']:
  gLogger.error( "Problem submitting job", result['Message'] )
  exit( 1 )

gLogger.info( "\nALL JOBS SUBMITTED CORRECTLY\n" )
