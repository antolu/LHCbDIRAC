#!/usr/bin/env python
""" Submission of test jobs for use by Jenkins
"""

#pylint: disable=wrong-import-position,unused-wildcard-import,wildcard-import

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
inputJ.setInputSandbox( [find_all( 'exe-script-with-input-jenkins.py', '..', 'GridTestSubmission' )[0]] )
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
helloJMP = LHCbJob()
helloJMP.setName( "helloWorld-TEST-TO-Jenkins-MP" )
helloJMP.setInputSandbox( [find_all( 'exe-script.py', '..', 'GridTestSubmission' )[0]] )
helloJMP.setExecutable( "exe-script.py", "", "helloWorld.log" )
helloJMP.setCPUTime( 17800 )
helloJMP.setDestination( 'DIRAC.Jenkins.ch' )
helloJMP.setTag('MultiProcessor')
result = dirac.submit( helloJMP ) # this should make the difference!
gLogger.info( "Hello world job MP: ", result )
if not result['OK']:
  gLogger.error( "Problem submitting job", result['Message'] )
  exit( 1 )

# Simple GaudiApplication job to DIRAC.Jenkins.ch
gLogger.info( "\n Submitting gaudi application job targeting DIRAC.Jenkins.ch" )
gaudiJ = LHCbJob()
gaudiJ.setName( "GaudiJob-TO-Jenkins" )
gaudiJ.setApplication('Gauss', 'v49r5', '$APPCONFIGOPTS/Gauss/DataType-2012.py',
                      extraPackages = 'AppConfig.v3r277;DecFiles.v29r10')
gaudiJ.setCPUTime( 17800 )
gaudiJ.setDestination( 'DIRAC.Jenkins.ch' )
result = dirac.submit( gaudiJ )
gLogger.info( "Gaudi job: ", result )
if not result['OK']:
  gLogger.error( "Problem submitting job", result['Message'] )
  exit( 1 )

gLogger.info( "\nALL JOBS SUBMITTED CORRECTLY\n" )
