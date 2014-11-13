from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

import os.path

from DIRAC import gLogger

from TestDIRAC.Utilities.utils import find_all

from LHCbDIRAC.Interfaces.API.LHCbJob import LHCbJob
from LHCbDIRAC.Interfaces.API.DiracLHCb import DiracLHCb
from LHCbTestDirac.Integration.Workflow.Test_UserJobs import createJob

gLogger.setLevel( 'DEBUG' )

cwd = os.path.realpath( '.' )

gLogger.info( "\n Submitting hello world job targeting DIRAC.Test.ch" )

helloJ = LHCbJob()
dirac = DiracLHCb()

helloJ.setName( "helloWorld-TEST-TO-Jenkins" )
helloJ.setInputSandbox( [find_all( 'exe-script.py', '.', 'GridTestSubmission' )[0]] )
helloJ.setExecutable( "exe-script.py", "", "helloWorld.log" )

helloJ.setCPUTime( 17800 )
helloJ.setDestination( 'DIRAC.Test.ch' )
result = dirac.submit( helloJ )
gLogger.info( "Hello world job: ", result )
