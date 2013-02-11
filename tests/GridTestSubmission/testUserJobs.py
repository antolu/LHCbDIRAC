from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from DIRAC import gLogger
from DIRAC.Core.Utilities.List import breakListIntoChunks
from LHCbDIRAC.Interfaces.API.LHCbJob import LHCbJob
from LHCbDIRAC.Interfaces.API.DiracLHCb import DiracLHCb

gLogger.setLevel( 'DEBUG' )

gLogger.info( "Submitting hello world job" )

helloJ = LHCbJob()
dirac = DiracLHCb()

helloJ.setName( "helloWorld-test" )
helloJ.setInputSandbox( ['/afs/cern.ch/user/f/fstagni/userJobs/_inputHello.tgz',
                         '/afs/cern.ch/user/f/fstagni/userJobs/hello-script.py'] )

helloJ.setExecutable( "exe-script.py", "", "helloWorld.log" )

helloJ.setCPUTime( 172800 )
# helloJ.setBannedSites( ['LCG.CERN.ch', 'LCG.CNAF.it', 'LCG.GRIDKA.de',
# 'LCG.IN2P3.fr', 'LCG.NIKHEF.nl', 'LCG.PIC.es', 'LCG.RAL.uk',
# 'LCG.SARA.nl'] )
helloJ.setDestination( 'LCG.RAL.uk' )
result = dirac.submit( helloJ )
gLogger.info( "Hello world job: ", result )



inputs = ['/lhcb/LHCb/Collision12/PID.MDST/00017968/0000/00017968_00000034_1.pid.mdst',
          '/lhcb/LHCb/Collision12/PID.MDST/00017968/0000/00017968_00000008_1.pid.mdst',
          '/lhcb/LHCb/Collision12/PID.MDST/00017968/0000/00017968_00000005_1.pid.mdst',
          '/lhcb/LHCb/Collision12/PID.MDST/00017968/0000/00017968_00000014_1.pid.mdst']

gLogger.info( "Submitting standard user job" )

j = LHCbJob()


j.setName( "Standard user job" )
j.setInputSandbox( ['/afs/cern.ch/user/f/fstagni/userJobs/InputSandboxes/_input_sandbox_260_0.tgz',
                   '/afs/cern.ch/user/f/fstagni/userJobs/InputSandboxes/_input_sandbox_260_master.tgz'] )
j.setOutputSandbox( ['DVHistos.root'] )
j.setApplicationScript( "DaVinci", "v30r4p1",
                       "/afs/cern.ch/user/f/fstagni/userJobs/InputSandboxes/gaudi-script.py",
                       logFile = "standardJob.log" )
j.setInputData( inputs )
j.setAncestorDepth( 2 )
j.setSystemConfig( 'x86_64-slc5-gcc43-opt' )

j.setCPUTime( 172800 )

result = dirac.submit( j )
gLogger.info( 'Submission Result: ', result )



gLogger.info( "Submitting job with parametric input data" )

parametricJ = LHCbJob()

listOfLists = breakListIntoChunks( inputs, 2 )
parametricJ.setName( "parametric job" )
parametricJ.setInputSandbox( ['/afs/cern.ch/user/f/fstagni/userJobs/InputSandboxes/_input_sandbox_260_0.tgz',
                              '/afs/cern.ch/user/f/fstagni/userJobs/InputSandboxes/_input_sandbox_260_master.tgz'] )
parametricJ.setOutputSandbox( ['DVHistos.root'] )
parametricJ.setApplicationScript( "DaVinci", "v30r4p1",
                                  "/afs/cern.ch/user/f/fstagni/userJobs/InputSandboxes/gaudi-script.py",
                                  logFile = "prova.log" )
parametricJ.setAncestorDepth( 0 )
parametricJ.setSystemConfig( 'x86_64-slc5-gcc43-opt' )

parametricJ.setParametricInputData( listOfLists )

result = dirac.submit( parametricJ )
gLogger.info( 'Parametric job: ', result )
