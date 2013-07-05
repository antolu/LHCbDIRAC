from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

import os.path

from DIRAC import gLogger
from LHCbDIRAC.Interfaces.API.LHCbJob import LHCbJob
from LHCbDIRAC.Interfaces.API.DiracLHCb import DiracLHCb
from LHCbTestDirac.Utilities.utils import find_all
from LHCbTestDirac.Integration.Test_UserJobs import createJob

gLogger.setLevel( 'DEBUG' )

cwd = os.path.realpath( '.' )

########################################################################################

gLogger.info( "Submitting hello world job banning T1s" )

helloJ = LHCbJob()
dirac = DiracLHCb()

helloJ.setName( "helloWorld-test-T2s" )
helloJ.setInputSandbox( [find_all( 'exe-script.py', '.', 'GridTestSubmission' )[0]] )

helloJ.setExecutable( "exe-script.py", "", "helloWorld.log" )

helloJ.setCPUTime( 17800 )
helloJ.setBannedSites( ['LCG.CERN.ch', 'LCG.CNAF.it', 'LCG.GRIDKA.de', 'LCG.IN2P3.fr',
                        'LCG.NIKHEF.nl', 'LCG.PIC.es', 'LCG.RAL.uk', 'LCG.SARA.nl'] )
result = dirac.submit( helloJ )
gLogger.info( "Hello world job: ", result )

########################################################################################

gLogger.info( "Submitting hello world job targeting LCG.CERN.ch" )

helloJ = LHCbJob()
dirac = DiracLHCb()

helloJ.setName( "helloWorld-test-CERN" )
helloJ.setInputSandbox( [find_all( 'exe-script.py', '.', 'GridTestSubmission' )[0]] )
helloJ.setExecutable( "exe-script.py", "", "helloWorld.log" )

helloJ.setCPUTime( 17800 )
helloJ.setDestination( 'LCG.CERN.ch' )
result = dirac.submit( helloJ )
gLogger.info( "Hello world job: ", result )

########################################################################################

gLogger.info( "Submitting gaudiRun job (Gauss only)" )

gaudirunJob = LHCbJob()

gaudirunJob.setName( "gaudirun-Gauss-test" )
gaudirunJob.setInputSandbox( [find_all( 'prodConf_Gauss_00012345_00067890_1.py', '.', 'GridTestSubmission' )[0]] )
gaudirunJob.setOutputSandbox( '00012345_00067890_1.sim' )

optGauss = "$APPCONFIGOPTS/Gauss/Sim08-Beam4000GeV-mu100-2012-nu2.5.py;"
optDec = "$DECFILESROOT/options/11102400.py;"
optPythia = "$LBPYTHIA8ROOT/options/Pythia8.py;"
optOpts = " $APPCONFIGOPTS/Gauss/G4PL_FTFP_BERT_EmNoCuts.py;"
optCompr = "$APPCONFIGOPTS/Persistency/Compression-ZLIB-1.py;"
optPConf = "prodConf_Gauss_00012345_00067890_1.py"
options = optGauss + optDec + optPythia + optOpts + optCompr + optPConf
gaudirunJob.addPackage( 'AppConfig', 'v3r171' )
gaudirunJob.addPackage( 'ProdConf', 'v1r9' )
gaudirunJob.setApplication( 'Gauss', 'v45r3', options, extraPackages = 'AppConfig.v3r171;ProdConf.v1r9' )

gaudirunJob.setSystemConfig( 'ANY' )
gaudirunJob.setCPUTime( 172800 )

result = dirac.submit( gaudirunJob )
gLogger.info( 'Submission Result: ', result )

########################################################################################

gLogger.info( "Submitting gaudiRun job (Boole only)" )

gaudirunJob = LHCbJob()

gaudirunJob.setName( "gaudirun-Boole-test" )
gaudirunJob.setInputSandbox( [find_all( 'prodConf_Boole_00012345_00067890_1.py', '.', 'GridTestSubmission' )[0]] )
gaudirunJob.setOutputSandbox( '00012345_00067890_1.digi' )

opts = "$APPCONFIGOPTS/Boole/Default.py;"
optDT = "$APPCONFIGOPTS/Boole/DataType-2012.py;"
optTCK = "$APPCONFIGOPTS/L0/L0TCK-0x0042.py;"
optComp = "$APPCONFIGOPTS/Persistency/Compression-ZLIB-1.py;"
optPConf = "prodConf_Boole_00012345_00067890_1.py"
options = opts + optDT + optTCK + optComp + optPConf

gaudirunJob.addPackage( 'AppConfig', 'v3r155' )
gaudirunJob.setApplication( 'Boole', 'v24r0', options,
                            inputData = '/lhcb/user/f/fstagni/test/12345/12345678/00012345_00067890_1.sim',
                            extraPackages = 'AppConfig.v3r155;ProdConf.v1r9' )

gaudirunJob.setSystemConfig( 'ANY' )
gaudirunJob.setCPUTime( 172800 )

result = dirac.submit( gaudirunJob )
gLogger.info( 'Submission Result: ', result )

########################################################################################

gLogger.info( "Submitting gaudiRun job (Gauss only) that will use a configuration file that contains wrong info" )
gLogger.info( "This will generate a job that should become Completed, use the failover, and only later it will be Done" )

gaudirunJob = createJob()
result = dirac.submit( gaudirunJob )
gLogger.info( 'Submission Result: ', result )
