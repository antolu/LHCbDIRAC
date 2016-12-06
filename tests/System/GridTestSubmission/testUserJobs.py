from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

import os.path

from DIRAC import gLogger

from DIRAC.tests.Utilities.utils import find_all

from LHCbDIRAC.Interfaces.API.LHCbJob import LHCbJob
from LHCbDIRAC.Interfaces.API.DiracLHCb import DiracLHCb
from DIRAC.DataManagementSystem.Utilities.DMSHelpers import DMSHelpers
from tests.Workflow.Integration.test_UserJobs import createJob

gLogger.setLevel( 'DEBUG' )

cwd = os.path.realpath( '.' )

########################################################################################

gLogger.info( "\n Submitting hello world job banning T1s" )

helloJ = LHCbJob()
dirac = DiracLHCb()

helloJ.setName( "helloWorld-test-T2s" )
helloJ.setInputSandbox( [find_all( 'exe-script.py', '.', 'GridTestSubmission' )[0]] )

helloJ.setExecutable( "exe-script.py", "", "helloWorld.log" )

helloJ.setCPUTime( 17800 )
try:
  tier1s = DMSHelpers().getTiers( tier = ( 0, 1 ) )
except AttributeError:
  tier1s = ['LCG.CERN.ch', 'LCG.CNAF.it', 'LCG.GRIDKA.de', 'LCG.IN2P3.fr',
            'LCG.NIKHEF.nl', 'LCG.PIC.es', 'LCG.RAL.uk', 'LCG.RRCKI.ru', 'LCG.SARA.nl']
cernSite = [s for s in tier1s if '.CERN.' in s][0]
helloJ.setBannedSites( tier1s )
result = dirac.submit( helloJ )
gLogger.info( "Hello world job: ", result )

########################################################################################

gLogger.info( "\n Submitting hello world job targeting %s" % cernSite )

helloJ = LHCbJob()
dirac = DiracLHCb()

helloJ.setName( "helloWorld-test-CERN" )
helloJ.setInputSandbox( [find_all( 'exe-script.py', '.', 'GridTestSubmission' )[0]] )
helloJ.setExecutable( "exe-script.py", "", "helloWorld.log" )

helloJ.setCPUTime( 17800 )
helloJ.setDestination( cernSite )
result = dirac.submit( helloJ )
gLogger.info( "Hello world job: ", result )

########################################################################################

gLogger.info( "\n Submitting hello world job targeting slc6 machines" )

helloJ = LHCbJob()
dirac = DiracLHCb()

helloJ.setName( "helloWorld-test-slc6" )
helloJ.setInputSandbox( [find_all( 'exe-script.py', '.', 'GridTestSubmission' )[0]] )
helloJ.setExecutable( "exe-script.py", "", "helloWorld.log" )

helloJ.setCPUTime( 17800 )
helloJ.setPlatform( 'x86_64-slc6' )
result = dirac.submit( helloJ )
gLogger.info( "Hello world job: ", result )

########################################################################################

gLogger.info( "\n Submitting hello world job targeting slc5 machines" )

helloJ = LHCbJob()
dirac = DiracLHCb()

helloJ.setName( "helloWorld-test-slc5" )
helloJ.setInputSandbox( [find_all( 'exe-script.py', '.', 'GridTestSubmission' )[0]] )
helloJ.setExecutable( "exe-script.py", "", "helloWorld.log" )

helloJ.setCPUTime( 17800 )
helloJ.setPlatform( 'x86_64-slc5' )
result = dirac.submit( helloJ )
gLogger.info( "Hello world job: ", result )

########################################################################################

gLogger.info( "\n Submitting a job that uploads an output" )

helloJ = LHCbJob()
dirac = DiracLHCb()

helloJ.setName( "upload-Output-test" )
helloJ.setInputSandbox( [find_all( 'testFileUpload.txt', '.', 'GridTestSubmission' )[0]] )
helloJ.setExecutable( "exe-script.py", "", "helloWorld.log" )

helloJ.setCPUTime( 17800 )

helloJ.setOutputData( ['testFileUpload.txt'] )

result = dirac.submit( helloJ )
gLogger.info( "Hello world with output: ", result )

########################################################################################

gLogger.info( "\n Submitting a job that uploads an output and replicates it" )

helloJ = LHCbJob()
dirac = DiracLHCb()

helloJ.setName( "upload-Output-test-with-replication" )
helloJ.setInputSandbox( [find_all( 'testFileReplication.txt', '.', 'GridTestSubmission' )[0]] )
helloJ.setExecutable( "exe-script.py", "", "helloWorld.log" )

helloJ.setCPUTime( 17800 )

helloJ.setOutputData( ['testFileReplication.txt'], replicate = 'True' )

result = dirac.submit( helloJ )
gLogger.info( "Hello world with output and replication: ", result )

########################################################################################

gLogger.info( "\n Submitting gaudiRun job (Gauss only)" )

gaudirunJob = LHCbJob()

gaudirunJob.setName( "gaudirun-Gauss-test" )
gaudirunJob.setInputSandbox( [find_all( 'prodConf_Gauss_00012345_00067890_1.py', '.', 'GridTestSubmission' )[0]] )
gaudirunJob.setOutputSandbox( '00012345_00067890_1.sim' )

optGauss = "$APPCONFIGOPTS/Gauss/Sim08-Beam3500GeV-md100-2011-nu2.py;"
optDec = "$DECFILESROOT/options/34112104.py;"
optPythia = "$LBPYTHIAROOT/options/Pythia.py;"
optOpts = "$APPCONFIGOPTS/Gauss/G4PL_FTFP_BERT_EmNoCuts.py;"
optCompr = "$APPCONFIGOPTS/Persistency/Compression-ZLIB-1.py;"
optPConf = "prodConf_Gauss_00012345_00067890_1.py"
options = optGauss + optDec + optPythia + optOpts + optCompr + optPConf
# gaudirunJob.addPackage( 'AppConfig', 'v3r179' )
# gaudirunJob.addPackage( 'DecFiles', 'v27r14p1' )
# gaudirunJob.addPackage( 'ProdConf', 'v1r9' )
gaudirunJob.setApplication( 'Gauss', 'v45r5', options, extraPackages = 'AppConfig.v3r179;DecFiles.v27r14p1;ProdConf.v1r9',
                            systemConfig = 'x86_64-slc5-gcc43-opt' )

gaudirunJob.setDIRACPlatform()
gaudirunJob.setCPUTime( 172800 )

result = dirac.submit( gaudirunJob )
gLogger.info( 'Submission Result: ', result )

########################################################################################

gLogger.info( "\n Submitting gaudiRun job (Gauss only) that should use TAG to run on a multi-core queue" )

gaudirunJob = LHCbJob()

gaudirunJob.setName( "gaudirun-Gauss-test-TAG-multicore" )
gaudirunJob.setInputSandbox( [find_all( 'prodConf_Gauss_00012345_00067890_1.py', '.', 'GridTestSubmission' )[0]] )
gaudirunJob.setOutputSandbox( '00012345_00067890_1.sim' )

optGauss = "$APPCONFIGOPTS/Gauss/Sim08-Beam3500GeV-md100-2011-nu2.py;"
optDec = "$DECFILESROOT/options/34112104.py;"
optPythia = "$LBPYTHIAROOT/options/Pythia.py;"
optOpts = "$APPCONFIGOPTS/Gauss/G4PL_FTFP_BERT_EmNoCuts.py;"
optCompr = "$APPCONFIGOPTS/Persistency/Compression-ZLIB-1.py;"
optPConf = "prodConf_Gauss_00012345_00067890_1.py"
options = optGauss + optDec + optPythia + optOpts + optCompr + optPConf
# gaudirunJob.addPackage( 'AppConfig', 'v3r179' )
# gaudirunJob.addPackage( 'DecFiles', 'v27r14p1' )
# gaudirunJob.addPackage( 'ProdConf', 'v1r9' )
gaudirunJob.setApplication( 'Gauss', 'v45r5', options, extraPackages = 'AppConfig.v3r179;DecFiles.v27r14p1;ProdConf.v1r9',
                            systemConfig = 'x86_64-slc5-gcc43-opt' )

gaudirunJob.setDIRACPlatform()
gaudirunJob.setCPUTime( 172800 )
gaudirunJob.setTag( ['MultiProcessor'] )

result = dirac.submit( gaudirunJob )
gLogger.info( 'Submission Result: ', result )

########################################################################################

gLogger.info( "\n Submitting gaudiRun job (Boole only)" )

gaudirunJob = LHCbJob()

gaudirunJob.setName( "gaudirun-Boole-test" )
gaudirunJob.setInputSandbox( [find_all( 'prodConf_Boole_00012345_00067890_1.py', '.', 'GridTestSubmission' )[0]] )
gaudirunJob.setOutputSandbox( '00012345_00067890_1.digi' )

opts = "$APPCONFIGOPTS/Boole/Default.py;"
optDT = "$APPCONFIGOPTS/Boole/DataType-2011.py;"
optTCK = "$APPCONFIGOPTS/Boole/Boole-SiG4EnergyDeposit.py;"
optComp = "$APPCONFIGOPTS/Persistency/Compression-ZLIB-1.py;"
optPConf = "prodConf_Boole_00012345_00067890_1.py"
options = opts + optDT + optTCK + optComp + optPConf

# gaudirunJob.addPackage( 'AppConfig', 'v3r171' )
gaudirunJob.setApplication( 'Boole', 'v26r3', options,
                            inputData = '/lhcb/user/f/fstagni/test/12345/12345678/00012345_00067890_1.sim',
                            extraPackages = 'AppConfig.v3r171;ProdConf.v1r9',
                            systemConfig = 'x86_64-slc5-gcc43-opt' )

gaudirunJob.setDIRACPlatform()
gaudirunJob.setCPUTime( 172800 )

result = dirac.submit( gaudirunJob )
gLogger.info( 'Submission Result: ', result )

########################################################################################

gLogger.info( "\n Submitting gaudiRun job (Gauss only) that will use a configuration file that contains wrong info" )
gLogger.info( "This will generate a job that should become Completed, use the failover, and only later it will be Done" )

gaudirunJob = createJob()
result = dirac.submit( gaudirunJob )
gLogger.info( 'Submission Result: ', result )
