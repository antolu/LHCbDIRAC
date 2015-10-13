""" Collection of user jobs for testing purposes
"""

# imports

from TestDIRAC.Utilities.utils import find_all
from TestDIRAC.Utilities.testJobDefinitions import *

from LHCbDIRAC.Interfaces.API.LHCbJob import LHCbJob
from LHCbDIRAC.Interfaces.API.DiracLHCb import DiracLHCb

# FIXME: use this to submit on behalf of another user
from DIRAC.Core.Utilities.Proxy import executeWithUserProxy

# parameters

jobClass = LHCbJob
diracClass = DiracLHCb

tier1s = ['LCG.CERN.ch', 'LCG.CNAF.it', 'LCG.GRIDKA.de', 'LCG.IN2P3.fr', 'LCG.NIKHEF.nl',
          'LCG.PIC.es', 'LCG.RAL.uk', 'LCG.SARA.nl', 'LCG.RRCKI.ru']


# List of jobs

@executeWithUserProxy
def helloWorldTestT2s():
  
  J = baseToAllJobs( 'helloWorldTestT2s', jobClass )
  J.setInputSandbox( [find_all( 'exe-script.py', '.', 'GridTestSubmission' )[0]] )
  J.setExecutable( "exe-script.py", "", "helloWorld.log" )
  J.setBannedSites( tier1s )
  return endOfAllJobs( J )

@executeWithUserProxy
def helloWorldTestCERN():

  J = baseToAllJobs( 'helloWorld-test-CERN', jobClass )
  J.setInputSandbox( [find_all( 'exe-script.py', '.', 'GridTestSubmission' )[0]] )
  J.setExecutable( "exe-script.py", "", "helloWorld.log" )
  J.setDestination( 'LCG.CERN.ch' )
  return endOfAllJobs( J )

@executeWithUserProxy
def helloWorldTestIN2P3():
  J = baseToAllJobs( 'helloWorld-test-IN2P3', jobClass )
  J.setInputSandbox( [find_all( 'exe-script.py', '.', 'GridTestSubmission' )[0]] )
  J.setExecutable( "exe-script.py", "", "helloWorld.log" )
  J.setDestination( 'LCG.IN2P3.fr' )
  return endOfAllJobs( J )

@executeWithUserProxy
def helloWorldTestGRIDKA():
  J = baseToAllJobs( 'helloWorld-test-GRIDKA', jobClass )
  J.setInputSandbox( [find_all( 'exe-script.py', '.', 'GridTestSubmission' )[0]] )
  J.setExecutable( "exe-script.py", "", "helloWorld.log" )
  J.setDestination( 'LCG.GRIDKA.de' )
  return endOfAllJobs( J )

@executeWithUserProxy
def helloWorldTestARC():

  J = baseToAllJobs( 'helloWorld-test-ARC', jobClass )
  J.setInputSandbox( [find_all( 'exe-script.py', '.', 'GridTestSubmission' )[0]] )
  J.setExecutable( "exe-script.py", "", "helloWorld.log" )
  J.setDestination( ['LCG.RAL.uk'] )
  return endOfAllJobs( J )

@executeWithUserProxy
def helloWorldTestSSH():

  J = baseToAllJobs( 'helloWorld-test-SSH', jobClass )
  J.setInputSandbox( [find_all( 'exe-script.py', '.', 'GridTestSubmission' )[0]] )
  J.setExecutable( "exe-script.py", "", "helloWorld.log" )
  J.setDestination( ['DIRAC.YANDEX.ru', 'DIRAC.OSC.us', 'DIRAC.Zurich.ch'] )
  return endOfAllJobs( J )

@executeWithUserProxy
def helloWorldTestSSHCondor():

  J = baseToAllJobs( 'helloWorld-test-SSHCondor', jobClass )
  J.setInputSandbox( [find_all( 'exe-script.py', '.', 'GridTestSubmission' )[0]] )
  J.setExecutable( "exe-script.py", "", "helloWorld.log" )
  J.setDestination( ['DIRAC.Syracuse.us'] )
  return endOfAllJobs( J )

@executeWithUserProxy
def helloWorldTestVAC():

  J = baseToAllJobs( 'helloWorld-test-VAC', jobClass )
  J.setInputSandbox( [find_all( 'exe-script.py', '.', 'GridTestSubmission' )[0]] )
  J.setExecutable( "exe-script.py", "", "helloWorld.log" )
  J.setDestination( ['VAC.Manchester.uk'] )
  return endOfAllJobs( J )

@executeWithUserProxy
def helloWorldTestCLOUD():

  J = baseToAllJobs( 'helloWorld-test-CLOUD', jobClass )
  J.setInputSandbox( [find_all( 'exe-script.py', '.', 'GridTestSubmission' )[0]] )
  J.setExecutable( "exe-script.py", "", "helloWorld.log" )
  J.setDestination( ['CLOUD.CERN.ch', 'CLOUD.EGI.eu'] )
  return endOfAllJobs( J )

@executeWithUserProxy
def helloWorldTestBOINC():

  J = baseToAllJobs( 'helloWorld-test-BOINC', jobClass )
  J.setInputSandbox( [find_all( 'exe-script.py', '.', 'GridTestSubmission' )[0]] )
  J.setExecutable( "exe-script.py", "", "helloWorld.log" )
  J.setDestination( ['BOINC.World.org'] )
  return endOfAllJobs( J )

@executeWithUserProxy
def helloWorldTestSLC6():

  J = baseToAllJobs( 'helloWorld-test-SLC6', jobClass )
  J.setInputSandbox( [find_all( 'exe-script.py', '.', 'GridTestSubmission' )[0]] )
  J.setExecutable( "exe-script.py", "", "helloWorld.log" )
  J.setPlatform( 'x86_64-slc6' )
  return endOfAllJobs( J )

@executeWithUserProxy
def helloWorldTestSLC5():

  J = baseToAllJobs( 'helloWorld-test-SLC5', jobClass )
  J.setInputSandbox( [find_all( 'exe-script.py', '.', 'GridTestSubmission' )[0]] )
  J.setExecutable( "exe-script.py", "", "helloWorld.log" )
  J.setPlatform( 'x86_64-slc5' )
  return endOfAllJobs( J )

@executeWithUserProxy
def jobWithOutput():

  J = baseToAllJobs( 'jobWithOutput', jobClass )
  J.setInputSandbox( [find_all( 'testFileUpload.txt', '.', 'GridTestSubmission' )[0]] + \
                     [find_all( 'exe-script.py', '.', 'GridTestSubmission' )[0]] )
  J.setExecutable( "exe-script.py", "", "helloWorld.log" )
  J.setOutputData( ['testFileUpload.txt'] )
  return endOfAllJobs( J )

@executeWithUserProxy
def jobWithOutputAndPrepend():

  J = baseToAllJobs( 'jobWithOutputAndPrepend', jobClass )
  J.setInputSandbox( [find_all( 'testFileUploadNewPath.txt', '.', 'GridTestSubmission' )[0]] + \
                     [find_all( 'exe-script.py', '.', 'GridTestSubmission' )[0]] )
  J.setExecutable( "exe-script.py", "", "helloWorld.log" )
  J.setOutputData( ['testFileUploadNewPath.txt'], filePrepend = 'testFilePrepend' )
  return endOfAllJobs( J )

@executeWithUserProxy
def jobWithOutputAndPrependWithUnderscore():

  J = baseToAllJobs( 'jobWithOutputAndPrependWithUnderscore', jobClass )
  J.setInputSandbox( [find_all( 'testFileUploadNewPath.txt', '.', 'GridTestSubmission' )[0]] + \
                     [find_all( 'exe-script.py', '.', 'GridTestSubmission' )[0]] )
  J.setExecutable( "exe-script.py", "", "helloWorld.log" )
  res = J.setOutputData( ['testFileUpload_NewPath.txt'], filePrepend = 'testFilePrepend' )
  if not res['OK']:
    return 0
  return endOfAllJobs( J )

@executeWithUserProxy
def jobWithOutputAndReplication():

  J = baseToAllJobs( 'jobWithOutputAndReplication', jobClass )
  J.setInputSandbox( [find_all( 'testFileReplication.txt', '.', 'GridTestSubmission' )[0]] + \
                     [find_all( 'exe-script.py', '.', 'GridTestSubmission' )[0]] )
  J.setExecutable( "exe-script.py", "", "helloWorld.log" )
  J.setOutputData( ['testFileReplication.txt'], replicate = 'True' )
  return endOfAllJobs( J )

@executeWithUserProxy
def jobWith2OutputsToBannedSE():

  J = baseToAllJobs( 'jobWith2OutputsToBannedSE', jobClass )
  J.setInputSandbox( [find_all( 'testFileUploadBanned-1.txt', '.', 'GridTestSubmission' )[0]] \
                     + [find_all( 'testFileUploadBanned-2.txt', '.', 'GridTestSubmission' )[0]] \
                     + [find_all( 'exe-script.py', '.', 'GridTestSubmission' )[0]] \
                     + [find_all( 'partialConfig.cfg', '.', 'GridTestSubmission' )[0] ] )
  J.setExecutable( "exe-script.py", "", "helloWorld.log" )
  J.setConfigArgs( 'partialConfig.cfg' )
  J.setDestination( 'LCG.PIC.es' )
  J.setOutputData( ['testFileUploadBanned-1.txt', 'testFileUploadBanned-2.txt'], OutputSE = ['PIC-USER'] )
  return endOfAllJobs( J )

@executeWithUserProxy
def jobWithSingleInputData():

  J = baseToAllJobs( 'jobWithSingleInputData', jobClass )
  J.setInputSandbox( [find_all( 'exe-script-with-input-single-location.py', '.', 'GridTestSubmission' )[0]] )
  J.setExecutable( "exe-script-with-input-single-location.py", "", "exeWithInput.log" )
  J.setInputData( '/lhcb/user/f/fstagni/test/testInputFileSingleLocation.txt' )  # this file should be at CERN-USER only
  J.setInputDataPolicy( 'download' )
  return endOfAllJobs( J )

@executeWithUserProxy
def jobWithSingleInputDataSpreaded():

  J = baseToAllJobs( 'jobWithSingleInputDataSpreaded', jobClass )
  J.setInputSandbox( [find_all( 'exe-script-with-input.py', '.', 'GridTestSubmission' )[0]] )
  J.setExecutable( "exe-script-with-input.py", "", "exeWithInput.log" )
  J.setInputData( '/lhcb/user/f/fstagni/test/testInputFile.txt' )  # this file should be at CERN-USER and IN2P3-USER
  J.setInputDataPolicy( 'download' )
  return endOfAllJobs( J )

@executeWithUserProxy
def jobWithInputDataAndAncestor():

  J = baseToAllJobs( 'jobWithInputDataAndAncestor', jobClass )
  J.setInputSandbox( [find_all( 'exe-script-with-input-and-ancestor.py', '.', 'GridTestSubmission' )[0]] )
  J.setExecutable( "exe-script-with-input-and-ancestor.py", "", "exeWithInput.log" )
  # WARNING: Collision10!!
  J.setInputData( '/lhcb/data/2010/SDST/00008375/0005/00008375_00053941_1.sdst' )  # this file should be at SARA-RDST
  J.setAncestorDepth( 1 )  # the ancestor should be /lhcb/data/2010/RAW/FULL/LHCb/COLLISION10/81616/081616_0000000213.raw (CERN and SARA)
  J.setInputDataPolicy( 'download' )
  return endOfAllJobs( J )

@executeWithUserProxy
def gaussJob():

  J = baseToAllJobs( 'gaussJob', jobClass )
  J.setInputSandbox( [find_all( 'prodConf_Gauss_00012345_00067890_1.py', '.', 'GridTestSubmission' )[0]] )
  J.setOutputSandbox( '00012345_00067890_1.sim' )

  optGauss = "$APPCONFIGOPTS/Gauss/Sim08-Beam3500GeV-md100-2011-nu2.py;"
  optDec = "$DECFILESROOT/options/34112104.py;"
  optPythia = "$LBPYTHIAROOT/options/Pythia.py;"
  optOpts = "$APPCONFIGOPTS/Gauss/G4PL_FTFP_BERT_EmNoCuts.py;"
  optCompr = "$APPCONFIGOPTS/Persistency/Compression-ZLIB-1.py;"
  optPConf = "prodConf_Gauss_00012345_00067890_1.py"
  options = optGauss + optDec + optPythia + optOpts + optCompr + optPConf
  J.setApplication( 'Gauss', 'v45r5', options,
                    extraPackages = 'AppConfig.v3r179;DecFiles.v27r14p1;ProdConf.v1r9',
                    systemConfig = 'x86_64-slc5-gcc43-opt' )
  J.setDIRACPlatform()
  J.setCPUTime( 172800 )
  return endOfAllJobs( J )


@executeWithUserProxy
def booleJob():

  J = baseToAllJobs( 'booleJob', jobClass )
  J.setInputSandbox( [find_all( 'prodConf_Boole_00012345_00067890_1.py', '.', 'GridTestSubmission' )[0]] )
  J.setOutputSandbox( '00012345_00067890_1.digi' )

  opts = "$APPCONFIGOPTS/Boole/Default.py;"
  optDT = "$APPCONFIGOPTS/Boole/DataType-2011.py;"
  optTCK = "$APPCONFIGOPTS/Boole/Boole-SiG4EnergyDeposit.py;"
  optComp = "$APPCONFIGOPTS/Persistency/Compression-ZLIB-1.py;"
  optPConf = "prodConf_Boole_00012345_00067890_1.py"
  options = opts + optDT + optTCK + optComp + optPConf

  J.setApplication( 'Boole', 'v26r3', options,
                    inputData = '/lhcb/user/f/fstagni/test/12345/12345678/00012345_00067890_1.sim',
                    extraPackages = 'AppConfig.v3r171;ProdConf.v1r9',
                    systemConfig = 'x86_64-slc5-gcc43-opt' )

  J.setDIRACPlatform()
  J.setCPUTime( 172800 )
  return endOfAllJobs( J )


@executeWithUserProxy
def wrongJob():

  print "\n Submitting gaudiRun job (Gauss only) that will use a configuration file that contains wrong info"
  print "This will generate a job that should become Completed, use the failover, and only later it will be Done"

  from LHCbTestDirac.Integration.Workflow.Test_UserJobs import createJob
  J = baseToAllJobs( 'wrongJob', jobClass )
  J = createJob( local = False )
  J.setName( "gaudirun-gauss-completed-than-done" )
  return endOfAllJobs( J )




#
# #     print "**********************************************************************************************************"
# #
# #     gLogger.info( "\n Submitting gaudiRun job (Gauss only) that should use TAG to run on a multi-core queue" )
# #
# #     gaudirunJob = LHCbJob()
# #
# #     gaudirunJob.setName( "gaudirun-Gauss-test-TAG-multicore" )
# #     gaudirunJob.setInputSandbox( [find_all( 'prodConf_Gauss_00012345_00067890_1.py', '.', 'GridTestSubmission' )[0]] )
# #     gaudirunJob.setOutputSandbox( '00012345_00067890_1.sim' )
# #
# #     optGauss = "$APPCONFIGOPTS/Gauss/Sim08-Beam3500GeV-md100-2011-nu2.py;"
# #     optDec = "$DECFILESROOT/options/34112104.py;"
# #     optPythia = "$LBPYTHIAROOT/options/Pythia.py;"
# #     optOpts = "$APPCONFIGOPTS/Gauss/G4PL_FTFP_BERT_EmNoCuts.py;"
# #     optCompr = "$APPCONFIGOPTS/Persistency/Compression-ZLIB-1.py;"
# #     optPConf = "prodConf_Gauss_00012345_00067890_1.py"
# #     options = optGauss + optDec + optPythia + optOpts + optCompr + optPConf
# #     gaudirunJob.setApplication( 'Gauss', 'v45r5', options,
# #                                 extraPackages = 'AppConfig.v3r179;DecFiles.v27r14p1;ProdConf.v1r9',
# #                                 systemConfig = 'x86_64-slc5-gcc43-opt' )
# #
# #     gaudirunJob.setDIRACPlatform()
# #     gaudirunJob.setCPUTime( 172800 )
# #     gaudirunJob.setTag( ['MultiCore'] )
# #
# #     result = self.dirac.submit( gaudirunJob )
# #     gLogger.info( 'Submission Result: ', result )
# #
# #     jobID = int( result['Value'] )
# #     jobsSubmittedList.append( jobID )
# #
# #     self.assert_( result['OK'] )
#
