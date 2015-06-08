""" Collection of user jobs for testing purposes
"""

from TestDIRAC.Utilities.utils import find_all
from TestDIRAC.Utilities.testJobDefinitions import *


tier1s = ['LCG.CERN.ch', 'LCG.CNAF.it', 'LCG.GRIDKA.de', 'LCG.IN2P3.fr', 'LCG.NIKHEF.nl',
          'LCG.PIC.es', 'LCG.RAL.uk', 'LCG.SARA.nl', 'LCG.RRCKI.ru']

# Common functions

def getJob():
  from LHCbDIRAC.Interfaces.API.LHCbJob import LHCbJob
  return Job()

def getDIRAC():
  from LHCbDIRAC.Interfaces.API.DiracLHCb import DiracLHCb
  return Dirac()


# List of jobs

def helloWorldTestT2s():
  
  J = baseToAllJobs( 'helloWorldTestT2s' )
  J.setInputSandbox( [find_all( 'exe-script.py', '.', 'GridTestSubmission' )[0]] )
  J.setExecutable( "exe-script.py", "", "helloWorld.log" )
  J.setBannedSites( tier1s )
  return endOfAllJobs( J )

def helloWorldTestCERN():

  J = baseToAllJobs( 'helloWorld-test-CERN' )
  J.setInputSandbox( [find_all( 'exe-script.py', '.', 'GridTestSubmission' )[0]] )
  J.setExecutable( "exe-script.py", "", "helloWorld.log" )
  J.setDestination( 'LCG.CERN.ch' )
  return endOfAllJobs( J )

def helloWorldTestSLC6():

  J = baseToAllJobs( 'helloWorld-test-SLC6' )
  J.setInputSandbox( [find_all( 'exe-script.py', '.', 'GridTestSubmission' )[0]] )
  J.setExecutable( "exe-script.py", "", "helloWorld.log" )
  J.setPlatform( 'x86_64-slc6' )
  return endOfAllJobs( J )

def helloWorldTestSLC5():

  J = baseToAllJobs( 'helloWorld-test-SLC5' )
  J.setInputSandbox( [find_all( 'exe-script.py', '.', 'GridTestSubmission' )[0]] )
  J.setExecutable( "exe-script.py", "", "helloWorld.log" )
  J.setPlatform( 'x86_64-slc5' )
  return endOfAllJobs( J )

def jobWithOutput():

  J = baseToAllJobs( 'jobWithOutput' )
  J.setInputSandbox( [find_all( 'testFileUpload.txt', '.', 'GridTestSubmission' )[0]] + \
                     [find_all( 'exe-script.py', '.', 'GridTestSubmission' )[0]] )
  J.setExecutable( "exe-script.py", "", "helloWorld.log" )
  J.setOutputData( ['testFileUpload.txt'] )
  return endOfAllJobs( J )

def jobWithOutputAndPrepend():

  J = baseToAllJobs( 'jobWithOutputAndPrepend' )
  J.setInputSandbox( [find_all( 'testFileUploadNewPath.txt', '.', 'GridTestSubmission' )[0]] + \
                     [find_all( 'exe-script.py', '.', 'GridTestSubmission' )[0]] )
  J.setExecutable( "exe-script.py", "", "helloWorld.log" )
  J.setOutputData( ['testFileUploadNewPath.txt'], filePrepend = 'testFilePrepend' )
  return endOfAllJobs( J )

def jobWithOutputAndPrependWithUnderscore():

  J = baseToAllJobs( 'jobWithOutputAndPrependWithUnderscore' )
  J.setInputSandbox( [find_all( 'testFileUploadNewPath.txt', '.', 'GridTestSubmission' )[0]] + \
                     [find_all( 'exe-script.py', '.', 'GridTestSubmission' )[0]] )
  J.setExecutable( "exe-script.py", "", "helloWorld.log" )
  J.setOutputData( ['testFileUpload_NewPath.txt'], filePrepend = 'testFilePrepend' )
  return endOfAllJobs( J )

def jobWithOutputAndReplication():

  J = baseToAllJobs( 'jobWithOutputAndReplication' )
  J.setInputSandbox( [find_all( 'testFileReplication.txt', '.', 'GridTestSubmission' )[0]] + \
                     [find_all( 'exe-script.py', '.', 'GridTestSubmission' )[0]] )
  J.setExecutable( "exe-script.py", "", "helloWorld.log" )
  J.setOutputData( ['testFileReplication.txt'], replicate = 'True' )
  return endOfAllJobs( J )

def jobWith2OutputsToBannedSE():

  J = baseToAllJobs( 'jobWith2OutputsToBannedSE' )
  J.setInputSandbox( [find_all( 'testFileUploadBanned-1.txt', '.', 'GridTestSubmission' )[0]] \
                     + [find_all( 'testFileUploadBanned-2.txt', '.', 'GridTestSubmission' )[0]] \
                     + [find_all( 'exe-script.py', '.', 'GridTestSubmission' )[0]] \
                     + [find_all( 'partialConfig.cfg', '.', 'GridTestSubmission' )[0] ] )
  J.setExecutable( "exe-script.py", "", "helloWorld.log" )
  J.setConfigArgs( 'partialConfig.cfg' )
  J.setDestination( 'LCG.PIC.es' )
  J.setOutputData( ['testFileUploadBanned-1.txt', 'testFileUploadBanned-2.txt'], OutputSE = ['PIC-USER'] )
  return endOfAllJobs( J )

def jobWithSingleInputData():

  J = baseToAllJobs( 'jobWithSingleInputData' )
  J.setInputSandbox( [find_all( 'exe-script-with-input.py', '.', 'GridTestSubmission' )[0]] )
  J.setExecutable( "exe-script-with-input.py", "", "exeWithInput.log" )
  J.setInputData( '/lhcb/user/f/fstagni/test/testInputFileSingleLocation.txt' )  # this file should be at CERN-USER only
  J.setInputDataPolicy( 'download' )
  return endOfAllJobs( J )

def jobWithSingleInputDataSpreaded():

  J = baseToAllJobs( 'jobWithSingleInputDataSpreaded' )
  J.setInputSandbox( [find_all( 'exe-script-with-input.py', '.', 'GridTestSubmission' )[0]] )
  J.setExecutable( "exe-script-with-input.py", "", "exeWithInput.log" )
  J.setInputData( '/lhcb/user/f/fstagni/test/testInputFile.txt' )  # this file should be at CERN-USER and IN2P3-USER
  J.setInputDataPolicy( 'download' )
  return endOfAllJobs( J )

def gaussJob():

  J = baseToAllJobs( 'gaussJob' )
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


def booleJob():

  J = baseToAllJobs( 'booleJob' )
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


def wrongJob():

  gLogger.info( "\n Submitting gaudiRun job (Gauss only) that will use a configuration file that contains wrong info" )
  gLogger.info( "This will generate a job that should become Completed, use the failover, and only later it will be Done" )

  from LHCbTestDirac.Integration.Workflow.Test_UserJobs import createJob
  J = baseToAllJobs( 'booleJob' )
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
