""" Collection of user jobs for testing purposes
"""

#pylint: disable=missing-docstring

# imports

from DIRAC.tests.Utilities.utils import find_all
from DIRAC.tests.Utilities.testJobDefinitions import *
import time
import os
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
wdir = os.getcwd()

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

  timenow = time.strftime("%s")
  with open( os.path.join( wdir,timenow+"testFileUpload.txt" ), "w" ) as f:
    f.write( timenow )
  J = baseToAllJobs( 'jobWithOutput', jobClass )
  J.setInputSandbox( [find_all( timenow+'testFileUpload.txt', wdir, 'GridTestSubmission' )[0]] + \
                     [find_all( 'exe-script.py', '.', 'GridTestSubmission' )[0]] )
  J.setExecutable( "exe-script.py", "", "helloWorld.log" )
  J.setOutputData( [timenow+'testFileUpload.txt'] )
  os.remove( os.path.join( wdir,timenow+"testFileUpload.txt" )
  return endOfAllJobs( J )

@executeWithUserProxy
def jobWithOutputAndPrepend():

  timenow = time.strftime("%s")
  with open( os.path.join( wdir,timenow+"testFileUploadNewPath.txt" ), "w" ) as f:
    f.write( timenow )
  J = baseToAllJobs( 'jobWithOutputAndPrepend', jobClass )
  J.setInputSandbox( [find_all( timenow+'testFileUploadNewPath.txt', wdir, 'GridTestSubmission' )[0]] + \
                     [find_all( 'exe-script.py', '.', 'GridTestSubmission' )[0]] )
  J.setExecutable( "exe-script.py", "", "helloWorld.log" )
  J.setOutputData( [timenow+'testFileUploadNewPath.txt'], filePrepend = 'testFilePrepend' )
  os.remove( os.path.join( wdir,timenow+"testFileUploadNewPath.txt" )
  return endOfAllJobs( J )

@executeWithUserProxy
def jobWithOutputAndPrependWithUnderscore():

  timenow = time.strftime("%s")
  with open( os.path.join( wdir,timenow+"testFileUpload_NewPath.txt" ), "w")  as f:
    f.write( timenow )
  J = baseToAllJobs( 'jobWithOutputAndPrependWithUnderscore', jobClass )
  J.setInputSandbox( [find_all( timenow+'testFileUploadNewPath.txt', wdir, 'GridTestSubmission' )[0]] + \
                     [find_all( 'exe-script.py', '.', 'GridTestSubmission' )[0]] )
  J.setExecutable( "exe-script.py", "", "helloWorld.log" )
  res = J.setOutputData( [timenow+'testFileUpload_NewPath.txt'], filePrepend = 'testFilePrepend' )
  if not res['OK']:
    return 0
  os.remove( os.path.join( wdir,timenow+'testFileUpload_NewPath.txt' )
  return endOfAllJobs( J )

@executeWithUserProxy
def jobWithOutputAndReplication():

  timenow = strtime.strftime("%s")
  with open( os.path.join( wdir,timenow+"testFileReplication.txt" ), "w" ) as f:
    f.write( timenow )
  J = baseToAllJobs( 'jobWithOutputAndReplication', jobClass )
  J.setInputSandbox( [find_all( timenow+'testFileReplication.txt', wdir, 'GridTestSubmission' )[0]] + \
                     [find_all( 'exe-script.py', '.', 'GridTestSubmission' )[0]] )
  J.setExecutable( "exe-script.py", "", "helloWorld.log" )
  J.setOutputData( [timenow+'testFileReplication.txt'], replicate = 'True' )
  os.remove( os.path.join( wdir,timenow+'testFileReplication.txt' )
  return endOfAllJobs( J )

@executeWithUserProxy
def jobWith2OutputsToBannedSE():

  timenow = time.strftime("%s")
  with open( os.path.join( wdir,timenow+"testFileUploadBanned-1.txt" ), "w" ) as f:
    f.write( timenow ) 
  with open( os.path.join( wdir,timenow+"testFileUploadBanned-2.txt" ), "w" ) as f:
    f.write( timenow )
  J = baseToAllJobs( 'jobWith2OutputsToBannedSE', jobClass )
  J.setInputSandbox( [find_all( timenow+'testFileUploadBanned-1.txt', wdir, 'GridTestSubmission' )[0]] \
                     + [find_all( timenow+'testFileUploadBanned-2.txt', wdir, 'GridTestSubmission' )[0]] \
                     + [find_all( 'exe-script.py', '.', 'GridTestSubmission' )[0]] \
                     + [find_all( 'partialConfig.cfg', '.', 'GridTestSubmission' )[0] ] )
  J.setExecutable( "exe-script.py", "", "helloWorld.log" )
  J.setConfigArgs( 'partialConfig.cfg' )
  J.setDestination( 'LCG.PIC.es' )
  J.setOutputData( [timenow+'testFileUploadBanned-1.txt', timenow+'testFileUploadBanned-2.txt'], OutputSE = ['PIC-USER'] )
  os.remove( os.path.join( wdir,timenow+'testFileUploadBanned-1.txt' ) 
  os.remove( os.path.join( wdir,timenow+'testFileUploadBanned-2.txt' )  
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
  # the ancestor should be /lhcb/data/2010/RAW/FULL/LHCb/COLLISION10/81616/081616_0000000213.raw (CERN and SARA)
  J.setAncestorDepth( 1 )  #pylint: disable=no-member
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
  J.setApplication( 'Gauss', 'v45r5', options, #pylint: disable=no-member
                    extraPackages = 'AppConfig.v3r179;DecFiles.v27r14p1;ProdConf.v1r9',
                    systemConfig = 'x86_64-slc5-gcc43-opt' )
  J.setDIRACPlatform()  #pylint: disable=no-member
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

  J.setApplication( 'Boole', 'v26r3', options, #pylint: disable=no-member
                    inputData = '/lhcb/user/f/fstagni/test/12345/12345678/00012345_00067890_1.sim',
                    extraPackages = 'AppConfig.v3r171;ProdConf.v1r9',
                    systemConfig = 'x86_64-slc5-gcc43-opt' )

  J.setDIRACPlatform() #pylint: disable=no-member
  J.setCPUTime( 172800 )
  return endOfAllJobs( J )


@executeWithUserProxy
def wrongJob():

  print "\n Submitting gaudiRun job (Gauss only) that will use a configuration file that contains wrong info"
  print "This will generate a job that should become Completed, use the failover, and only later it will be Done"

  from tests.Workflow.Integration.test_UserJobs import createJob
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
# #     gaudirunJob.setTag( ['MultiProcessor'] )
# #
# #     result = self.dirac.submit( gaudirunJob )
# #     gLogger.info( 'Submission Result: ', result )
# #
# #     jobID = int( result['Value'] )
# #     jobsSubmittedList.append( jobID )
# #
# #     self.assert_( result['OK'] )
#
