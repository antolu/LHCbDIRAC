""" Collection of user jobs for testing purposes
"""

# pylint: disable=protected-access, wrong-import-position, invalid-name, missing-docstring, unused-wildcard-import

# imports
import time
import os
import errno

from DIRAC.DataManagementSystem.Utilities.DMSHelpers import DMSHelpers
from DIRAC.tests.Utilities.testJobDefinitions import *
from DIRAC.Core.Utilities.Proxy import executeWithUserProxy
from LHCbDIRAC.Interfaces.API.LHCbJob import LHCbJob
from LHCbDIRAC.Interfaces.API.DiracLHCb import DiracLHCb
try:
  from LHCbDIRAC.tests.Workflow.Integration.Test_UserJobs import createJob
except ImportError:
  from tests.Workflow.Integration.Test_UserJobs import createJob

from LHCbDIRAC import rootPath

# parameters

jobClass = LHCbJob
diracClass = DiracLHCb

try:
  tier1s = DMSHelpers().getTiers( tier = ( 0, 1 ) )
except AttributeError:
  tier1s = ['LCG.CERN.cern', 'LCG.CNAF.it', 'LCG.GRIDKA.de', 'LCG.IN2P3.fr',
            'LCG.NIKHEF.nl', 'LCG.PIC.es', 'LCG.RAL.uk', 'LCG.RRCKI.ru', 'LCG.SARA.nl']

# List of jobs
wdir = os.getcwd()

@executeWithUserProxy
def helloWorldTestT2s():

  job = baseToAllJobs( 'helloWorldTestT2s', jobClass )
  job.setInputSandbox( [find_all( 'exe-script.py', rootPath, '/LHCbDIRAC/tests/System/GridTestSubmission' )[0]] )
  job.setExecutable( "exe-script.py", "", "helloWorld.log" )
  job.setBannedSites( tier1s )
  return endOfAllJobs( job )

@executeWithUserProxy
def helloWorldTestCERN():

  job = baseToAllJobs( 'helloWorld-test-CERN', jobClass )
  job.setInputSandbox( [find_all( 'exe-script.py', rootPath, '/LHCbDIRAC/tests/System/GridTestSubmission' )[0]] )
  job.setExecutable( "exe-script.py", "", "helloWorld.log" )
  job.setDestination( 'LCG.CERN.cern' )
  return endOfAllJobs( job )

@executeWithUserProxy
def helloWorldTestIN2P3():
  job = baseToAllJobs( 'helloWorld-test-IN2P3', jobClass )
  job.setInputSandbox( [find_all( 'exe-script.py', rootPath, '/LHCbDIRAC/tests/System/GridTestSubmission' )[0]] )
  job.setExecutable( "exe-script.py", "", "helloWorld.log" )
  job.setDestination( 'LCG.IN2P3.fr' )
  return endOfAllJobs( job )

@executeWithUserProxy
def helloWorldTestGRIDKA():
  job = baseToAllJobs( 'helloWorld-test-GRIDKA', jobClass )
  job.setInputSandbox( [find_all( 'exe-script.py', rootPath, '/LHCbDIRAC/tests/System/GridTestSubmission' )[0]] )
  job.setExecutable( "exe-script.py", "", "helloWorld.log" )
  job.setDestination( 'LCG.GRIDKA.de' )
  return endOfAllJobs( job )

@executeWithUserProxy
def helloWorldTestARC():

  job = baseToAllJobs( 'helloWorld-test-ARC', jobClass )
  job.setInputSandbox( [find_all( 'exe-script.py', rootPath, '/LHCbDIRAC/tests/System/GridTestSubmission' )[0]] )
  job.setExecutable( "exe-script.py", "", "helloWorld.log" )
  job.setDestination( ['LCG.RAL.uk'] )
  return endOfAllJobs( job )

@executeWithUserProxy
def helloWorldTestSSH():

  job = baseToAllJobs( 'helloWorld-test-SSH', jobClass )
  job.setInputSandbox( [find_all( 'exe-script.py', rootPath, '/LHCbDIRAC/tests/System/GridTestSubmission' )[0]] )
  job.setExecutable( "exe-script.py", "", "helloWorld.log" )
  job.setDestination( ['DIRAC.YANDEX.ru', 'DIRAC.OSC.us', 'DIRAC.Zurich.ch'] )
  return endOfAllJobs( job )

@executeWithUserProxy
def helloWorldTestSSHCondor():

  job = baseToAllJobs( 'helloWorld-test-SSHCondor', jobClass )
  job.setInputSandbox( [find_all( 'exe-script.py', rootPath, '/LHCbDIRAC/tests/System/GridTestSubmission' )[0]] )
  job.setExecutable( "exe-script.py", "", "helloWorld.log" )
  job.setDestination( ['DIRAC.Syracuse.us'] )
  return endOfAllJobs( job )

@executeWithUserProxy
def helloWorldTestVAC():

  job = baseToAllJobs( 'helloWorld-test-VAC', jobClass )
  job.setInputSandbox( [find_all( 'exe-script.py', rootPath, '/LHCbDIRAC/tests/System/GridTestSubmission' )[0]] )
  job.setExecutable( "exe-script.py", "", "helloWorld.log" )
  job.setDestination( ['VAC.Manchester.uk'] )
  return endOfAllJobs( job )

@executeWithUserProxy
def helloWorldTestCLOUD():

  job = baseToAllJobs( 'helloWorld-test-CLOUD', jobClass )
  job.setInputSandbox( [find_all( 'exe-script.py', rootPath, '/LHCbDIRAC/tests/System/GridTestSubmission' )[0]] )
  job.setExecutable( "exe-script.py", "", "helloWorld.log" )
  job.setDestination( ['CLOUD.CERN.cern', 'CLOUD.YANDEX.ru'] )
  return endOfAllJobs( job )

@executeWithUserProxy
def helloWorldTestBOINC():

  job = baseToAllJobs( 'helloWorld-test-BOINC', jobClass )
  job.setInputSandbox( [find_all( 'exe-script.py', rootPath, '/LHCbDIRAC/tests/System/GridTestSubmission' )[0]] )
  job.setExecutable( "exe-script.py", "", "helloWorld.log" )
  job.setDestination( ['BOINC.World.org'] )
  return endOfAllJobs( job )

@executeWithUserProxy
def helloWorldTestSLC6():

  job = baseToAllJobs( 'helloWorld-test-SLC6', jobClass )
  job.setInputSandbox( [find_all( 'exe-script.py', rootPath, '/LHCbDIRAC/tests/System/GridTestSubmission' )[0]] )
  job.setExecutable( "exe-script.py", "", "helloWorld.log" )
  job.setPlatform( 'x86_64-slc6' )
  return endOfAllJobs( job )

@executeWithUserProxy
def helloWorldTestSLC5():

  job = baseToAllJobs( 'helloWorld-test-SLC5', jobClass )
  job.setInputSandbox( [find_all( 'exe-script.py', rootPath, '/LHCbDIRAC/tests/System/GridTestSubmission' )[0]] )
  job.setExecutable( "exe-script.py", "", "helloWorld.log" )
  job.setPlatform( 'x86_64-slc5' )
  return endOfAllJobs( job )

@executeWithUserProxy
def jobWithOutput():

  timenow = time.strftime( "%s" )
  with open( os.path.join( wdir, timenow + "testFileUpload.txt" ), "w" ) as f:
    f.write( timenow )
  job = baseToAllJobs( 'jobWithOutput', jobClass )
  job.setInputSandbox( [find_all( timenow + 'testFileUpload.txt', '.' )[0]] + \
                     [find_all( 'exe-script.py', rootPath, '/LHCbDIRAC/tests/System/GridTestSubmission' )[0]] )
  job.setExecutable( "exe-script.py", "", "helloWorld.log" )
  job.setOutputData( [timenow + 'testFileUpload.txt'] )
  res = endOfAllJobs( job )
  try:
    os.remove( os.path.join( wdir, timenow + "testFileUpload.txt" ) )
  except OSError as e:
    return e.errno == errno.ENOENT
  return res

@executeWithUserProxy
def jobWithOutputAndPrepend():

  timenow = time.strftime( "%s" )
  with open( os.path.join( wdir, timenow + "testFileUploadNewPath.txt" ), "w" ) as f:
    f.write( timenow )
  job = baseToAllJobs( 'jobWithOutputAndPrepend', jobClass )
  job.setInputSandbox( [find_all( timenow + 'testFileUploadNewPath.txt', '.' )[0]] + \
                     [find_all( 'exe-script.py', rootPath, '/LHCbDIRAC/tests/System/GridTestSubmission' )[0]] )
  job.setExecutable( "exe-script.py", "", "helloWorld.log" )
  job.setOutputData( [timenow + 'testFileUploadNewPath.txt'], filePrepend = 'testFilePrepend' )
  res = endOfAllJobs( job )
  try:
    os.remove( os.path.join( wdir, timenow + "testFileUploadNewPath.txt" ) )
  except OSError as e:
    return e.errno == errno.ENOENT
  return res

@executeWithUserProxy
def jobWithOutputAndPrependWithUnderscore():

  timenow = time.strftime( "%s" )
  with open( os.path.join( wdir, timenow + "testFileUpload_NewPath.txt" ), "w" )  as f:
    f.write( timenow )
  job = baseToAllJobs( 'jobWithOutputAndPrependWithUnderscore', jobClass )
  job.setInputSandbox( [find_all( timenow + 'testFileUpload_NewPath.txt', '.' )[0]] + \
                     [find_all( 'exe-script.py', rootPath, '/LHCbDIRAC/tests/System/GridTestSubmission' )[0]] )
  job.setExecutable( "exe-script.py", "", "helloWorld.log" )
  res = job.setOutputData( [timenow + 'testFileUpload_NewPath.txt'], filePrepend = 'testFilePrepend' )
  if not res['OK']:
    return 0
  res = endOfAllJobs( job )
  try:
    os.remove( os.path.join( wdir, timenow + 'testFileUpload_NewPath.txt' ) )
  except OSError as e:
    return e.errno == errno.ENOENT
  return res

@executeWithUserProxy
def jobWithOutputAndReplication():

  timenow = time.strftime( "%s" )
  with open( os.path.join( wdir, timenow + "testFileReplication.txt" ), "w" ) as f:
    f.write( timenow )
  job = baseToAllJobs( 'jobWithOutputAndReplication', jobClass )
  job.setInputSandbox( [find_all( timenow + 'testFileReplication.txt', '.' )[0]] + \
                     [find_all( 'exe-script.py', rootPath, '/LHCbDIRAC/tests/System/GridTestSubmission' )[0]] )
  job.setExecutable( "exe-script.py", "", "helloWorld.log" )
  job.setOutputData( [timenow + 'testFileReplication.txt'], replicate = 'True' )
  res = endOfAllJobs( job )
  try:
    os.remove( os.path.join( wdir, timenow + 'testFileReplication.txt' ) )
  except OSError as e:
    return e.errno == errno.ENOENT
  return res

@executeWithUserProxy
def jobWith2OutputsToBannedSE():

  timenow = time.strftime( "%s" )
  with open( os.path.join( wdir, timenow + "testFileUploadBanned-1.txt" ), "w" ) as f:
    f.write( timenow )
  with open( os.path.join( wdir, timenow + "testFileUploadBanned-2.txt" ), "w" ) as f:
    f.write( timenow )
  job = baseToAllJobs( 'jobWith2OutputsToBannedSE', jobClass )
  job.setInputSandbox( [find_all( timenow + 'testFileUploadBanned-1.txt', '.' )[0]] \
                     + [find_all( timenow + 'testFileUploadBanned-2.txt', '.' )[0]] \
                     + [find_all( 'exe-script.py', rootPath, '/LHCbDIRAC/tests/System/GridTestSubmission' )[0]] \
                     + [find_all( 'partialConfig.cfg', rootPath, '/LHCbDIRAC/tests/System/GridTestSubmission' )[0] ] )
  job.setExecutable( "exe-script.py", "", "helloWorld.log" )
  job.setConfigArgs( 'partialConfig.cfg' )
  job.setDestination( 'LCG.PIC.es' )
  job.setOutputData( [timenow + 'testFileUploadBanned-1.txt', timenow + 'testFileUploadBanned-2.txt'], OutputSE = ['PIC-USER'] )
  res = endOfAllJobs( job )
  try:
    os.remove( os.path.join( wdir, timenow + 'testFileUploadBanned-1.txt' ) )
  except OSError as e:
    return e.errno == errno.ENOENT
  try:
    os.remove( os.path.join( wdir, timenow + 'testFileUploadBanned-2.txt' ) )
  except OSError as e:
    return e.errno == errno.ENOENT
  return res

@executeWithUserProxy
def jobWithSingleInputData():

  job = baseToAllJobs( 'jobWithSingleInputData', jobClass )
  job.setInputSandbox( [find_all( 'exe-script-with-input-single-location.py', rootPath, '/LHCbDIRAC/tests/System/GridTestSubmission' )[0]] )
  job.setExecutable( "exe-script-with-input-single-location.py", "", "exeWithInput.log" )
  job.setInputData( '/lhcb/user/f/fstagni/test/testInputFileSingleLocation.txt' )  # this file should be at CERN-USER only
  job.setInputDataPolicy( 'download' )
  res = endOfAllJobs( job )
  return res

@executeWithUserProxy
def jobWithSingleInputDataCERN():

  job = baseToAllJobs( 'jobWithSingleInputDataCERN', jobClass )
  job.setInputSandbox( [find_all( 'exe-script-with-input-single-location.py', rootPath, '/LHCbDIRAC/tests/System/GridTestSubmission' )[0]] )
  job.setExecutable( "exe-script-with-input-single-location.py", "", "exeWithInput.log" )
  job.setInputData( '/lhcb/user/f/fstagni/test/testInputFileSingleLocation.txt' )  # this file should be at CERN-USER only
  job.setInputDataPolicy( 'protocol' )
  job.setDestination( ['LCG.CERN.cern'] )
  res = endOfAllJobs( job )
  return res

@executeWithUserProxy
def jobWithSingleInputDataRAL():

  job = baseToAllJobs( 'jobWithSingleInputDataRAL', jobClass )
  job.setInputSandbox( [find_all( 'exe-script-with-input-single-location.py', rootPath, '/LHCbDIRAC/tests/System/GridTestSubmission' )[0]] )
  job.setExecutable( "exe-script-with-input-single-location.py", "", "exeWithInput.log" )
  job.setInputData( '/lhcb/user/f/fstagni/test/testInputFileSingleLocation.txt' )  # this file should be at CERN-USER only
  job.setInputDataPolicy( 'protocol' )
  job.setDestination( ['LCG.RAL.uk'] )
  res = endOfAllJobs( job )
  return res

@executeWithUserProxy
def jobWithSingleInputDataIN2P3():

  job = baseToAllJobs( 'jobWithSingleInputDataIN2P3', jobClass )
  job.setInputSandbox( [find_all( 'exe-script-with-input-single-location.py', rootPath, '/LHCbDIRAC/tests/System/GridTestSubmission' )[0]] )
  job.setExecutable( "exe-script-with-input-single-location.py", "", "exeWithInput.log" )
  job.setInputData( '/lhcb/user/f/fstagni/test/testInputFileSingleLocation.txt' )  # this file should be at CERN-USER only
  job.setInputDataPolicy( 'protocol' )
  job.setDestination( ['LCG.IN2P3.fr'] )
  res = endOfAllJobs( job )
  return res

@executeWithUserProxy
def jobWithSingleInputDataRRCKI():

  job = baseToAllJobs( 'jobWithSingleInputDataRRCKI', jobClass )
  job.setInputSandbox( [find_all( 'exe-script-with-input-single-location.py', rootPath, '/LHCbDIRAC/tests/System/GridTestSubmission' )[0]] )
  job.setExecutable( "exe-script-with-input-single-location.py", "", "exeWithInput.log" )
  job.setInputData( '/lhcb/user/f/fstagni/test/testInputFileSingleLocation.txt' )  # this file should be at CERN-USER only
  job.setInputDataPolicy( 'protocol' )
  job.setDestination( ['LCG.RRCKI.ru'] )
  res = endOfAllJobs( job )
  return res

@executeWithUserProxy
def jobWithSingleInputDataSARA():

  job = baseToAllJobs( 'jobWithSingleInputDataSARA', jobClass )
  job.setInputSandbox( [find_all( 'exe-script-with-input-single-location.py', rootPath, '/LHCbDIRAC/tests/System/GridTestSubmission' )[0]] )
  job.setExecutable( "exe-script-with-input-single-location.py", "", "exeWithInput.log" )
  job.setInputData( '/lhcb/user/f/fstagni/test/testInputFileSingleLocation.txt' )  # this file should be at CERN-USER only
  job.setInputDataPolicy( 'protocol' )
  job.setDestination( ['LCG.SARA.nl'] )
  res = endOfAllJobs( job )
  return res

@executeWithUserProxy
def jobWithSingleInputDataNIKHEF():

  job = baseToAllJobs( 'jobWithSingleInputDataNIKHEF', jobClass )
  job.setInputSandbox( [find_all( 'exe-script-with-input-single-location.py', rootPath, '/LHCbDIRAC/tests/System/GridTestSubmission' )[0]] )
  job.setExecutable( "exe-script-with-input-single-location.py", "", "exeWithInput.log" )
  job.setInputData( '/lhcb/user/f/fstagni/test/testInputFileSingleLocation.txt' )  # this file should be at CERN-USER only
  job.setInputDataPolicy( 'protocol' )
  job.setDestination( ['LCG.NIKHEF.nl'] )
  res = endOfAllJobs( job )
  return res

@executeWithUserProxy
def jobWithSingleInputDataPIC():

  job = baseToAllJobs( 'jobWithSingleInputDataPIC', jobClass )
  job.setInputSandbox( [find_all( 'exe-script-with-input-single-location.py', rootPath, '/LHCbDIRAC/tests/System/GridTestSubmission' )[0]] )
  job.setExecutable( "exe-script-with-input-single-location.py", "", "exeWithInput.log" )
  job.setInputData( '/lhcb/user/f/fstagni/test/testInputFileSingleLocation.txt' )  # this file should be at CERN-USER only
  job.setInputDataPolicy( 'protocol' )
  job.setDestination( ['LCG.PIC.es'] )
  res = endOfAllJobs( job )
  return res

@executeWithUserProxy
def jobWithSingleInputDataGRIDKA():

  job = baseToAllJobs( 'jobWithSingleInputDataGRIDKA', jobClass )
  job.setInputSandbox( [find_all( 'exe-script-with-input-single-location.py', rootPath, '/LHCbDIRAC/tests/System/GridTestSubmission' )[0]] )
  job.setExecutable( "exe-script-with-input-single-location.py", "", "exeWithInput.log" )
  job.setInputData( '/lhcb/user/f/fstagni/test/testInputFileSingleLocation.txt' )  # this file should be at CERN-USER only
  job.setInputDataPolicy( 'protocol' )
  job.setDestination( ['LCG.GRIDKA.de'] )
  res = endOfAllJobs( job )
  return res


@executeWithUserProxy
def jobWithSingleInputDataSpreaded():

  job = baseToAllJobs( 'jobWithSingleInputDataSpreaded', jobClass )
  job.setInputSandbox( [find_all( 'exe-script-with-input.py', rootPath, '/LHCbDIRAC/tests/System/GridTestSubmission' )[0]] )
  job.setExecutable( "exe-script-with-input.py", "", "exeWithInput.log" )
  job.setInputData( '/lhcb/user/f/fstagni/test/testInputFile.txt' )  # this file should be at CERN-USER and IN2P3-USER
  job.setInputDataPolicy( 'download' )
  res = endOfAllJobs( job )
  return res

@executeWithUserProxy
def jobWithInputDataAndAncestor():

  job = baseToAllJobs( 'jobWithInputDataAndAncestor', jobClass )
  job.setInputSandbox( [find_all( 'exe-script-with-input-and-ancestor.py', rootPath, '/LHCbDIRAC/tests/System/GridTestSubmission' )[0]] )
  job.setExecutable( "exe-script-with-input-and-ancestor.py", "", "exeWithInput.log" )
  # WARNING: Collision10!!
  job.setInputData( '/lhcb/data/2010/SDST/00008375/0005/00008375_00053941_1.sdst' )  # this file should be at SARA-RDST
  # the ancestor should be /lhcb/data/2010/RAW/FULL/LHCb/COLLISION10/81616/081616_0000000213.raw (CERN and SARA)
  job.setAncestorDepth( 1 )  # pylint: disable=no-member
  job.setInputDataPolicy( 'download' )
  res = endOfAllJobs( job )
  return res

@executeWithUserProxy
def gaussJob():

  job = baseToAllJobs( 'gaussJob', jobClass )
  job.setInputSandbox( [find_all( 'prodConf_Gauss_00012345_00067890_1.py', rootPath, '/LHCbDIRAC/tests/System/GridTestSubmission' )[0]] )
  job.setOutputSandbox( '00012345_00067890_1.sim' )

  optGauss = "$APPCONFIGOPTS/Gauss/Sim08-Beam3500GeV-md100-2011-nu2.py;"
  optDec = "$DECFILESROOT/options/34112104.py;"
  optPythia = "$LBPYTHIAROOT/options/Pythia.py;"
  optOpts = "$APPCONFIGOPTS/Gauss/G4PL_FTFP_BERT_EmNoCuts.py;"
  optCompr = "$APPCONFIGOPTS/Persistency/Compression-ZLIB-1.py;"
  optPConf = "prodConf_Gauss_00012345_00067890_1.py"
  options = optGauss + optDec + optPythia + optOpts + optCompr + optPConf
  job.setApplication( 'Gauss', 'v45r5', options,  # pylint: disable=no-member
                      extraPackages = 'AppConfig.v3r179;DecFiles.v27r14p1;ProdConf.v1r9',
                      systemConfig = 'x86_64-slc5-gcc43-opt' )
  job.setDIRACPlatform()  # pylint: disable=no-member
  job.setCPUTime( 172800 )
  res = endOfAllJobs( job )
  return res


@executeWithUserProxy
def booleJob():

  job = baseToAllJobs( 'booleJob', jobClass )
  job.setInputSandbox( [find_all( 'prodConf_Boole_00012345_00067890_1.py', rootPath, '/LHCbDIRAC/tests/System/GridTestSubmission' )[0]] )
  job.setOutputSandbox( '00012345_00067890_1.digi' )

  opts = "$APPCONFIGOPTS/Boole/Default.py;"
  optDT = "$APPCONFIGOPTS/Boole/DataType-2011.py;"
  optTCK = "$APPCONFIGOPTS/Boole/Boole-SiG4EnergyDeposit.py;"
  optComp = "$APPCONFIGOPTS/Persistency/Compression-ZLIB-1.py;"
  optPConf = "prodConf_Boole_00012345_00067890_1.py"
  options = opts + optDT + optTCK + optComp + optPConf

  job.setApplication( 'Boole', 'v26r3', options,  # pylint: disable=no-member
                      inputData = '/lhcb/user/f/fstagni/test/12345/12345678/00012345_00067890_1.sim',
                      extraPackages = 'AppConfig.v3r171;ProdConf.v1r9',
                      systemConfig = 'x86_64-slc5-gcc43-opt' )

  job.setDIRACPlatform()  # pylint: disable=no-member
  job.setCPUTime( 172800 )
  res = endOfAllJobs( job )
  return res

@executeWithUserProxy
def booleJobWithConf():

  job = baseToAllJobs( 'booleJobWithConf', jobClass )
  job.setInputSandbox( [find_all( 'prodConf_Boole_00012345_00067890_1.py', rootPath, '/LHCbDIRAC/tests/System/GridTestSubmission' )[0],
                        find_all( 'rootConfig.cfg', rootPath, '/LHCbDIRAC/tests/System/GridTestSubmission' )[0] ] )
  job.setOutputSandbox( '00012345_00067890_1.digi' )

  opts = "$APPCONFIGOPTS/Boole/Default.py;"
  optDT = "$APPCONFIGOPTS/Boole/DataType-2011.py;"
  optTCK = "$APPCONFIGOPTS/Boole/Boole-SiG4EnergyDeposit.py;"
  optComp = "$APPCONFIGOPTS/Persistency/Compression-ZLIB-1.py;"
  optPConf = "prodConf_Boole_00012345_00067890_1.py"
  options = opts + optDT + optTCK + optComp + optPConf

  job.setApplication( 'Boole', 'v26r3', options,  # pylint: disable=no-member
                      inputData = '/lhcb/user/f/fstagni/test/12345/12345678/00012345_00067890_1.sim',
                      extraPackages = 'AppConfig.v3r171;ProdConf.v1r9',
                      systemConfig = 'x86_64-slc5-gcc43-opt' )


  job.setDIRACPlatform()  # pylint: disable=no-member
  job.setConfigArgs( 'rootConfig.cfg' )
  job.setCPUTime( 172800 )
  res = endOfAllJobs( job )
  return res

@executeWithUserProxy
def gaudiApplicationScriptJob():

  job = baseToAllJobs( 'gaudiApplicationScriptJob', jobClass )
  job.setInputSandbox( [find_all( '_input_sandbox_1324_master.tgz', wdir, 'GridTestSubmission' )[0]] \
                     + [find_all( 'runToys.C', wdir, 'GridTestSubmission' )[0]] \
                     + [find_all( 'script_wrapper.py', wdir, 'GridTestSubmission' )[0]] )
  job.setApplicationScript( 'root', '6.06.02', find_all( 'script_wrapper.py', wdir, 'GridTestSubmission' )[0], # pylint: disable=no-member
                            systemConfig = 'x86_64-slc6-gcc49-opt' )
  job.setOutputSandbox( 'FitResultsToyData*.root' )
  job.setDIRACPlatform()  # pylint: disable=no-member
  job.setCPUTime( 172800 )
  res = endOfAllJobs( job )
  return res

@executeWithUserProxy
def daVinciLHCbScriptJob_v41r2():
  """ This job internally will try to run using 'x86_64-slc6-gcc49-opt'

      DaVinci v41r2 is not on lb-run
  """

  job = baseToAllJobs( 'daVinciLHCbScriptJob_v41r2', jobClass )
  job.setInputSandbox( ['LFN:/lhcb/user/f/fstagni/GangaInputFile/LHCb-Scripts.tar.gz',
                        'LFN:/lhcb/user/f/fstagni/GangaInputFile/minimal_DaVinciDev_v41r2.tgz'] )
  job.setExecutable( 'jobScript/GaudiExec_Job_6_script.py', arguments = '', logFile = 'Ganga_GaudiExec.log',
                     systemConfig = 'x86_64-slc6-gcc49-opt' )
  job.setDIRACPlatform()  # pylint: disable=no-member
  job.setCPUTime( 172800 )
  res = endOfAllJobs( job )
  return res


@executeWithUserProxy
def daVinciLHCbScriptJob_v42r1():
  """ This job internally will try to run using 'x86_64-slc6-gcc49-opt' while here 'x86_64-slc6-gcc48-opt' is set

      DaVinci v42r1 uses lb-run only
  """

  job = baseToAllJobs( 'daVinciLHCbScriptJob_v42r1', jobClass )
  job.setInputSandbox( ['LFN:/lhcb/user/f/fstagni/GangaInputFile/LHCb-Scripts.tar.gz',
                        'LFN:/lhcb/user/f/fstagni/GangaInputFile/minimal_DaVinciDev_v42r1.tgz'] )
  job.setExecutable( 'jobScript/GaudiExec_Job_6_script.py', arguments = '', logFile = 'Ganga_GaudiExec.log',
                     systemConfig = 'x86_64-slc6-gcc48-opt' )
  job.setDIRACPlatform()  # pylint: disable=no-member
  job.setCPUTime( 172800 )
  res = endOfAllJobs( job )
  return res

@executeWithUserProxy
def wrongJob():

  print "\n Submitting gaudiRun job (Gauss only) that will use a configuration file that contains wrong info"
  print "This will generate a job that should become Completed, use the failover, and only later it will be Done"

  job = createJob( local = False )
  job.setName( "gaudirun-gauss-completed-than-done" )
  res = endOfAllJobs( job )
  return res




#
# #     print "**********************************************************************************************************"
# #
# #     gLogger.info( "\n Submitting gaudiRun job (Gauss only) that should use TAG to run on a multi-core queue" )
# #
# #     gaudirunJob = LHCbJob()
# #
# #     gaudirunJob.setName( "gaudirun-Gauss-test-TAG-multicore" )
# #     gaudirunJob.setInputSandbox( [find_all( 'prodConf_Gauss_00012345_00067890_1.py', rootPath, '/LHCbDIRAC/tests/System/GridTestSubmission' )[0]] )
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
# #     self.assertTrue(result['OK'])
#
