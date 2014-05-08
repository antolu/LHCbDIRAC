from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

import unittest
import time

import os.path

from DIRAC import gLogger
from LHCbDIRAC.Interfaces.API.LHCbJob import LHCbJob
from LHCbDIRAC.Interfaces.API.DiracLHCb import DiracLHCb
from LHCbTestDirac.Utilities.utils import find_all
from LHCbTestDirac.Integration.Test_UserJobs import createJob

gLogger.setLevel( 'VERBOSE' )

cwd = os.path.realpath( '.' )

jobsSubmittedList = []

class GridSubmissionTestCase( unittest.TestCase ):
  """ Base class for the Regression test cases
  """
  def setUp( self ):
    self.dirac = DiracLHCb()

  def tearDown( self ):
    pass

class submitSuccess( GridSubmissionTestCase ):

  def test_submit( self ):


    print "**********************************************************************************************************"

    gLogger.info( "\n Submitting hello world job banning T1s" )

    helloJ = LHCbJob()

    helloJ.setName( "helloWorld-test-T2s" )
    helloJ.setInputSandbox( [find_all( 'exe-script.py', '.', 'GridTestSubmission' )[0]] )

    helloJ.setExecutable( "exe-script.py", "", "helloWorld.log" )

    helloJ.setCPUTime( 17800 )
    tier1s = ['LCG.CERN.ch', 'LCG.CNAF.it', 'LCG.GRIDKA.de', 'LCG.IN2P3.fr', 'LCG.NIKHEF.nl', 'LCG.PIC.es', 'LCG.RAL.uk', 'LCG.SARA.nl']
    helloJ.setBannedSites( tier1s )
    result = self.dirac.submit( helloJ )
    gLogger.info( "Hello world job: ", result )

    jobID = int( result['Value'] )
    jobsSubmittedList.append( jobID )

    self.assert_( result['OK'] )


    print "**********************************************************************************************************"

    gLogger.info( "\n Submitting hello world job targeting LCG.CERN.ch" )

    helloJ = LHCbJob()

    helloJ.setName( "helloWorld-test-CERN" )
    helloJ.setInputSandbox( [find_all( 'exe-script.py', '.', 'GridTestSubmission' )[0]] )
    helloJ.setExecutable( "exe-script.py", "", "helloWorld.log" )

    helloJ.setCPUTime( 17800 )
    helloJ.setDestination( 'LCG.CERN.ch' )
    result = self.dirac.submit( helloJ )
    gLogger.info( "Hello world job: ", result )
    jobID = int( result['Value'] )
    jobsSubmittedList.append( jobID )

    self.assert_( result['OK'] )


    print "**********************************************************************************************************"

    gLogger.info( "\n Submitting hello world job targeting slc6 machines" )

    helloJ = LHCbJob()

    helloJ.setName( "helloWorld-test-slc6" )
    helloJ.setInputSandbox( [find_all( 'exe-script.py', '.', 'GridTestSubmission' )[0]] )
    helloJ.setExecutable( "exe-script.py", "", "helloWorld.log" )

    helloJ.setCPUTime( 17800 )
    helloJ.setPlatform( 'x86_64-slc6' )
    result = self.dirac.submit( helloJ )
    gLogger.info( "Hello world job: ", result )

    jobID = int( result['Value'] )
    jobsSubmittedList.append( jobID )

    self.assert_( result['OK'] )


    print "**********************************************************************************************************"

    gLogger.info( "\n Submitting hello world job targeting slc5 machines" )

    helloJ = LHCbJob()

    helloJ.setName( "helloWorld-test-slc5" )
    helloJ.setInputSandbox( [find_all( 'exe-script.py', '.', 'GridTestSubmission' )[0]] )
    helloJ.setExecutable( "exe-script.py", "", "helloWorld.log" )

    helloJ.setCPUTime( 17800 )
    helloJ.setPlatform( 'x86_64-slc5' )
    result = self.dirac.submit( helloJ )
    gLogger.info( "Hello world job: ", result )

    jobID = int( result['Value'] )
    jobsSubmittedList.append( jobID )

    self.assert_( result['OK'] )


    print "**********************************************************************************************************"

    gLogger.info( "\n Submitting a job that uploads an output" )

    helloJ = LHCbJob()

    fileUploaded = 'testFileUpload.txt'

    helloJ.setName( "upload-Output-test" )
    helloJ.setInputSandbox( [find_all( fileUploaded, '.', 'GridTestSubmission' )[0]] )
    helloJ.setExecutable( "exe-script.py", "", "helloWorld.log" )

    helloJ.setCPUTime( 17800 )

    helloJ.setOutputData( ['testFileUpload.txt'] )

    result = self.dirac.submit( helloJ )
    gLogger.info( "Hello world with output: ", result )

    jobID = int( result['Value'] )
    jobsSubmittedList.append( jobID )

    self.assert_( result['OK'] )


    print "**********************************************************************************************************"

    gLogger.info( "\n Submitting a job that uploads an output and replicates it" )

    helloJ = LHCbJob()

    helloJ.setName( "upload-Output-test-with-replication" )
    helloJ.setInputSandbox( [find_all( 'testFileReplication.txt', '.', 'GridTestSubmission' )[0]] )
    helloJ.setExecutable( "exe-script.py", "", "helloWorld.log" )

    helloJ.setCPUTime( 17800 )

    helloJ.setOutputData( ['testFileReplication.txt'], replicate = 'True' )

    result = self.dirac.submit( helloJ )
    gLogger.info( "Hello world with output and replication: ", result )

    jobID = int( result['Value'] )
    jobsSubmittedList.append( jobID )

    self.assert_( result['OK'] )


    print "**********************************************************************************************************"

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
    gaudirunJob.addPackage( 'AppConfig', 'v3r179' )
    gaudirunJob.addPackage( 'DecFiles', 'v27r14p1' )
    gaudirunJob.addPackage( 'ProdConf', 'v1r9' )
    gaudirunJob.setApplication( 'Gauss', 'v45r5', options,
                                extraPackages = 'AppConfig.v3r179;DecFiles.v27r14p1;ProdConf.v1r9',
                                systemConfig = 'x86_64-slc5-gcc43-opt' )

    gaudirunJob.setDIRACPlatform()
    gaudirunJob.setCPUTime( 172800 )

    result = self.dirac.submit( gaudirunJob )
    gLogger.info( 'Submission Result: ', result )

    jobID = int( result['Value'] )
    jobsSubmittedList.append( jobID )

    self.assert_( result['OK'] )


#     print "**********************************************************************************************************"
#
#     gLogger.info( "\n Submitting gaudiRun job (Gauss only) that should use TAG to run on a multi-core queue" )
#
#     gaudirunJob = LHCbJob()
#
#     gaudirunJob.setName( "gaudirun-Gauss-test-TAG-multicore" )
#     gaudirunJob.setInputSandbox( [find_all( 'prodConf_Gauss_00012345_00067890_1.py', '.', 'GridTestSubmission' )[0]] )
#     gaudirunJob.setOutputSandbox( '00012345_00067890_1.sim' )
#
#     optGauss = "$APPCONFIGOPTS/Gauss/Sim08-Beam3500GeV-md100-2011-nu2.py;"
#     optDec = "$DECFILESROOT/options/34112104.py;"
#     optPythia = "$LBPYTHIAROOT/options/Pythia.py;"
#     optOpts = "$APPCONFIGOPTS/Gauss/G4PL_FTFP_BERT_EmNoCuts.py;"
#     optCompr = "$APPCONFIGOPTS/Persistency/Compression-ZLIB-1.py;"
#     optPConf = "prodConf_Gauss_00012345_00067890_1.py"
#     options = optGauss + optDec + optPythia + optOpts + optCompr + optPConf
#     gaudirunJob.addPackage( 'AppConfig', 'v3r179' )
#     gaudirunJob.addPackage( 'DecFiles', 'v27r14p1' )
#     gaudirunJob.addPackage( 'ProdConf', 'v1r9' )
#     gaudirunJob.setApplication( 'Gauss', 'v45r5', options,
#                                 extraPackages = 'AppConfig.v3r179;DecFiles.v27r14p1;ProdConf.v1r9',
#                                 systemConfig = 'x86_64-slc5-gcc43-opt' )
#
#     gaudirunJob.setDIRACPlatform()
#     gaudirunJob.setCPUTime( 172800 )
#     gaudirunJob.setTag( ['MultiCore'] )
#
#     result = self.dirac.submit( gaudirunJob )
#     gLogger.info( 'Submission Result: ', result )
#
#     jobID = int( result['Value'] )
#     jobsSubmittedList.append( jobID )
#
#     self.assert_( result['OK'] )


    print "**********************************************************************************************************"

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

    gaudirunJob.addPackage( 'AppConfig', 'v3r171' )
    gaudirunJob.setApplication( 'Boole', 'v26r3', options,
                                inputData = '/lhcb/user/f/fstagni/test/12345/12345678/00012345_00067890_1.sim',
                                extraPackages = 'AppConfig.v3r171;ProdConf.v1r9',
                                systemConfig = 'x86_64-slc5-gcc43-opt' )

    gaudirunJob.setDIRACPlatform()
    gaudirunJob.setCPUTime( 172800 )

    result = self.dirac.submit( gaudirunJob )
    gLogger.info( 'Submission Result: ', result )

    jobID = int( result['Value'] )
    jobsSubmittedList.append( jobID )

    self.assert_( result['OK'] )


    print "**********************************************************************************************************"

    gLogger.info( "\n Submitting gaudiRun job (Gauss only) that will use a configuration file that contains wrong info" )
    gLogger.info( "This will generate a job that should become Completed, use the failover, and only later it will be Done" )

    gaudirunJob = createJob()
    result = self.dirac.submit( gaudirunJob )
    gLogger.info( 'Submission Result: ', result )

    jobID = int( result['Value'] )
    jobsSubmittedList.append( jobID )

    self.assert_( result['OK'] )

    jobID = int( result['Value'] )
    jobsSubmittedList.append( jobID )

    self.assert_( result['OK'] )

########################################################################################

class monitorSuccess( GridSubmissionTestCase ):

  def test_monitor( self ):

    toRemove = []

    # we will check every 10 minutes, up to 6 hours
    counter = 0
    while counter < 36:
      jobStatus = self.dirac.status( jobsSubmittedList )
      self.assert_( jobStatus['OK'] )
      for jobID in jobsSubmittedList:
        status = jobStatus['Value'][jobID]['Status']
        minorStatus = jobStatus['Value'][jobID]['MinorStatus']
        if status == 'Done':
          self.assert_( minorStatus in ['Execution Complete', 'Requests Done'] )
          jobsSubmittedList.remove( jobID )
          res = self.dirac.getJobOutputLFNs( jobID )
          if res['OK']:
            lfns = res['Value']
            toRemove += lfns
      if jobsSubmittedList:
        time.sleep( 600 )
        counter = counter + 1
      else:
        break

    res = self.dirac.removeFile( toRemove )
    self.assert_( res['OK'] )


#############################################################################
# Test Suite run
#############################################################################

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( GridSubmissionTestCase )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( submitSuccess ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( monitorSuccess ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
