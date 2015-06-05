""" This submits user jobs and then starts a thread that checks their results
"""

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

import unittest
import time

from DIRAC import gLogger
from DIRAC.Core.Security.ProxyInfo import getProxyInfo
from DIRAC.ResourceStatusSystem.Client.ResourceStatus import ResourceStatus
from DIRAC.DataManagementSystem.Client.DataManager import DataManager


from TestDIRAC.System.unitTestUserJobs import GridSubmissionTestCase as DIRACGridSubmissionTestCase
from TestDIRAC.Utilities.utils import find_all

from LHCbDIRAC.Interfaces.API.LHCbJob import LHCbJob
from LHCbDIRAC.Interfaces.API.DiracLHCb import DiracLHCb
from LHCbTestDirac.Integration.Workflow.Test_UserJobs import createJob
from LHCbTestDirac.Utilities.testJobDefinitions import *

gLogger.setLevel( 'VERBOSE' )

jobsSubmittedList = []

class GridSubmissionTestCase( unittest.TestCase ):
  """ Base class for the Regression test cases
  """
  def setUp( self ):
    self.dirac = DiracLHCb()

    result = getProxyInfo()
    if result['Value']['group'] not in ['lhcb_user', 'dirac_user']:
      print "GET A USER GROUP"
      exit( 1 )
      
    result = ResourceStatus().getStorageElementStatus( 'PIC-USER', 'WriteAccess' )
    if result['Value']['PIC-USER']['WriteAccess'].lower() != 'banned':
      print "BAN PIC-USER in writing! and then restart this test"
      exit( 1 )

    res = DataManager().getReplicas( ['/lhcb/user/f/fstagni/test/testInputFileSingleLocation.txt',
                                      '/lhcb/user/f/fstagni/test/testInputFile.txt'] )
    if not res['OK']:
      print "DATAMANAGER.getRepicas failure: %s" % res['Message']
      exit( 1 )
    if res['Value']['Failed']:
      print "DATAMANAGER.getRepicas failed for someting: %s" % res['Value']['Failed']
      exit( 1 )

    replicas = res['Value']['Successful']
    if replicas['/lhcb/user/f/fstagni/test/testInputFile.txt'].keys() != ['CERN-USER', 'IN2P3-USER']:
      print "/lhcb/user/f/fstagni/test/testInputFile.txt locations are not correct"
    if replicas['/lhcb/user/f/fstagni/test/testInputFileSingleLocation.txt'].keys() != ['CERN-USER']:
      print "/lhcb/user/f/fstagni/test/testInputFileSingleLocation.txt locations are not correct"

  def tearDown( self ):
    pass

class LHCbsubmitSuccess( GridSubmissionTestCase, DIRACGridSubmissionTestCase ):

  def test_LHCbsubmit( self ):
    
    res = helloWorldTestT2s()
    self.assert_( res['OK'] )
    jobsSubmittedList.append( res['Value'] )

    res = helloWorldTestCERN()
    self.assert_( res['OK'] )
    jobsSubmittedList.append( res['Value'] )

    res = helloWorldTestSLC6()
    self.assert_( res['OK'] )
    jobsSubmittedList.append( res['Value'] )

    res = helloWorldTestSLC5()
    self.assert_( res['OK'] )
    jobsSubmittedList.append( res['Value'] )

    res = jobWithOuput()
    self.assert_( res['OK'] )
    jobsSubmittedList.append( res['Value'] )

    res = jobWithOutputAndPrepend()
    self.assert_( res['OK'] )
    jobsSubmittedList.append( res['Value'] )

    res = jobWithOutputAndPrependWithUnderscore()
    self.assert_( res['OK'] )
    jobsSubmittedList.append( res['Value'] )



#     print "**********************************************************************************************************"
#
#     gLogger.info( "\n Submitting a job that uploads an output and replicates it" )
#
#     helloJ = LHCbJob()
#
#     helloJ.setName( "upload-Output-test-with-replication" )
#     helloJ.setInputSandbox( [find_all( 'testFileReplication.txt', '.', 'GridTestSubmission' )[0]] + \
#                             [find_all( 'exe-script.py', '.', 'GridTestSubmission' )[0]] )
#     helloJ.setExecutable( "exe-script.py", "", "helloWorld.log" )
#
#     helloJ.setCPUTime( 17800 )
#
#     helloJ.setOutputData( ['testFileReplication.txt'], replicate = 'True' )
#
#     result = self.dirac.submit( helloJ )
#     gLogger.info( "Hello world with output and replication: ", result )
#
#     jobID = int( result['Value'] )
#     jobsSubmittedList.append( jobID )
#
#     self.assert_( result['OK'] )
#
#     print "**********************************************************************************************************"
#
#     gLogger.info( "\n Submitting a job that uploads 2 outputs to an SE that is banned (PIC-USER)" )
#
#     helloJ = LHCbJob()
#
#     helloJ.setName( "upload-2-Output-to-banned-SE" )
#     helloJ.setInputSandbox( [find_all( 'testFileUploadBanned-1.txt', '.', 'GridTestSubmission' )[0]] \
#                             + [find_all( 'testFileUploadBanned-2.txt', '.', 'GridTestSubmission' )[0]] \
#                             + [find_all( 'exe-script.py', '.', 'GridTestSubmission' )[0]] \
#                             + [find_all( 'partialConfig.cfg', '.', 'GridTestSubmission' )[0] ] )
#     helloJ.setExecutable( "exe-script.py", "", "helloWorld.log" )
#
#     helloJ.setCPUTime( 17800 )
#     helloJ.setConfigArgs( 'partialConfig.cfg' )
#
#     helloJ.setDestination( 'LCG.PIC.es' )
#     helloJ.setOutputData( ['testFileUploadBanned-1.txt', 'testFileUploadBanned-2.txt'], OutputSE = ['PIC-USER'] )
#
#     result = self.dirac.submit( helloJ )
#     gLogger.info( "Hello world submitting a job that uploads 2 outputs to an SE that is banned: ", result )
#
#     jobID = int( result['Value'] )
#     jobsSubmittedList.append( jobID )
#
#     self.assert_( result['OK'] )
#
#     print "**********************************************************************************************************"
#
#     gLogger.info( "\n Submitting a job that has an input data - use download policy" )
#
#     inputJ = LHCbJob()
#
#     inputJ.setName( "job-with-input-data" )
#     inputJ.setInputSandbox( [find_all( 'exe-script-with-input.py', '.', 'GridTestSubmission' )[0]] )
#     inputJ.setExecutable( "exe-script-with-input.py", "", "exeWithInput.log" )
#     inputJ.setInputData( '/lhcb/user/f/fstagni/test/testInputFileSingleLocation.txt' )  # this file should be at CERN-USER only
#     inputJ.setInputDataPolicy( 'download' )
#
#     inputJ.setCPUTime( 17800 )
#
#     result = self.dirac.submit( inputJ )
#     gLogger.info( "Hello world with with input: ", result )
#
#     jobID = int( result['Value'] )
#     jobsSubmittedList.append( jobID )
#
#     self.assert_( result['OK'] )
#
#
#     print "**********************************************************************************************************"
#
#     gLogger.info( "\n Submitting a job that has an input data in more than one location - use download policy" )
#
#     inputJ = LHCbJob()
#
#     inputJ.setName( "job-with-input-data" )
#     inputJ.setInputSandbox( [find_all( 'exe-script-with-input.py', '.', 'GridTestSubmission' )[0]] )
#     inputJ.setExecutable( "exe-script-with-input.py", "", "exeWithInput.log" )
#     inputJ.setInputData( '/lhcb/user/f/fstagni/test/testInputFile.txt' )  # this file should be at CERN-USER and IN2P3-USER
#     inputJ.setInputDataPolicy( 'download' )
#
#     inputJ.setCPUTime( 17800 )
#
#     result = self.dirac.submit( inputJ )
#     gLogger.info( "Hello world with input at multiple locations: ", result )
#
#     jobID = int( result['Value'] )
#     jobsSubmittedList.append( jobID )
#
#     self.assert_( result['OK'] )
#
#
#     print "**********************************************************************************************************"
#
#     gLogger.info( "\n Submitting gaudiRun job (Gauss only)" )
#
#     gaudirunJob = LHCbJob()
#
#     gaudirunJob.setName( "gaudirun-Gauss-test" )
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
#     gaudirunJob.setApplication( 'Gauss', 'v45r5', options,
#                                 extraPackages = 'AppConfig.v3r179;DecFiles.v27r14p1;ProdConf.v1r9',
#                                 systemConfig = 'x86_64-slc5-gcc43-opt' )
#
#     gaudirunJob.setDIRACPlatform()
#     gaudirunJob.setCPUTime( 172800 )
#
#     result = self.dirac.submit( gaudirunJob )
#     gLogger.info( 'Submission Result: ', result )
#
#     jobID = int( result['Value'] )
#     jobsSubmittedList.append( jobID )
#
#     self.assert_( result['OK'] )
#
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
#
#     print "**********************************************************************************************************"
#
#     gLogger.info( "\n Submitting gaudiRun job (Boole only)" )
#
#     gaudirunJob = LHCbJob()
#
#     gaudirunJob.setName( "gaudirun-Boole-test" )
#     gaudirunJob.setInputSandbox( [find_all( 'prodConf_Boole_00012345_00067890_1.py', '.', 'GridTestSubmission' )[0]] )
#     gaudirunJob.setOutputSandbox( '00012345_00067890_1.digi' )
#
#     opts = "$APPCONFIGOPTS/Boole/Default.py;"
#     optDT = "$APPCONFIGOPTS/Boole/DataType-2011.py;"
#     optTCK = "$APPCONFIGOPTS/Boole/Boole-SiG4EnergyDeposit.py;"
#     optComp = "$APPCONFIGOPTS/Persistency/Compression-ZLIB-1.py;"
#     optPConf = "prodConf_Boole_00012345_00067890_1.py"
#     options = opts + optDT + optTCK + optComp + optPConf
#
#     gaudirunJob.setApplication( 'Boole', 'v26r3', options,
#                                 inputData = '/lhcb/user/f/fstagni/test/12345/12345678/00012345_00067890_1.sim',
#                                 extraPackages = 'AppConfig.v3r171;ProdConf.v1r9',
#                                 systemConfig = 'x86_64-slc5-gcc43-opt' )
#
#     gaudirunJob.setDIRACPlatform()
#     gaudirunJob.setCPUTime( 172800 )
#
#     result = self.dirac.submit( gaudirunJob )
#     gLogger.info( 'Submission Result: ', result )
#
#     jobID = int( result['Value'] )
#     jobsSubmittedList.append( jobID )
#
#     self.assert_( result['OK'] )
#
#
#     print "**********************************************************************************************************"
#
#     gLogger.info( "\n Submitting gaudiRun job (Gauss only) that will use a configuration file that contains wrong info" )
#     gLogger.info( "This will generate a job that should become Completed, use the failover, and only later it will be Done" )
#
#     gaudirunJob = createJob( local = False )
#     gaudirunJob.setName( "gaudirun-gauss-completed-than-done" )
#     result = self.dirac.submit( gaudirunJob )
#     gLogger.info( 'Submission Result: ', result )
#
#     jobID = int( result['Value'] )
#     jobsSubmittedList.append( jobID )
#
#     self.assert_( result['OK'] )
#
#     jobID = int( result['Value'] )
#     jobsSubmittedList.append( jobID )
#
#     self.assert_( result['OK'] )

########################################################################################
#
# class monitorSuccess( GridSubmissionTestCase ):
#
#   def test_monitor( self ):
#
#     toRemove = []
#     fail = False
#
#     # we will check every 10 minutes, up to 6 hours
#     counter = 0
#     while counter < 36:
#       jobStatus = self.dirac.status( jobsSubmittedList )
#       self.assert_( jobStatus['OK'] )
#       for jobID in jobsSubmittedList:
#         status = jobStatus['Value'][jobID]['Status']
#         minorStatus = jobStatus['Value'][jobID]['MinorStatus']
#         if status == 'Done':
#           self.assert_( minorStatus in ['Execution Complete', 'Requests Done'] )
#           jobsSubmittedList.remove( jobID )
#           res = self.dirac.getJobOutputLFNs( jobID )
#           if res['OK']:
#             lfns = res['Value']
#             toRemove += lfns
#         if status in ['Failed', 'Killed', 'Deleted']:
#           fail = True
#           jobsSubmittedList.remove( jobID )
#       if jobsSubmittedList:
#         time.sleep( 600 )
#         counter = counter + 1
#       else:
#         break
#
#     # removing produced files
#     res = self.dirac.removeFile( toRemove )
#     self.assert_( res['OK'] )
#
#     if fail:
#       self.assertFalse( True )

#############################################################################
# Test Suite run
#############################################################################

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( GridSubmissionTestCase )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( LHCbsubmitSuccess ) )
#   suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( monitorSuccess ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
