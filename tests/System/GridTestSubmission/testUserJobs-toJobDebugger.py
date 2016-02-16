from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

import unittest
import time

from DIRAC import gLogger
from DIRAC.Core.Security.ProxyInfo import getProxyInfo

from DIRAC.tests.System.unitTestUserJobs import GridSubmissionTestCase as DIRACGridSubmissionTestCase
from DIRAC.tests.Utilities.utils import find_all

from LHCbDIRAC.Interfaces.API.LHCbJob import LHCbJob
from LHCbDIRAC.Interfaces.API.DiracLHCb import DiracLHCb
from LHCbDIRAC.tests.Integration.Workflow.Test_UserJobs import createJob

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

  def tearDown( self ):
    pass

class LHCbsubmitSuccess( GridSubmissionTestCase, DIRACGridSubmissionTestCase ):

  def test_LHCbsubmit( self ):

    print "**********************************************************************************************************"

    gLogger.info( "\n Submitting hello world job targeting LCG.CERN.ch" )

    helloJ = LHCbJob()

    helloJ.setName( "helloWorld-test-CERN" )
    helloJ.setInputSandbox( [find_all( 'exe-script.py', '.', 'GridTestSubmission' )[0]] )
    helloJ.setExecutable( "exe-script.py", "", "helloWorld.log" )

    helloJ.setCPUTime( 17800 )
    helloJ.setDestination( 'DIRAC.JobDebugger.ch' )
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

    gLogger.info( "\n Submitting a job that uploads an output" )

    helloJ = LHCbJob()

    helloJ.setName( "upload-Output-test" )
    helloJ.setInputSandbox( [find_all( 'testFileUpload.txt', '.', 'GridTestSubmission' )[0]] + [find_all( 'exe-script.py', '.', 'GridTestSubmission' )[0]] )
    helloJ.setExecutable( "exe-script.py", "", "helloWorld.log" )

    helloJ.setCPUTime( 17800 )
    helloJ.setDestination( 'DIRAC.JobDebugger.ch' )

    helloJ.setOutputData( ['testFileUpload.txt'] )

    result = self.dirac.submit( helloJ )
    gLogger.info( "Hello world with output: ", result )

    jobID = int( result['Value'] )
    jobsSubmittedList.append( jobID )

    self.assert_( result['OK'] )


#############################################################################

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( GridSubmissionTestCase )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( LHCbsubmitSuccess ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
