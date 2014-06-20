from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

import unittest, os

from DIRAC.Core.Utilities.Shifter import setupShifterProxyInEnv

from TestDIRAC.Utilities.utils import find_all

from LHCbTestDirac.Utilities.IntegrationTest import IntegrationTest
from LHCbTestDirac.Utilities.IntegrationTest import FailingUserJobTestCase

from LHCbDIRAC.Interfaces.API.LHCbJob import LHCbJob
from LHCbDIRAC.Interfaces.API.DiracLHCb import DiracLHCb

class UserJobTestCase( IntegrationTest ):
  """ Base class for the UserJob test cases
  """
  def setUp( self ):
    super( IntegrationTest, self ).setUp()

    self.dLHCb = DiracLHCb()
    self.exeScriptLocation = find_all( 'exe-script.py', '.', 'Integration' )[0]

  def tearDown( self ):
    pass

class HelloWorldSuccess( UserJobTestCase ):
  def test_execute( self ):

    lhcbJob = LHCbJob()

    lhcbJob.setName( "helloWorld-test" )
    lhcbJob.setExecutable( self.exeScriptLocation )
    res = lhcbJob.runLocal( self.dLHCb )
    self.assertTrue( res['OK'] )

class HelloWorldSuccessWithJobID( UserJobTestCase ):
  def test_execute( self ):

    os.environ['JOBID'] = '12345'

    lhcbJob = LHCbJob()

    lhcbJob.setName( "helloWorld-test" )
    lhcbJob.setExecutable( self.exeScriptLocation )
    res = lhcbJob.runLocal( self.dLHCb )
    self.assertTrue( res['OK'] )  # There's nothing to upload, so it will complete happily

    del os.environ['JOBID']

class HelloWorldSuccessOutput( UserJobTestCase ):
  def test_execute( self ):

    lhcbJob = LHCbJob()

    lhcbJob.setName( "helloWorld-test" )
    lhcbJob.setExecutable( self.exeScriptLocation )
    lhcbJob.setOutputData( "applicationLog.txt" )
    res = lhcbJob.runLocal( self.dLHCb )
    self.assertTrue( res['OK'] )

class HelloWorldSuccessOutputWithJobID( UserJobTestCase ):
  def test_execute( self ):

    os.environ['JOBID'] = '12345'

    lhcbJob = LHCbJob()

    lhcbJob.setName( "helloWorld-test" )
    lhcbJob.setExecutable( self.exeScriptLocation )
    lhcbJob.setOutputData( "applicationLog.txt" )
    res = lhcbJob.runLocal( self.dLHCb )  # Can't upload, so it will fail
    self.assertFalse( res['OK'] )

    del os.environ['JOBID']

class GaudirunSuccess( UserJobTestCase ):
  def test_mc( self ):

    lhcbJob = LHCbJob()

    lhcbJob.setName( "gaudirun-test" )
    lhcbJob.setInputSandbox( find_all( 'prodConf_Gauss_00012345_00067890_1.py', '.', 'Integration' )[0] )

    optGauss = "$APPCONFIGOPTS/Gauss/Sim08-Beam4000GeV-mu100-2012-nu2.5.py;"
    optDec = "$DECFILESROOT/options/11102400.py;"
    optPythia = "$LBPYTHIA8ROOT/options/Pythia8.py;"
    optOpts = " $APPCONFIGOPTS/Gauss/G4PL_FTFP_BERT_EmNoCuts.py;"
    optCompr = "$APPCONFIGOPTS/Persistency/Compression-ZLIB-1.py;"
    optPConf = "prodConf_Gauss_00012345_00067890_1.py"
    options = optGauss + optDec + optPythia + optOpts + optCompr + optPConf

    lhcbJob.setApplication( 'Gauss', 'v45r3', options,
                            extraPackages = 'AppConfig.v3r171;ProdConf.v1r9',
                            events = '3' )
    lhcbJob.setDIRACPlatform()

    res = lhcbJob.runLocal( self.dLHCb )
    self.assertTrue( res['OK'] )

  def test_boole( self ):

    # get a shifter proxy
    setupShifterProxyInEnv( 'ProductionManager' )

    lhcbJob = LHCbJob()

    lhcbJob.setName( "gaudirun-test-inputs" )
    lhcbJob.setInputSandbox( find_all( 'prodConf_Boole_00012345_00067890_1.py', '.', 'Integration' )[0] )

    opts = "$APPCONFIGOPTS/Boole/Default.py;"
    optDT = "$APPCONFIGOPTS/Boole/DataType-2012.py;"
    optTCK = "$APPCONFIGOPTS/L0/L0TCK-0x0042.py;"
    optComp = "$APPCONFIGOPTS/Persistency/Compression-ZLIB-1.py;"
    optPConf = "prodConf_Boole_00012345_00067890_1.py"
    options = opts + optDT + optTCK + optComp + optPConf

    lhcbJob.setApplication( 'Boole', 'v24r0', options,
                            inputData = '/lhcb/user/f/fstagni/test/12345/12345678/00012345_00067890_1.sim',
                            extraPackages = 'AppConfig.v3r155;ProdConf.v1r9' )

    lhcbJob.setDIRACPlatform()
    res = lhcbJob.runLocal( self.dLHCb )
    self.assertTrue( res['OK'] )

class GaudiScriptSuccess( UserJobTestCase ):
  # FIXME: this, doens't work!
  def test_execute( self ):

    lhcbJob = LHCbJob()

    lhcbJob.setName( "gaudiScript-test" )
    script = find_all( 'gaudi-script.py', '.', 'Integration' )[0]
    pConfFile = find_all( 'prodConf_Gauss_00012345_00067890_1.py', '.', 'Integration' )[0]
    lhcbJob.setInputSandbox( [pConfFile, script] )

    lhcbJob.setApplicationScript( 'Gauss', 'v45r3', script,
                                  extraPackages = 'AppConfig.v3r171;ProdConf.v1r9' )

    lhcbJob.setDIRACPlatform()
    res = lhcbJob.runLocal( self.dLHCb )
    self.assertTrue( res['OK'] )

########################################################################################################################

class UserJobsFailingLocalSuccess( FailingUserJobTestCase ):

  def test_run( self ):
    print "Submitting gaudiRun job (Gauss only) that will use a configuration file that contains wrong info"
    print "This will generate a local job"
    os.environ['JOBID'] = '12345'

    gaudirunJob = createJob()
    result = DiracLHCb().submit( gaudirunJob, mode = 'Local' )
    self.assertTrue( result['OK'] )

    del os.environ['JOBID']

def createJob():

  gaudirunJob = LHCbJob()

  gaudirunJob.setName( "gaudirun-Gauss-test" )
  gaudirunJob.setInputSandbox( [find_all( 'prodConf_Gauss_00012345_00067890_1.py', '..', 'GridTestSubmission' )[0],
                                find_all( 'wrongConfig.cfg', '..', 'GridTestSubmission' )[0]] )
  gaudirunJob.setOutputSandbox( '00012345_00067890_1.sim' )

  optGauss = "$APPCONFIGOPTS/Gauss/Sim08-Beam3500GeV-md100-2011-nu2.py;"
  optDec = "$DECFILESROOT/options/34112104.py;"
  optPythia = "$LBPYTHIAROOT/options/Pythia.py;"
  optOpts = "$APPCONFIGOPTS/Gauss/G4PL_FTFP_BERT_EmNoCuts.py;"
  optCompr = "$APPCONFIGOPTS/Persistency/Compression-ZLIB-1.py;"
  optPConf = "prodConf_Gauss_00012345_00067890_1.py"
  options = optGauss + optDec + optPythia + optOpts + optCompr + optPConf

  gaudirunJob.setApplication( 'Gauss', 'v45r5', options, extraPackages = 'AppConfig.v3r171;DecFiles.v27r14p1;ProdConf.v1r9',
                              systemConfig = 'x86_64-slc5-gcc43-opt',
                              modulesNameList = ['CreateDataFile',
                                                  'GaudiApplication',
                                                  'FileUsage',
                                                  'UploadOutputData',
                                                  'UploadLogFile',
                                                  'FailoverRequest',
                                                  'UserJobFinalization'],
                             parametersList = [( 'applicationName', 'string', '', 'Application Name' ),
                                               ( 'applicationVersion', 'string', '', 'Application Version' ),
                                               ( 'applicationLog', 'string', '', 'Name of the output file of the application' ),
                                               ( 'optionsFile', 'string', '', 'Options File' ),
                                               ( 'extraOptionsLine', 'string', '', 'This is appended to standard options' ),
                                               ( 'inputDataType', 'string', '', 'Input Data Type' ),
                                               ( 'inputData', 'string', '', 'Input Data' ),
                                               ( 'numberOfEvents', 'string', '', 'Events treated' ),
                                               ( 'extraPackages', 'string', '', 'ExtraPackages' ),
                                               ( 'listoutput', 'list', [], 'StepOutputList' ),
                                               ( 'SystemConfig', 'string', '', 'CMT Config' ),
                                               ]
 )

  gaudirunJob._addParameter( gaudirunJob.workflow, 'PRODUCTION_ID', 'string', '00012345', 'ProductionID' )
  gaudirunJob._addParameter( gaudirunJob.workflow, 'JOB_ID', 'string', '00067890', 'JobID' )
  gaudirunJob._addParameter( gaudirunJob.workflow, 'configName', 'string', 'testCfg', 'ConfigName' )
  gaudirunJob._addParameter( gaudirunJob.workflow, 'configVersion', 'string', 'testVer', 'ConfigVersion' )
  outputList = [{'stepName': 'GaussStep1', 'outputDataType': 'sim', 'outputBKType': 'SIM',
                 'outputDataSE': 'Tier1_MC_M-DST',
                 'outputDataName': '00012345_00067890_1.sim'}]
  gaudirunJob._addParameter( gaudirunJob.workflow, 'outputList', 'list', outputList, 'outputList' )
  gaudirunJob._addParameter( gaudirunJob.workflow, 'outputDataFileMask', 'string', '', 'outputFM' )
  gaudirunJob._addParameter( gaudirunJob.workflow, 'outputMode', 'string', 'Local', 'OM' )
  gaudirunJob._addParameter( gaudirunJob.workflow, 'LogLevel', 'string', 'DEBUG', 'LL' )
  outputFilesDict = [{'outputDataName': '00012345_00067890_1.sim',
                      'outputDataSE': 'Tier1_MC-DST',
                      'outputDataType': 'SIM'}]
  gaudirunJob._addParameter( gaudirunJob.workflow.step_instances[0], 'listoutput', 'list', outputFilesDict, 'listoutput' )

  gaudirunJob.setDIRACPlatform()
  gaudirunJob.setConfigArgs( 'wrongConfig.cfg' )

  gaudirunJob.setCPUTime( 172800 )

  return gaudirunJob


if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( UserJobTestCase )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( HelloWorldSuccess ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( HelloWorldSuccessWithJobID ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( HelloWorldSuccessOutput ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( HelloWorldSuccessOutputWithJobID ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( GaudirunSuccess ) )
#  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( GaudiScriptSuccess ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )

  suiteFailures = unittest.defaultTestLoader.loadTestsFromTestCase( FailingUserJobTestCase )
  suiteFailures.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( UserJobsFailingLocalSuccess ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suiteFailures )
