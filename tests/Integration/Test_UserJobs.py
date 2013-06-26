from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

import unittest, os

from DIRAC.Core.Utilities.Shifter import setupShifterProxyInEnv

from LHCbTestDirac.Utilities.IntegrationTest import IntegrationTest
from LHCbTestDirac.Utilities.utils import find_all

from LHCbDIRAC.Interfaces.API.LHCbJob import LHCbJob
from LHCbDIRAC.Interfaces.API.DiracLHCb import DiracLHCb

class UserJobTestCase( IntegrationTest ):
  ''' Base class for the UserJob test cases
  '''
  def setUp( self ):
    super( IntegrationTest, self ).setUp()

    self.dLHCb = DiracLHCb()
    self.exeScriptLocation = find_all( 'exe-script.py', '.', 'Integration' )[0]

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
    lhcbJob.setOutputData( "Script1_exe-script.py.log" )
    res = lhcbJob.runLocal( self.dLHCb )
    self.assertTrue( res['OK'] )

class HelloWorldSuccessOutputWithJobID( UserJobTestCase ):
  def test_execute( self ):

    os.environ['JOBID'] = '12345'

    lhcbJob = LHCbJob()

    lhcbJob.setName( "helloWorld-test" )
    lhcbJob.setExecutable( self.exeScriptLocation )
    lhcbJob.setOutputData( "Script1_exe-script.py.log" )
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
    lhcbJob.addPackage( 'AppConfig', 'v3r171' )
    lhcbJob.addPackage( 'ProdConf', 'v1r9' )

    lhcbJob.setApplication( 'Gauss', 'v45r3', options,
                            extraPackages = 'AppConfig.v3r171;ProdConf.v1r9',
                            events = '3' )
    lhcbJob.setSystemConfig( 'x86_64-slc5-gcc43-opt' )

    res = lhcbJob.runLocal( self.dLHCb )
    self.assertTrue( res['OK'] )

  def test_boole( self ):

    # get a shifter proxy
    res = setupShifterProxyInEnv( 'ProductionManager' )
    print res

    lhcbJob = LHCbJob()

    lhcbJob.setName( "gaudirun-test-inputs" )
    lhcbJob.setInputSandbox( find_all( 'prodConf_Boole_00012345_00067890_1.py', '.', 'Integration' )[0] )

    opts = "$APPCONFIGOPTS/Boole/Default.py;"
    optDT = "$APPCONFIGOPTS/Boole/DataType-2012.py;"
    optTCK = "$APPCONFIGOPTS/L0/L0TCK-0x0042.py;"
    optComp = "$APPCONFIGOPTS/Persistency/Compression-ZLIB-1.py;"
    optPConf = "prodConf_Boole_00012345_00067890_1.py"
    options = opts + optDT + optTCK + optComp + optPConf
    lhcbJob.addPackage( 'AppConfig', 'v3r155' )

    lhcbJob.setApplication( 'Boole', 'v24r0', options,
                            inputData = '/lhcb/user/f/fstagni/test/12345/12345678/00012345_00067890_1.sim',
                            extraPackages = 'AppConfig.v3r155;ProdConf.v1r9' )

    res = lhcbJob.runLocal( self.dLHCb )
    self.assertTrue( res['OK'] )

class GaudiScriptSuccess( UserJobTestCase ):
  def test_execute( self ):

    lhcbJob = LHCbJob()

    lhcbJob.setName( "gaudiScript-test" )
    script = find_all( 'gaudi-script.py', '.', 'Integration' )[0]
    pConfFile = find_all( 'prodConf_Gauss_00012345_00067890_1.py', '.', 'Integration' )[0]
    lhcbJob.setInputSandbox( [pConfFile, script] )

    lhcbJob.addPackage( 'AppConfig', 'v3r171' )
    lhcbJob.addPackage( 'ProdConf', 'v1r9' )

    lhcbJob.setApplicationScript( 'Gauss', 'v45r3', script,
                                  extraPackages = 'AppConfig.v3r171;ProdConf.v1r9' )
    lhcbJob.setSystemConfig( 'x86_64-slc5-gcc43-opt' )

    res = lhcbJob.runLocal( self.dLHCb )
    self.assertTrue( res['OK'] )



if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( UserJobTestCase )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( HelloWorldSuccess ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( HelloWorldSuccessWithJobID ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( HelloWorldSuccessOutput ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( HelloWorldSuccessOutputWithJobID ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( GaudirunSuccess ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( GaudiScriptSuccess ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
