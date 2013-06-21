import unittest, os

from LHCbTestDirac.Utilities.IntegrationTest import IntegrationTest

from LHCbDIRAC.Interfaces.API.LHCbJob import LHCbJob
from LHCbDIRAC.Interfaces.API.DiracLHCb import DiracLHCb

class UserJobTestCase( IntegrationTest ):
  ''' Base class for the UserJob test cases
  '''
  def setUp( self ):
    super( IntegrationTest, self ).setUp()

    self.dLHCb = DiracLHCb()


class HelloWorldSuccess( UserJobTestCase ):
  def test_execute( self ):

    lhcbJob = LHCbJob()

    lhcbJob.setName( "helloWorld-test" )
    lhcbJob.setExecutable( "exe-script.py" )
    res = lhcbJob.runLocal( self.dLHCb )
    self.assertTrue( res['OK'] )

# class HelloWorldSuccessWithJobID( UserJobTestCase ):
#  def test_execute( self ):
#
#    os.environ['JOBID'] = '12345'
#
#    lhcbJob = LHCbJob()
#
#    lhcbJob.setName( "helloWorld-test" )
#    lhcbJob.setExecutable( "exe-script.py" )
#    res = lhcbJob.runLocal( self.dLHCb )
#    self.assertTrue( res['OK'] )  # There's nothing to upload, so it will complete happily
#
#    del os.environ['JOBID']
#
# class HelloWorldSuccessOutput( UserJobTestCase ):
#  def test_execute( self ):
#
#    lhcbJob = LHCbJob()
#
#    lhcbJob.setName( "helloWorld-test" )
#    lhcbJob.setExecutable( "exe-script.py" )
#    lhcbJob.setOutputData( "Script1_exe-script.py.log" )
#    res = lhcbJob.runLocal( self.dLHCb )
#    self.assertTrue( res['OK'] )
#
# class HelloWorldSuccessOutputWithJobID( UserJobTestCase ):
#  def test_execute( self ):
#
#    os.environ['JOBID'] = '12345'
#
#    lhcbJob = LHCbJob()
#
#    lhcbJob.setName( "helloWorld-test" )
#    lhcbJob.setExecutable( "exe-script.py" )
#    lhcbJob.setOutputData( "Script1_exe-script.py.log" )
#    res = lhcbJob.runLocal( self.dLHCb )  # Can't upload, so it will fail
#    self.assertFalse( res['OK'] )
#
#    del os.environ['JOBID']
#
# class GaudirunSuccess( UserJobTestCase ):
#  def test_mc( self ):
#
#    lhcbJob = LHCbJob()
#
#    lhcbJob.setName( "gaudirun-test" )
#    lhcbJob.setInputSandbox( 'prodConf_Gauss_00012345_00067890_1.py' )
#
#    optGauss = "$APPCONFIGOPTS/Gauss/Beam4000GeV-md100-JulSep2012-nu2.5.py;"
#    optDec = "$DECFILESROOT/options/15512012.py;"
#    optPythia = "$LBPYTHIAROOT/options/Pythia.py;"
#    optOpts = " $APPCONFIGOPTS/Gauss/G4PL_LHEP_EmNoCuts.py;"
#    optCompr = "$APPCONFIGOPTS/Persistency/Compression-ZLIB-1.py;"
#    optPConf = "prodConf_Gauss_00012345_00067890_1.py"
#    options = optGauss + optDec + optPythia + optOpts + optCompr + optPConf
#    lhcbJob.addPackage( 'AppConfig', 'v3r160' )
#    lhcbJob.addPackage( 'DecFiles', 'v26r24' )
#    lhcbJob.addPackage( 'ProdConf', 'v1r9' )
#
#    lhcbJob.setApplication( 'Gauss', 'v42r4', options,
#                            extraPackages = 'AppConfig.v3r160;DecFiles.v26r24;ProdConf.v1r9' )
#
#    res = lhcbJob.runLocal( self.dLHCb )
#    self.assertTrue( res['OK'] )
#
#  def test_boole( self ):
#
#    lhcbJob = LHCbJob()
#
#    lhcbJob.setName( "gaudirun-test-inputs" )
#    lhcbJob.setInputSandbox( 'prodConf_Boole_00012345_00067890_1.py' )
#
#    opts = "$APPCONFIGOPTS/Boole/Default.py;"
#    optDT = "$APPCONFIGOPTS/Boole/DataType-2012.py;"
#    optTCK = "$APPCONFIGOPTS/L0/L0TCK-0x0042.py;"
#    optComp = "$APPCONFIGOPTS/Persistency/Compression-ZLIB-1.py;"
#    optPConf = "prodConf_Boole_00012345_00067890_1.py"
#    options = opts + optDT + optTCK + optComp + optPConf
#    lhcbJob.addPackage( 'AppConfig', 'v3r155' )
#
#    lhcbJob.setApplication( 'Boole', 'v24r0', options,
#                            inputData = '/lhcb/user/f/fstagni/test/12345/12345678/00012345_00067890_1.sim',
#                            extraPackages = 'AppConfig.v3r155;ProdConf.v1r9' )
#
#    res = lhcbJob.runLocal( self.dLHCb )
#    self.assertTrue( res['OK'] )

# class GaudiScriptSuccess( UserJobTestCase ):
#  def test_execute( self ):
#
#    lhcbJob = LHCbJob()
#
#    lhcbJob.setName( "gaudiScript-test" )
#    lhcbJob.setInputSandbox( ['prodConf_Gauss_00012345_00067890_1.py', 'gaudi-script.py'] )
#
#    lhcbJob.addPackage( 'AppConfig', 'v3r160' )
#    lhcbJob.addPackage( 'DecFiles', 'v26r24' )
#    lhcbJob.addPackage( 'ProdConf', 'v1r9' )
#    script = 'gaudi-script.py'
#
#    lhcbJob.setApplicationScript( 'Gauss', 'v42r4', script,
#                                  extraPackages = 'AppConfig.v3r160;DecFiles.v26r24;ProdConf.v1r9' )
#
#    res = lhcbJob.runLocal( self.dLHCb )
#    self.assertTrue( res['OK'] )



if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( UserJobTestCase )
#  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( HelloWorldSuccess ) )
#  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( HelloWorldSuccessWithJobID ) )
#  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( HelloWorldSuccessOutput ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( HelloWorldSuccessOutputWithJobID ) )
#  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( GaudirunSuccess ) )
#  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( GaudiScriptSuccess ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
