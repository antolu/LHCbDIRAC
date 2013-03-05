from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

import os.path
import unittest

from LHCbTestDirac.Regression.utils import cleanTestDir

from DIRAC import gLogger
from LHCbDIRAC.Interfaces.API.LHCbJob import LHCbJob
from LHCbDIRAC.Interfaces.API.DiracLHCb import DiracLHCb

gLogger.setLevel( 'DEBUG' )

cwd = os.path.realpath( '.' )

class UserJobTestCase( unittest.TestCase ):
  ''' Base class for the UserJob test cases
  '''
  def setUp( self ):
    cleanTestDir()

    gLogger.setLevel( 'DEBUG' )
    self.dLHCb = DiracLHCb()


class HelloWorldSuccess( UserJobTestCase ):
  def test_execute( self ):

    lhcbJob = LHCbJob()

    lhcbJob.setName( "helloWorld-test" )
    lhcbJob.setExecutable( "exe-script.py" )
    res = lhcbJob.runLocal( self.dLHCb )
    self.assertTrue( res['OK'] )

class GaudirunSuccess( UserJobTestCase ):
  def test_mc( self ):

    lhcbJob = LHCbJob()

    lhcbJob.setName( "gaudirun-test" )
    lhcbJob.setInputSandbox( 'prodConf_Gauss_00012345_00067890_1.py' )

    optGauss = "$APPCONFIGOPTS/Gauss/Beam4000GeV-md100-JulSep2012-nu2.5.py;"
    optDec = "$DECFILESROOT/options/15512012.py;"
    optPythia = "$LBPYTHIAROOT/options/Pythia.py;"
    optOpts = " $APPCONFIGOPTS/Gauss/G4PL_LHEP_EmNoCuts.py;"
    optCompr = "$APPCONFIGOPTS/Persistency/Compression-ZLIB-1.py;"
    optPConf = "prodConf_Gauss_00012345_00067890_1.py"
    options = optGauss + optDec + optPythia + optOpts + optCompr + optPConf
    lhcbJob.addPackage( 'AppConfig', 'v3r160' )
    lhcbJob.addPackage( 'DecFiles', 'v26r24' )
    lhcbJob.addPackage( 'ProdConf', 'v1r9' )

    lhcbJob.setApplication( 'Gauss', 'v42r4', options,
                            extraPackages = 'AppConfig.v3r160;DecFiles.v26r24;ProdConf.v1r9' )

    res = lhcbJob.runLocal( self.dLHCb )
    self.assertTrue( res['OK'] )

  def test_boole( self ):

    lhcbJob = LHCbJob()

    lhcbJob.setName( "gaudirun-test-inputs" )
    lhcbJob.setInputSandbox( 'prodConf_Boole_00012345_00067890_1.py' )

    opts = "$APPCONFIGOPTS/Boole/Default.py;$APPCONFIGOPTS/Boole/DataType-2012.py;$APPCONFIGOPTS/L0/L0TCK-0x0042.py;$APPCONFIGOPTS/Persistency/Compression-ZLIB-1.py"
    optPConf = "prodConf_Boole_00012345_00067890_1.py"
    options = opts + optPConf
    lhcbJob.addPackage( 'AppConfig', 'v3r155' )

    lhcbJob.setApplication( 'Boole', 'v24r0', options,
                            inputData = '/lhcb/user/f/fstagni/test/12345/12345678/00012345_00067890_1.sim',
                            extraPackages = 'AppConfig.v3r155;ProdConf.v1r9' )

    res = lhcbJob.runLocal( self.dLHCb )
    self.assertTrue( res['OK'] )

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
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( HelloWorldSuccess ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( GaudirunSuccess ) )
#  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( GaudiScriptSuccess ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
