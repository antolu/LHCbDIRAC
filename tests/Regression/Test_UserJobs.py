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
  def test_execute( self ):

    lhcbJob = LHCbJob()

    lhcbJob.setName( "gaudirun-test" )

    optGauss = "$cwd/Beam4000GeV-md100-JulSep2012-nu2.5.py"
    optDec = "$cwd/options/15512012.py"
    optPythia = "$cwd/Pythia.py"
    optOpts = "$cwd/G4PL_LHEP_EmNoCuts.py"
    optCompr = "$cwd/Compression-ZLIB-1.py"
    optPConf = "$cwd/prodConf_Gauss_00023060_00002595_1.py"
    options = optGauss + optDec + optPythia + optOpts + optCompr, optPConf
    lhcbJob.addPackage( 'AppConfig', 'v3r151' )
    lhcbJob.addPackage( 'DecFiles', 'v26r24' )
    lhcbJob.addPackage( 'ProdConf', 'v1r9' )

#    optGauss = "/cvmfs/lhcb.cern.ch/lib/lhcb/DBASE/AppConfig/v3r151/options/Gauss/Beam3500GeV-md100-MC11-nu2.py;"
#    optDec = "$DECFILESROOT/options/12133041.py;"
#    optPythia = "$LBPYTHIAROOT/options/Pythia.py;"
#    optOpts = "/cvmfs/lhcb.cern.ch/lib/lhcb/DBASE/AppConfig/v3r151/options/Gauss/G4PL_LHEP_EmNoCuts.py"
#    options = optGauss + optDec + optPythia + optOpts

    lhcbJob.setApplication( 'Gauss', 'v41r4', options )

    res = lhcbJob.runLocal( self.dLHCb )
    self.assertTrue( res['OK'] )


if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( UserJobTestCase )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( HelloWorldSuccess ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( GaudirunSuccess ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
