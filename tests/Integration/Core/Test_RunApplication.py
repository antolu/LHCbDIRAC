#!/usr/bin/env python
""" Tests of invocation of lb-run via RunApplication module
"""

#pylint: disable=invalid-name,wrong-import-position

import os
import unittest

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from DIRAC import gLogger
gLogger.setLevel('DEBUG')

from DIRAC.tests.Utilities.utils import find_all

from LHCbDIRAC.Core.Utilities.RunApplication import RunApplication
from LHCbDIRAC.Core.Utilities.ProductionOptions import getDataOptions, getModuleOptions


class RunApplicationTestCase( unittest.TestCase ):
  """ Base class for the RunApplication test cases
  """
  def setUp( self ):
    pass

  def tearDown( self ):
    pass

class GaussSuccess( RunApplicationTestCase ):
  """ Gauss cases
  """

  gLogger.always("\n ***************** Trying out GAUSS")

  def test_Gauss_Production_PR33857( self ):
    """ Using ProdConf (production style)

        This is taken from PR 33857 (and would fall back to SetupProject)
    """
    gLogger.always("**** GAUSS v49r5")

    ra = RunApplication()
    ra.applicationName = 'Gauss'
    ra.applicationVersion = 'v49r5'
    ra.systemConfig = 'x86_64-slc6-gcc48-opt'
    ra.commandOptions = ['$APPCONFIGOPTS/Gauss/Sim08-Beam4000GeV-mu100-2012-nu2.5.py',
                         '$APPCONFIGOPTS/Gauss/DataType-2012.py',
                         '$APPCONFIGOPTS/Gauss/RICHRandomHits.py',
                         '$APPCONFIGOPTS/Gauss/NoPacking.py',
                         '$DECFILESROOT/options/12877041.py',
                         '$LBPYTHIA8ROOT/options/Pythia8.py',
                         '$APPCONFIGOPTS/Gauss/G4PL_FTFP_BERT_EmNoCuts.py',
                         '$APPCONFIGOPTS/Persistency/Compression-ZLIB-1.py']
    ra.extraPackages = [('AppConfig', 'v3r277'),
                        ('DecFiles', 'v29r10'),
                        ('ProdConf', '')
                       ]
    ra.prodConfFileName = find_all('test_prodConf_gauss_v49r5.py', '..')[0]
    ra.applicationLog = '00033857_00000001_1_log.txt'
    ra.stdError = '00033857_00000001_1_err.txt'

    res = ra.run()
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value'], (0, '', ''))


  def test_Gauss_User_step133294( self ):
    """ Not using ProdConf (users style)

        This is taken from PR 33857 (and would fall back to SetupProject)
    """
    gLogger.always("**** GAUSS v49r5")

    ra = RunApplication()
    ra.applicationName = 'Gauss'
    ra.applicationVersion = 'v49r5'
    ra.systemConfig = 'x86_64-slc6-gcc48-opt'
    # ra.commandOptions = ['$APPCONFIGOPTS/Gauss/Sim08-Beam4000GeV-mu100-2012-nu2.5.py',
    #                      '$APPCONFIGOPTS/Gauss/DataType-2012.py',
    #                      '$APPCONFIGOPTS/Gauss/RICHRandomHits.py',
    #                      '$APPCONFIGOPTS/Gauss/NoPacking.py',
    #                      '$DECFILESROOT/options/12877041.py',
    #                      '$LBPYTHIA8ROOT/options/Pythia8.py',
    #                      '$APPCONFIGOPTS/Gauss/G4PL_FTFP_BERT_EmNoCuts.py',
    #                      '$APPCONFIGOPTS/Persistency/Compression-ZLIB-1.py']
    ra.extraPackages = [('AppConfig', 'v3r277'),
                        ('DecFiles', 'v29r10'),
                       ]

    generatedOpts = 'gaudi_extra_options.py'
    if os.path.exists( generatedOpts ):
      os.remove( generatedOpts )
    inputDataOpts = getDataOptions( 'Gauss',
                                    [],
                                    None,
                                    'pool_xml_catalog.xml' )['Value']  # always OK
    projectOpts = getModuleOptions( 'Gauss',
                                    1,
                                    inputDataOpts,
                                    '',
                                    5732353,
                                    4340993,
                                    'User' )['Value']  # always OK
    options = open( generatedOpts, 'w' )
    options.write( projectOpts )
    options.close()

    ra.applicationLog = 'user_133294_log.txt'
    ra.stdError = 'user_133294_err.txt'

    res = ra.run() #this won't really do anything but it doesn't matters
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value'], (0, '', ''))


class BooleSuccess( RunApplicationTestCase ):
  """ Boole cases
  """

  gLogger.always("\n ***************** Trying out BOOLE")

  def test_Boole_Production_PR33857( self ):
    """ Using ProdConf (production style)

        This is taken from PR 33857
    """
    gLogger.always("**** Boole v30r1")

    ra = RunApplication()
    ra.applicationName = 'Boole'
    ra.applicationVersion = 'v30r1'
    ra.systemConfig = 'x86_64-slc6-gcc48-opt'
    ra.commandOptions = ['$APPCONFIGOPTS/Boole/Default.py',
                         '$APPCONFIGOPTS/Boole/DataType-2012.py',
                         '$APPCONFIGOPTS/Boole/NoPacking.py',
                         '$APPCONFIGOPTS/Boole/Boole-SetOdinRndTrigger.py',
                         '$APPCONFIGOPTS/Persistency/Compression-ZLIB-1.py']
    ra.extraPackages = [('AppConfig', 'v3r266'),
                        ('ProdConf', '')
                       ]
    ra.step_Number = 1
    ra.prodConfFileName = find_all('test_prodConf_boole_v30r1.py', '..')[0]
    ra.applicationLog = '00033857_00000002_2_log.txt'
    ra.stdError = '00033857_00000002_2_err.txt'

    res = ra.run()
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value'], (0, '', ''))

  def test_Boole_Production_PR33857_2( self ):
    """ Same as before but using "ANY" as CMT config
    """
    gLogger.always("**** Boole v30r1")

    ra = RunApplication()
    ra.applicationName = 'Boole'
    ra.applicationVersion = 'v30r1'
    ra.commandOptions = ['$APPCONFIGOPTS/Boole/Default.py',
                         '$APPCONFIGOPTS/Boole/DataType-2012.py',
                         '$APPCONFIGOPTS/Boole/NoPacking.py',
                         '$APPCONFIGOPTS/Boole/Boole-SetOdinRndTrigger.py',
                         '$APPCONFIGOPTS/Persistency/Compression-ZLIB-1.py']
    ra.extraPackages = [('AppConfig', 'v3r266'),
                        ('ProdConf', '')
                       ]
    ra.step_Number = 1
    ra.prodConfFileName = find_all('test_prodConf_boole_v30r1_2.py', ',,')[0]
    ra.applicationLog = '00033857_00000002_3_log.txt'
    ra.stdError = '00033857_00000002_3_err.txt'

    res = ra.run()
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value'], (0, '', ''))


class MooreSuccess( RunApplicationTestCase ):
  """ Moore cases
  """

  gLogger.always("\n ***************** Trying out MOORE")

  def test_Moore_Production_PR33857( self ):
    """ Using ProdConf (production style)

        This is taken from PR 33857
    """
    gLogger.always("**** MOORE v20r4")

    ra = RunApplication()
    ra.applicationName = 'Moore'
    ra.applicationVersion = 'v20r4'
    ra.systemConfig = 'x86_64-slc6-gcc48-opt'
    ra.commandOptions = ['$APPCONFIGOPTS/L0App/L0AppSimProduction.py',
                         '$APPCONFIGOPTS/L0App/L0AppTCK-0x0045.py',
                         '$APPCONFIGOPTS/L0App/DataType-2012.py']
    ra.extraPackages = [('AppConfig', 'v3r200'),
                        ('ProdConf', '')
                       ]
    ra.prodConfFileName = find_all('test_prodConf_moore_v20r4.py', '..')[0]
    ra.applicationLog = '00033857_00000003_3_log.txt'
    ra.stdError = '00033857_00000003_3_err.txt'

    res = ra.run()
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value'], (0, '', ''))

  def test_Moore_Production_PR33857_2( self ):
    """ Using ProdConf (production style)

        This is taken from PR 33857
    """
    gLogger.always("**** MOORE v14r8p1")

    ra = RunApplication()
    ra.applicationName = 'Moore'
    ra.applicationVersion = 'v14r8p1'
    ra.systemConfig = 'x86_64-slc5-gcc46-opt'
    ra.commandOptions = ['$APPCONFIGOPTS/Moore/MooreSimProductionForSeparateL0AppStep.py',
                         '$APPCONFIGOPTS/Conditions/TCK-0x409f0045.py',
                         '$APPCONFIGOPTS/Moore/DataType-2012.py']
    ra.extraPackages = [('AppConfig', 'v3r241'),
                        ('ProdConf', '')
                       ]
    ra.prodConfFileName = find_all('test_prodConf_moore_v14r8p1.py', '..')[0]
    ra.applicationLog = '00033857_00000004_4_log.txt'
    ra.stdError = '00033857_00000004_4_err.txt'

    res = ra.run()
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value'], (0, '', ''))

class BrunelSuccess( RunApplicationTestCase ):
  """ Brunel cases
  """

  gLogger.always("\n ***************** Trying out BRUNEL")

  def test_Brunel_Production_PR33857( self ):
    """ Using ProdConf (production style)

        This is taken from PR 33857
    """
    gLogger.always("**** BRUNEL v43r2p11")

    ra = RunApplication()
    ra.applicationName = 'Brunel'
    ra.applicationVersion = 'v43r2p11'
    ra.systemConfig = 'x86_64-slc5-gcc46-opt'
    ra.commandOptions = ['$APPCONFIGOPTS/Brunel/DataType-2012.py',
                         '$APPCONFIGOPTS/Brunel/MC-WithTruth.py',
                         '$APPCONFIGOPTS/Brunel/Sim09-Run1.py',
                         '$APPCONFIGOPTS/Persistency/DST-multipleTCK-2012.py',
                         '$APPCONFIGOPTS/Persistency/Compression-ZLIB-1.py']
    ra.extraPackages = [('AppConfig', 'v3r302'),
                        ('ProdConf', '')
                       ]
    ra.prodConfFileName = find_all('test_prodConf_brunel_v43r2p11.py', '..')[0]
    ra.applicationLog = '00033857_00000005_5_log.txt'
    ra.stdError = '00033857_00000005_5_err.txt'

    res = ra.run()
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value'], (0, '', ''))


class DaVinciSuccess( RunApplicationTestCase ):
  """ DaVinci cases
  """

  gLogger.always("\n ***************** Trying out DAVINCI")

  def test_DaVinci_Production_PR33857( self ):
    """ Using ProdConf (production style)

        This is taken from PR 33857
    """
    gLogger.always("**** DAVINCI v32r2p1")

    ra = RunApplication()
    ra.applicationName = 'DaVinci'
    ra.applicationVersion = 'v32r2p1'
    ra.systemConfig = 'x86_64-slc5-gcc46-opt'
    ra.commandOptions = ['$CHARMCONFIGOPTS/MCFiltering/D02K3PiFromB2DstmunuXStripTrigFiltering_2012.py',
                         '$APPCONFIGOPTS/DaVinci/DataType-2012.py',
                         '$APPCONFIGOPTS/DaVinci/InputType-DST.py',
                         '$APPCONFIGOPTS/Persistency/Compression-ZLIB-1.py']
    ra.extraPackages = [('AppConfig', 'v3r305'),
                        ('CharmConfig', 'v3r30'),
                        ('ProdConf', '')
                       ]
    ra.step_Number = 1
    ra.prodConfFileName = find_all('test_prodConf_davinci_v32r2p1.py', '..')[0]
    ra.applicationLog = '00033857_00000006_6_log.txt'
    ra.stdError = '00033857_00000006_6_err.txt'

    res = ra.run()
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value'], (0, '', ''))


  def test_DaVinci_Production_PR33857_2( self ):
    """ Using ProdConf (production style)

        This is taken from PR 33857
    """
    gLogger.always("**** DAVINCI v41r3")

    ra = RunApplication()
    ra.applicationName = 'DaVinci'
    ra.applicationVersion = 'v41r3'
    ra.systemConfig = 'x86_64-slc6-gcc49-opt'
    ra.commandOptions = ['$APPCONFIGOPTS/Merging/DVMergeDST.py',
                         '$APPCONFIGOPTS/DaVinci/DataType-2012.py',
                         '$APPCONFIGOPTS/Merging/WriteFSR.py',
                         '$APPCONFIGOPTS/Merging/MergeFSR.py']
    ra.extraPackages = [('AppConfig', 'v3r305'),
                        ('ProdConf', '')
                       ]
    ra.step_Number = 1
    ra.prodConfFileName = find_all('test_prodConf_davinci_v41r3.py', '..')[0]
    ra.applicationLog = '00033857_00000007_7_log.txt'
    ra.stdError = '00033857_00000007_7_err.txt'

    res = ra.run()
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value'], (0, '', ''))


  def test_DaVinci_new_gcc49( self ):
    """ Using ProdConf (production style)

        This is taken from step 130339
    """
    gLogger.always("**** DAVINCI v42r1")

    ra = RunApplication()
    ra.applicationName = 'DaVinci'
    ra.applicationVersion = 'v42r1'
    ra.systemConfig = 'x86_64-slc6-gcc49-opt'
    ra.commandOptions = ['$APPCONFIGOPTS/DaVinci/DV-Stripping27-Stripping.py',
                         '$APPCONFIGOPTS/DaVinci/DataType-2016.py',
                         '$APPCONFIGOPTS/DaVinci/InputType-RDST.py',
                         '$APPCONFIGOPTS/DaVinci/DV-RawEventJuggler-0_3-to-4_2.py',
                         '$APPCONFIGOPTS/Persistency/Compression-ZLIB-1.py']
    ra.extraPackages = [('AppConfig', 'v3r308'),
                        ('SQLDDDB', 'v7r10'),
                        ('ProdConf', '')
                       ]
    ra.step_Number = 1
    ra.prodConfFileName = find_all('test_prodConf_davinci_v42r1.py', '..')[0]
    ra.applicationLog = '0daVinci_000v42r1_49_log.txt'
    ra.stdError = '0daVinci_000v42r1_49_err.txt'

    res = ra.run()
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value'], (1, '', '')) #This will fail as there's no input file


  def test_DaVinci_new_gcc62( self ):
    """ Using ProdConf (production style)

        This is taken from step 130339
    """
    gLogger.always("**** DAVINCI v42r1")

    ra = RunApplication()
    ra.applicationName = 'DaVinci'
    ra.applicationVersion = 'v42r1'
    ra.systemConfig = 'x86_64-slc6-gcc62-opt'
    ra.commandOptions = ['$APPCONFIGOPTS/DaVinci/DV-Stripping27-Stripping.py',
                         '$APPCONFIGOPTS/DaVinci/DataType-2016.py',
                         '$APPCONFIGOPTS/DaVinci/InputType-RDST.py',
                         '$APPCONFIGOPTS/DaVinci/DV-RawEventJuggler-0_3-to-4_2.py',
                         '$APPCONFIGOPTS/Persistency/Compression-ZLIB-1.py']
    ra.extraPackages = [('AppConfig', 'v3r308'),
                        ('SQLDDDB', 'v7r10'),
                        ('ProdConf', '')
                       ]
    ra.step_Number = 1
    ra.prodConfFileName = find_all('test_prodConf_davinci_v42r1.py', '..')[0]
    ra.applicationLog = '0daVinci_000v42r1_62_log.txt'
    ra.stdError = '0daVinci_000v42r1_62_err.txt'

    res = ra.run()
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value'], (1, '', '')) #This will fail as there's no input file




#############################################################################
# Test Suite run
#############################################################################

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( RunApplicationTestCase )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( GaussSuccess ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( BooleSuccess ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( MooreSuccess ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( BrunelSuccess ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( DaVinciSuccess ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )

#try multicore
