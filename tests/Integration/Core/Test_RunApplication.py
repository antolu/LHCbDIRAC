#!/usr/bin/env python
""" Tests of invocation of lb-run via RunApplication module
"""

#pylint: disable=invalid-name,wrong-import-position

import unittest

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from DIRAC import gLogger
gLogger.setLevel('DEBUG')

from LHCbDIRAC.Core.Utilities.RunApplication import RunApplication


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
    ra.commandOptions = ['$APPCONFIGOPTS/Gauss/Sim08-Beam4000GeV-mu100-2012-nu2.5.pyi',
                         '$APPCONFIGOPTS/Gauss/DataType-2012.py',
                         '$APPCONFIGOPTS/Gauss/RICHRandomHits.py',
                         '$APPCONFIGOPTS/Gauss/NoPacking.py',
                         '$DECFILESROOT/options/@{eventType}.py',
                         '$LBPYTHIA8ROOT/options/Pythia8.py',
                         '$APPCONFIGOPTS/Gauss/G4PL_FTFP_BERT_EmNoCuts.py',
                         '$APPCONFIGOPTS/Persistency/Compression-ZLIB-1.py']
    ra.extraPackages = [('AppConfig', 'v3r277'),
                        ('DecFiles', 'v29r10'),
                        ('ProdConf', '')
                       ]
    ra.step_Number = 1
    ra.prodConfFileName = 'test_prodConf_gauss_v49r5.py'

    res = ra.run()
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value'], (0, '', ''))


  def test_Gauss_Production_step133294( self ):
    """ Using ProdConf (production style)

        This is taken from step 133294
    """

    gLogger.always("**** GAUSS v50r0")

    ra = RunApplication()
    ra.applicationName = 'Gauss'
    ra.applicationVersion = 'v50r0'
    ra.commandOptions = ['$APPCONFIGOPTS/Gauss/Beam7000GeV-md100-nu7.6-HorExtAngle.py',
                         '$DECFILESROOT/options/@{eventType}.py',
                         '$LBPYTHIA8ROOT/options/Pythia8.py',
                         '$APPCONFIGOPTS/Gauss/Gauss-Upgrade-Baseline-20150522.py',
                         '$APPCONFIGOPTS/Persistency/Compression-ZLIB-1.py']
    ra.extraPackages = [('AppConfig', 'v3r304'),
                        ('DecFiles', 'v29r9'),
                        ('ProdConf', '')
                       ]
    ra.systemConfig = 'x86_64-slc6-gcc48-opt'
    ra.step_Number = 1
    ra.prodConfFileName = 'test_prodConf_gauss_v50r0.py'

    res = ra.run()
    self.assertTrue(res['OK'])
    self.assertEqual(res['Value'], (0, '', ''))


#############################################################################
# Test Suite run
#############################################################################

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( RunApplicationTestCase )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( GaussSuccess ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )


#
#
# gLogger.always("\n ***************** Trying out GAUSS, no ProdConf (user style) \n")
#
# gLogger.always("**** GAUSS v49r5")
#
# ra = RunApplication()
# ra.applicationName = 'Gauss'
# ra.applicationVersion = 'v49r5'
# ra.commandOptions = ['$APPCONFIGOPTS/Gauss/Sim08-Beam4000GeV-mu100-2012-nu2.5.pyi',
#                      '$APPCONFIGOPTS/Gauss/DataType-2012.py',
#                      '$APPCONFIGOPTS/Gauss/RICHRandomHits.py',
#                      '$APPCONFIGOPTS/Gauss/NoPacking.py',
#                      '$DECFILESROOT/options/@{eventType}.py',
#                      '$LBPYTHIA8ROOT/options/Pythia8.py',
#                      '$APPCONFIGOPTS/Gauss/G4PL_FTFP_BERT_EmNoCuts.py',
#                      '$APPCONFIGOPTS/Persistency/Compression-ZLIB-1.py']
# ra.extraPackages = [('AppConfig', 'v3r277'),
#                     ('DecFiles', 'v29r10'),
#                    ]
# ra.step_Number = 1
#
# #print ra.gaudirunCommand()
#
# ra.run()
#
#
#
#
#
# ### Moore
# gLogger.always("\n ***************** Trying out Moore \n")


#try multicore
