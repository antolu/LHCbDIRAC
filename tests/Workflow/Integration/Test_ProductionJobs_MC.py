#!/usr/bin/env python
""" "Integration" production jobs. StepIDs are taken from REAL productions that ran "recently"
"""

# pylint: disable=line-too-long,protected-access,missing-docstring,invalid-name,wrong-import-position

import os
import sys
import unittest

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from DIRAC.tests.Utilities.utils import find_all

from tests.Utilities.IntegrationTest import IntegrationTest

from LHCbDIRAC import rootPath
from LHCbDIRAC.Interfaces.API.DiracProduction import DiracProduction
from LHCbDIRAC.ProductionManagementSystem.Client.ProductionRequest import ProductionRequest


class ProductionJobTestCase(IntegrationTest):
  """ Base class for the ProductionJob test cases
  """

  def setUp(self):
    super(ProductionJobTestCase, self).setUp()

    self.pr = ProductionRequest()
    self.diracProduction = DiracProduction()


class MCSuccess(ProductionJobTestCase):
  def test_Integration_Production(self):

    options = '$APPCONFIGOPTS/Gauss/Beam6500GeV-md100-2015-nu1.6.py;'
    options += '$APPCONFIGOPTS/Gauss/EnableSpillover-25ns.py;'
    options += '$APPCONFIGOPTS/Gauss/DataType-2015.py;'
    options += '$APPCONFIGOPTS/Gauss/RICHRandomHits.py;'
    options += '$DECFILESROOT/options/28144011.py;'
    options += '$LBPYTHIA8ROOT/options/Pythia8.py;'
    options += '$APPCONFIGOPTS/Gauss/G4PL_FTFP_BERT_EmNoCuts.py'

    # From request 48257
    stepsInProd = [{'StepId': 133659, 'StepName': 'Sim09d', 'ApplicationName': 'Gauss', 'ApplicationVersion': 'v49r10',
                    'ExtraPackages': 'AppConfig.v3r359;Gen/DecFiles.v30r17',
                    'ProcessingPass': 'Sim09d', 'Visible': 'Y', 'Usable': 'Yes',
                    'DDDB': 'dddb-20170721-3', 'CONDDB': 'sim-20161124-vc-md100', 'DQTag': '', 'OptionsFormat': '',
                    'OptionFiles': options,
                    'isMulticore': 'N', 'SystemConfig': 'x86_64-slc6-gcc48-opt', 'mcTCK': '', 'ExtraOptions': '',
                    'fileTypesIn': [],
                    'fileTypesOut':['SIM'],
                    'visibilityFlag':[{'Visible': 'N', 'FileType': 'SIM'}]}]

    # First create the production object
    prod = self.pr._buildProduction(prodType='MCSimulation', stepsInProd=stepsInProd, outputSE={'DIGI': 'Tier1_MC-DST'},
                                    priority=0, cpu=100, outputFileMask='DIGI')
    try:
      # This is the standard location in Jenkins
      prod.LHCbJob.setInputSandbox(find_all('pilot.cfg', os.environ['WORKSPACE'] + '/PilotInstallDIR')[0])
    except (IndexError, KeyError):
      prod.LHCbJob.setInputSandbox(find_all('pilot.cfg', rootPath)[0])
    prod.LHCbJob.setConfigArgs('pilot.cfg')
    prod.setParameter('numberOfEvents', 'string', 2, 'Number of events to test')
    # Then launch it
    res = self.diracProduction.launchProduction(prod, False, True, 0)

    self.assertTrue(res['OK'])


#############################################################################
# Test Suite run
#############################################################################

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(ProductionJobTestCase)
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(MCSuccess))
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)
  sys.exit(not testResult.wasSuccessful())
