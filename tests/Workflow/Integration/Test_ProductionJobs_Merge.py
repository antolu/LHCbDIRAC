#!/usr/bin/env python
""" "Integration" production jobs. StepIDs are taken from REAL productions that ran "recently"
"""

# pylint: disable=line-too-long,protected-access,invalid-name,wrong-import-position

import os
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


class MCMergeSuccess(ProductionJobTestCase):
  def test_Integration_Production(self):

    lfns = ['/lhcb/MC/2012/BDSTH.STRIP.DST/00051752/0000/00051752_00001269_2.bdsth.Strip.dst',
            '/lhcb/MC/2012/BDSTH.STRIP.DST/00051752/0000/00051752_00001263_2.bdsth.Strip.dst']
    # From request 31139
    optionFiles = '$APPCONFIGOPTS/Merging/DVMergeDST.py;$APPCONFIGOPTS/DaVinci/DataType-2012.py;'
    optionFiles += '$APPCONFIGOPTS/Merging/WriteFSR.py;$APPCONFIGOPTS/Merging/MergeFSR.py'
    stepsInProd = [{'StepId': 129424, 'StepName': 'Stripping24NoPrescalingFlagged',
                    'ApplicationName': 'DaVinci', 'ApplicationVersion': 'v40r1p2',
                    'ExtraPackages': 'AppConfig.v3r263', 'ProcessingPass': 'Stripping24NoPrescalingFlagged',
                    'Visible': 'N', 'Usable': 'Yes',
                    'DDDB': 'dddb-20130929-1', 'CONDDB': 'sim-20130522-1-vc-md100', 'DQTag': '', 'OptionsFormat': 'Merge',
                    'OptionFiles': optionFiles,
                    'mcTCK': '', 'ExtraOptions': '',
                    'isMulticore': 'N', 'SystemConfig': '',
                    'fileTypesIn': ['BDSTH.STRIP.DST'],
                    'fileTypesOut':['BDSTH.STRIP.DST'],
                    'visibilityFlag':[{'Visible': 'Y', 'FileType': 'BDSTH.STRIP.DST'}]}]

    prod = self.pr._buildProduction('Merge', stepsInProd, {'BDSTH.STRIP.DST': 'Tier1_MC-DST'}, 0, 100,
                                    inputDataPolicy='protocol', inputDataList=lfns)
    try:
      # This is the standard location in Jenkins
      prod.LHCbJob.setInputSandbox(find_all('pilot.cfg', os.environ['WORKSPACE'] + '/PilotInstallDIR')[0])
    except (IndexError, KeyError):
      prod.LHCbJob.setInputSandbox(find_all('pilot.cfg', rootPath)[0])
    prod.LHCbJob.setConfigArgs('pilot.cfg')
    res = self.diracProduction.launchProduction(prod, False, True, 0)
    self.assertTrue(res['OK'])


class RootMergeSuccess(ProductionJobTestCase):
  def test_Integration_Production(self):
    # From request 41740
    lfns = ['/lhcb/LHCb/Collision17/BRUNELHIST/00064899/0006/Brunel_00064899_00067792_1.Hist.root',
            '/lhcb/LHCb/Collision17/BRUNELHIST/00064899/0006/Brunel_00064899_00067793_1.Hist.root']

    stepsInProd = [{'StepId': 132150, 'StepName': 'Histo-merging-Reco17-6500GeV-MagUp-Full',
                    'ApplicationName': 'Noether', 'ApplicationVersion': 'v1r4',
                    'ExtraPackages': 'AppConfig.v3r337', 'ProcessingPass': 'HistoMerge02',
                    'Visible': 'Y', 'Usable': 'Yes',
                    'DDDB': '', 'CONDDB': '', 'DQTag': '', 'OptionsFormat': '',
                    'OptionFiles': ' $APPCONFIGOPTS/DataQuality/DQMergeRun.py',
                    'mcTCK': '', 'ExtraOptions': '',
                    'isMulticore': 'N', 'SystemConfig': '',
                    'fileTypesIn': ['BRUNELHIST', 'DAVINCIHIST'],
                    'fileTypesOut':['HIST.ROOT'],
                    'visibilityFlag':[{'Visible': 'Y', 'FileType': 'HIST.ROOT'}]}]

    prod = self.pr._buildProduction('HistoMerge', stepsInProd, {'HIST.ROOT': 'CERN-EOS-HIST'}, 0, 100,
                                    inputDataPolicy='protocol', inputDataList=lfns)
    try:
      # This is the standard location in Jenkins
      prod.LHCbJob.setInputSandbox(find_all('pilot.cfg', os.environ['WORKSPACE'] + '/PilotInstallDIR')[0])
    except (IndexError, KeyError):
      prod.LHCbJob.setInputSandbox(find_all('pilot.cfg', rootPath)[0])
    prod.LHCbJob.setConfigArgs('pilot.cfg')
    res = self.diracProduction.launchProduction(prod, False, True, 0)
    self.assertTrue(res['OK'])


#############################################################################
# Test Suite run
#############################################################################

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(ProductionJobTestCase)
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(MCMergeSuccess))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(RootMergeSuccess))
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)
