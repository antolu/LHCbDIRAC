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

    lfns = ['/lhcb/MC/2012/BDSTH.STRIP.DST/00051752/0000/00051752_00001269_1.bdsth.Strip.dst',
            '/lhcb/MC/2012/BDSTH.STRIP.DST/00051752/0000/00051752_00001263_1.bdsth.Strip.dst']
    # From request 31139
    optionFiles = '$APPCONFIGOPTS/DaVinci/DV-Stripping24-Stripping-MC-NoPrescaling.py;'
    optionFiles += '$APPCONFIGOPTS/DaVinci/DataType-2015.py;$APPCONFIGOPTS/DaVinci/InputType-DST.py'
    stepsInProd = [{'StepId': 129267, 'StepName': 'Stripping24NoPrescalingFlagged',
                    'ApplicationName': 'DaVinci', 'ApplicationVersion': 'v38r1p1',
                    'ExtraPackages': 'AppConfig.v3r262', 'ProcessingPass': 'Stripping24NoPrescalingFlagged',
                    'Visible': 'N', 'Usable': 'Yes',
                    'DDDB': 'dddb-20150724', 'CONDDB': ' sim-20160606-vc-md100', 'DQTag': '', 'OptionsFormat': 'merge',
                    'OptionFiles': optionFiles,
                    'mcTCK': '', 'ExtraOptions': '',
                    'isMulticore': 'N', 'SystemConfig': 'x86_64-slc6-gcc48-opt',
                    'fileTypesIn': ['DST'],
                    'fileTypesOut':['ALLSTREAMS.DST'],
                    'visibilityFlag':[{'Visible': 'Y', 'FileType': 'ALLSTREAMS.DST'}]}]

    prod = self.pr._buildProduction('Merge', stepsInProd, {'ALLSTREAMS.DST': 'Tier1_MC-DST'}, 0, 100,
                                    inputDataPolicy='protocol', inputDataList=lfns)
    try:
      # This is the standard location in Jenkins
      prod.LHCbJob.setInputSandbox(find_all('pilot.cfg', os.environ['WORKSPACE'] + '/PilotInstallDIR')[0])
    except (IndexError, KeyError):
      prod.LHCbJob.setInputSandbox(find_all('pilot.cfg', rootPath)[0])
    prod.LHCbJob.setConfigArgs('pilot.cfg')
    res = self.diracProduction.launchProduction(prod, False, True, 0)
    self.assertTrue(res['OK'])


class MergeMDFSuccess(ProductionJobTestCase):
  def test_Integration_Production(self):
    lfns = ['/lhcb/data/2011/RAW/EXPRESS/LHCb/COLLISION11/102360/102360_0000000031.raw',
            '/lhcb/data/2011/RAW/EXPRESS/LHCb/COLLISION11/97887/097887_0000000013.raw']
    # From request 9054
    stepsInProd = [{'StepId': 123897, 'StepName': 'MergeMDF', 'ApplicationName': 'MergeMDF', 'ApplicationVersion': '',
                    'ExtraPackages': '', 'ProcessingPass': 'Merging', 'Visible': 'Y', 'Usable': 'Yes',
                    'DDDB': '', 'CONDDB': '', 'DQTag': '', 'OptionsFormat': '',
                    'OptionFiles': '', 'SystemConfig': '', 'mcTCK': '', 'ExtraOptions': '',
                    'isMulticore': 'N',
                    'fileTypesIn': ['RAW'],
                    'fileTypesOut':['RAW'],
                    'visibilityFlag':[{'Visible': 'Y', 'FileType': 'RAW'}]}]
    self.pr.modulesList = ['MergeMDF', 'BookkeepingReport']
    prod = self.pr._buildProduction('Merge', stepsInProd, {'RAW': 'Tier1-Buffer'}, 0, 100,
                                    inputDataPolicy='download', inputDataList=lfns)
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
  #  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( MCMergeSuccess ) )
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(MergeMDFSuccess))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(RootMergeSuccess))
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)
