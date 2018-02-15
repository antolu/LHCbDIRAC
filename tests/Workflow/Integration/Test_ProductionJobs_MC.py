#!/usr/bin/env python
""" "Integration" production jobs. StepIDs are taken from REAL productions that ran "recently"
"""

# pylint: disable=line-too-long,protected-access,missing-docstring,invalid-name,wrong-import-position

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


class MCSuccess(ProductionJobTestCase):
  def test_Integration_Production(self):

    # From request 39072
    stepsInProd = [{'StepId': 131147, 'StepName': 'Sim09b', 'ApplicationName': 'Gauss', 'ApplicationVersion': 'v49r7',
                    'ExtraPackages': 'AppConfig.v3r304;DecFiles.v29r21', 'ProcessingPass': 'Sim09b', 'Visible': 'Y', 'Usable': 'Yes',
                    'DDDB': 'dddb-20150724', 'CONDDB': 'sim-20161124-2-vc-md100', 'DQTag': '', 'OptionsFormat': '',
                    'OptionFiles': '$APPCONFIGOPTS/Gauss/Beam6500GeV-md100-2016-nu1.6.py;$APPCONFIGOPTS/Gauss/EnableSpillover-25ns.py;$APPCONFIGOPTS/Gauss/DataType-2016.py;$APPCONFIGOPTS/Gauss/RICHRandomHits.py;$DECFILESROOT/options/27165100.py;$LBPYTHIA8ROOT/options/Pythia8.py;$APPCONFIGOPTS/Gauss/G4PL_FTFP_BERT_EmNoCuts.py;$APPCONFIGOPTS/Persistency/Compression-ZLIB-1.py',
                    'isMulticore': 'N', 'SystemConfig': 'x86_64-slc6-gcc48-opt', 'mcTCK': '', 'ExtraOptions': '',
                    'fileTypesIn': [],
                    'fileTypesOut':['SIM'],
                    'visibilityFlag':[{'Visible': 'N', 'FileType': 'SIM'}]},
                   {'StepId': 130262, 'StepName': 'Digi14b', 'ApplicationName': 'Boole', 'ApplicationVersion': 'v30r2',
                    'ExtraPackages': 'AppConfig.v3r304', 'ProcessingPass': 'Digi14b', 'Visible': 'N', 'Usable': 'Yes',
                    'DDDB': 'dddb-20150724', 'CONDDB': 'sim-20161124-2-vc-md100', 'DQTag': '', 'OptionsFormat': 'x86_64-slc6-gcc49-opt',
                    'isMulticore': 'N', 'SystemConfig': '', 'mcTCK': '', 'ExtraOptions': '',
                    'OptionFiles': '$APPCONFIGOPTS/Boole/Default.py;$APPCONFIGOPTS/Boole/EnableSpillover.py;$APPCONFIGOPTS/Boole/DataType-2015.py;$APPCONFIGOPTS/Boole/Boole-SetOdinRndTrigger.py;$APPCONFIGOPTS/Persistency/Compression-ZLIB-1.py',
                    'fileTypesIn': ['SIM'],
                    'fileTypesOut':['DIGI'],
                    'visibilityFlag':[{'Visible': 'N', 'FileType': 'DIGI'}]},
                   {'StepId': 130088, 'StepName': 'L0 emulation for 2016 - TCK 0x160F - DIGI', 'ApplicationName': 'Moore', 'ApplicationVersion': 'v25r4',
                    'ExtraPackages': 'AppConfig.v3r297', 'ProcessingPass': 'L0Trig0x160F', 'Visible': 'N', 'Usable': 'Yes',
                    'DDDB': 'dddb-20150724', 'CONDDB': 'sim-20161124-2-vc-md100', 'DQTag': '', 'OptionsFormat': 'l0app',
                    'isMulticore': 'N', 'SystemConfig': 'x86_64-slc6-gcc48-opt', 'mcTCK': '', 'ExtraOptions': '',
                    'OptionFiles': '$APPCONFIGOPTS/L0App/L0AppSimProduction.py;$APPCONFIGOPTS/L0App/L0AppTCK-0x160F.py;$APPCONFIGOPTS/L0App/ForceLUTVersionV8.py;$APPCONFIGOPTS/L0App/DataType-2016.py;$APPCONFIGOPTS/Persistency/Compression-ZLIB-1.py',
                    'fileTypesIn': ['DIGI'],
                    'fileTypesOut':['DIGI'],
                    'visibilityFlag':[{'Visible': 'N', 'FileType': 'DIGI'}]},
                   {'StepId': 130089, 'StepName': 'TCK-0x5138160F (HLT1) Flagged for 2016 - DIGI', 'ApplicationName': 'Moore', 'ApplicationVersion': 'v25r4',
                    'ExtraPackages': 'AppConfig.v3r297', 'ProcessingPass': 'Trig0x5138160F', 'Visible': 'N', 'Usable': 'Yes',
                    'DDDB': 'dddb-20150724', 'CONDDB': 'sim-20161124-2-vc-md100', 'DQTag': '', 'OptionsFormat': '',
                    'OptionFiles': '$APPCONFIGOPTS/Moore/MooreSimProductionForSeparateL0AppStep2015.py;$APPCONFIGOPTS/Conditions/TCK-0x5138160F.py;$APPCONFIGOPTS/Moore/DataType-2016.py;$APPCONFIGOPTS/Persistency/Compression-ZLIB-1.py;$APPCONFIGOPTS/Moore/MooreSimProductionHlt1.py',
                    'isMulticore': 'N', 'SystemConfig': 'x86_64-slc6-gcc48-opt', 'mcTCK': '', 'ExtraOptions': '',
                    'fileTypesIn': ['DIGI'],
                    'fileTypesOut':['DIGI'],
                    'visibilityFlag':[{'Visible': 'N', 'FileType': 'DIGI'}]},
                   {'StepId': 130090, 'StepName': 'TCK-0x6139160F (HLT2) Flagged for 2016 - DIGI', 'ApplicationName': 'Moore', 'ApplicationVersion': 'v25r4',
                    'ExtraPackages': 'AppConfig.v3r297', 'ProcessingPass': 'Trig0x6138160F', 'Visible': 'N', 'Usable': 'Yes',
                    'DDDB': 'dddb-20150724', 'CONDDB': 'sim-20161124-2-vc-md100', 'DQTag': '', 'OptionsFormat': '',
                    'OptionFiles': '$APPCONFIGOPTS/Moore/MooreSimProductionForSeparateL0AppStep2015.py;$APPCONFIGOPTS/Conditions/TCK-0x5138160F.py;$APPCONFIGOPTS/Moore/DataType-2016.py;$APPCONFIGOPTS/Persistency/Compression-ZLIB-1.py;$APPCONFIGOPTS/Moore/MooreSimProductionHlt1.py',
                    'isMulticore': 'N', 'SystemConfig': 'x86_64-slc6-gcc48-opt', 'mcTCK': '', 'ExtraOptions': '',
                    'fileTypesIn': ['DIGI'],
                    'fileTypesOut':['DIGI'],
                    'visibilityFlag':[{'Visible': 'N', 'FileType': 'DIGI'}]},
                   {'StepId': 130615, 'StepName': 'Reco16 for MC 2016', 'ApplicationName': 'Brunel', 'ApplicationVersion': 'v50r2',
                    'ExtraPackages': 'AppConfig.v3r314;SQLDDDB.v7r10', 'ProcessingPass': 'Reco16', 'Visible': 'N', 'Usable': 'Yes',
                    'DDDB': 'dddb-20150724', 'CONDDB': 'sim-20161124-2-vc-md100', 'DQTag': '', 'OptionsFormat': '',
                    'OptionFiles': '$APPCONFIGOPTS/Brunel/DataType-2016.py;$APPCONFIGOPTS/Brunel/MC-WithTruth.py;$APPCONFIGOPTS/Brunel/SplitRawEventOutput.4.3.py;$APPCONFIGOPTS/Persistency/Compression-ZLIB-1.py',
                    'isMulticore': 'N', 'SystemConfig': '', 'mcTCK': '', 'ExtraOptions': '',
                    'fileTypesIn': ['DIGI'],
                    'fileTypesOut':['DST'],
                    'visibilityFlag':[{'Visible': 'N', 'FileType': 'DST'}]},
                   ]

    # First create the production object
    prod = self.pr._buildProduction(prodType='MCSimulation', stepsInProd=stepsInProd, outputSE={'DST': 'Tier1_MC-DST'},
                                    priority=0, cpu=100, outputFileMask='DST')
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


class MCSuccessMultiProcessor(ProductionJobTestCase):
  def test_Integration_Production(self):

    # From request 12789
    stepsInProd = [{'StepId': 125080, 'StepName': 'Sim08a', 'ApplicationName': 'Gauss', 'ApplicationVersion': 'v45r3',
                    'ExtraPackages': 'AppConfig.v3r171', 'ProcessingPass': 'Sim08a', 'Visible': 'Y', 'Usable': 'Yes',
                    'DDDB': 'Sim08-20130503-1', 'CONDDB': 'Sim08-20130503-1-vc-mu100', 'DQTag': '', 'OptionsFormat': '',
                    'OptionFiles': '$APPCONFIGOPTS/Gauss/Sim08-Beam4000GeV-mu100-2012-nu2.5.py;$DECFILESROOT/options/11102400.py;$LBPYTHIA8ROOT/options/Pythia8.py;$APPCONFIGOPTS/Gauss/G4PL_FTFP_BERT_EmNoCuts.py;$APPCONFIGOPTS/Persistency/Compression-ZLIB-1.py',
                    'isMulticore': 'Y', 'SystemConfig': 'x86_64-slc5-gcc43-opt', 'mcTCK': '', 'ExtraOptions': '',
                    'fileTypesIn': [],
                    'fileTypesOut':['SIM'],
                    'visibilityFlag':[{'Visible': 'N', 'FileType': 'SIM'}]},
                   {'StepId': 124620, 'StepName': 'Digi13', 'ApplicationName': 'Boole', 'ApplicationVersion': 'v26r3',
                    'ExtraPackages': 'AppConfig.v3r164', 'ProcessingPass': 'Digi13', 'Visible': 'N', 'Usable': 'Yes',
                    'DDDB': 'Sim08-20130503-1', 'CONDDB': 'Sim08-20130503-1-vc-mu100', 'DQTag': '', 'OptionsFormat': '',
                    'isMulticore': 'Y', 'SystemConfig': 'x86_64-slc5-gcc43-opt', 'mcTCK': '', 'ExtraOptions': '',
                    'OptionFiles': '$APPCONFIGOPTS/Boole/Default.py;$APPCONFIGOPTS/Boole/DataType-2012.py;$APPCONFIGOPTS/Boole/Boole-SiG4EnergyDeposit.py;$APPCONFIGOPTS/Persistency/Compression-ZLIB-1.py',
                    'fileTypesIn': ['SIM'],
                    'fileTypesOut':['DIGI'],
                    'visibilityFlag':[{'Visible': 'N', 'FileType': 'DIGI'}]},
                   {'StepId': 124632, 'StepName': 'Trig0x409f0045', 'ApplicationName': 'Moore', 'ApplicationVersion': 'v14r8p1',
                    'ExtraPackages': 'AppConfig.v3r164', 'ProcessingPass': 'Trig0x40760037Flagged', 'Visible': 'N', 'Usable': 'Yes',
                    'DDDB': 'Sim08-20130503-1', 'CONDDB': 'Sim08-20130503-1-vc-mu100', 'DQTag': '', 'OptionsFormat': '',
                    'OptionFiles': '$APPCONFIGOPTS/Moore/MooreSimProductionWithL0Emulation.py;$APPCONFIGOPTS/Conditions/TCK-0x409f0045.py;$APPCONFIGOPTS/Moore/DataType-2012.py;$APPCONFIGOPTS/L0/L0TCK-0x0045.py',
                    'isMulticore': 'Y', 'SystemConfig': 'x86_64-slc5-gcc43-opt', 'mcTCK': '', 'ExtraOptions': '',
                    'fileTypesIn': ['DIGI'],
                    'fileTypesOut':['DIGI'],
                    'visibilityFlag':[{'Visible': 'N', 'FileType': 'DIGI'}]},
                   {'StepId': 124834, 'StepName': 'Reco14a', 'ApplicationName': 'Brunel', 'ApplicationVersion': 'v43r2p7',
                    'ExtraPackages': 'AppConfig.v3r164', 'ProcessingPass': 'Reco14a', 'Visible': 'N', 'Usable': 'Yes',
                    'DDDB': 'Sim08-20130503-1', 'CONDDB': 'Sim08-20130503-1-vc-mu100', 'DQTag': '', 'OptionsFormat': '',
                    'OptionFiles': '$APPCONFIGOPTS/Brunel/DataType-2012.py;$APPCONFIGOPTS/Brunel/MC-WithTruth.py;$APPCONFIGOPTS/Persistency/Compression-ZLIB-1.py',
                    'isMulticore': 'Y', 'SystemConfig': 'x86_64-slc5-gcc43-opt', 'mcTCK': '', 'ExtraOptions': '',
                    'fileTypesIn': ['DIGI'],
                    'fileTypesOut':['DST'],
                    'visibilityFlag':[{'Visible': 'N', 'FileType': 'DST'}]},
                   {'StepId': 124630, 'StepName': 'Stripping20NoPrescalingFlagged', 'ApplicationName': 'DaVinci', 'ApplicationVersion': 'v32r2p1',
                    'ExtraPackages': 'AppConfig.v3r164', 'ProcessingPass': 'Stripping20NoPrescalingFlagged', 'Visible': 'N', 'Usable': 'Yes',
                    'DDDB': 'Sim08-20130503-1', 'CONDDB': 'Sim08-20130503-1-vc-mu100', 'DQTag': '', 'OptionsFormat': '',
                    'OptionFiles': '$APPCONFIGOPTS/DaVinci/DV-Stripping20-Stripping-MC-NoPrescaling.py;$APPCONFIGOPTS/DaVinci/DataType-2012.py;$APPCONFIGOPTS/DaVinci/InputType-DST.py;$APPCONFIGOPTS/Persistency/Compression-ZLIB-1.py',
                    'isMulticore': 'Y', 'SystemConfig': 'x86_64-slc5-gcc43-opt', 'mcTCK': '', 'ExtraOptions': '',
                    'fileTypesIn': ['DST'],
                    'fileTypesOut':['ALLSTREAMS.DST'],
                    'visibilityFlag':[{'Visible': 'Y', 'FileType': 'ALLSTREAMS.DST'}]}]

    # First create the production object
    prod = self.pr._buildProduction('MCSimulation', stepsInProd, {'ALLSTREAMS.DST': 'Tier1_MC-DST'}, 0, 100,
                                    outputFileMask='ALLSTREAMS.DST')
    try:
      # This is the standard location in Jenkins
      prod.LHCbJob.setInputSandbox(find_all('pilot.cfg', os.environ['WORKSPACE'] + '/PilotInstallDIR')[0])
    except (IndexError, KeyError):
      prod.LHCbJob.setInputSandbox(find_all('pilot.cfg', rootPath)[0])
    prod.LHCbJob.setConfigArgs('pilot.cfg')
    prod.setParameter('numberOfEvents', 'string', 6, 'Number of events to test')
    # Then launch it
    res = self.diracProduction.launchProduction(prod, False, True, 0)

    self.assertTrue(res['OK'])


class MCSuccess_new(ProductionJobTestCase):
  def test_Integration_Production(self):

    # From request 12789
    stepsInProd = [{'StepId': 125080, 'StepName': 'Sim08a', 'ApplicationName': 'Gauss', 'ApplicationVersion': 'v45r3',
                    'ExtraPackages': 'AppConfig.v3r171', 'ProcessingPass': 'Sim08a', 'Visible': 'Y', 'Usable': 'Yes',
                    'DDDB': 'Sim08-20130503-1', 'CONDDB': 'Sim08-20130503-1-vc-mu100', 'DQTag': '', 'OptionsFormat': '',
                    'OptionFiles': '$APPCONFIGOPTS/Gauss/Sim08-Beam4000GeV-mu100-2012-nu2.5.py;$DECFILESROOT/options/11102400.py;$LBPYTHIA8ROOT/options/Pythia8.py;$APPCONFIGOPTS/Gauss/G4PL_FTFP_BERT_EmNoCuts.py;$APPCONFIGOPTS/Persistency/Compression-ZLIB-1.py',
                    'isMulticore': 'N', 'SystemConfig': 'x86_64-slc5-gcc43-opt', 'mcTCK': '', 'ExtraOptions': '',
                    'fileTypesIn': [],
                    'fileTypesOut':['SIM'],
                    'visibilityFlag':[{'Visible': 'N', 'FileType': 'SIM'}]},
                   {'StepId': 124620, 'StepName': 'Digi13', 'ApplicationName': 'Boole', 'ApplicationVersion': 'v26r3',
                    'ExtraPackages': 'AppConfig.v3r164', 'ProcessingPass': 'Digi13', 'Visible': 'N', 'Usable': 'Yes',
                    'DDDB': 'Sim08-20130503-1', 'CONDDB': 'Sim08-20130503-1-vc-mu100', 'DQTag': '', 'OptionsFormat': '',
                    'OptionFiles': '$APPCONFIGOPTS/Boole/Default.py;$APPCONFIGOPTS/Boole/DataType-2012.py;$APPCONFIGOPTS/Boole/Boole-SiG4EnergyDeposit.py;$APPCONFIGOPTS/Persistency/Compression-ZLIB-1.py',
                    'isMulticore': 'N', 'SystemConfig': 'x86_64-slc5-gcc43-opt', 'mcTCK': '', 'ExtraOptions': '',
                    'fileTypesIn': ['SIM'],
                    'fileTypesOut':['DIGI'],
                    'visibilityFlag':[{'Visible': 'N', 'FileType': 'DIGI'}]},
                   {'StepId': 124632, 'StepName': 'Trig0x409f0045', 'ApplicationName': 'Moore', 'ApplicationVersion': 'v14r8p1',
                    'ExtraPackages': 'AppConfig.v3r164', 'ProcessingPass': 'Trig0x40760037Flagged', 'Visible': 'N', 'Usable': 'Yes',
                    'DDDB': 'Sim08-20130503-1', 'CONDDB': 'Sim08-20130503-1-vc-mu100', 'DQTag': '', 'OptionsFormat': '',
                    'OptionFiles': '$APPCONFIGOPTS/Moore/MooreSimProductionWithL0Emulation.py;$APPCONFIGOPTS/Conditions/TCK-0x409f0045.py;$APPCONFIGOPTS/Moore/DataType-2012.py;$APPCONFIGOPTS/L0/L0TCK-0x0045.py',
                    'isMulticore': 'N', 'SystemConfig': 'x86_64-slc5-gcc43-opt', 'mcTCK': '', 'ExtraOptions': '',
                    'fileTypesIn': ['DIGI'],
                    'fileTypesOut':['DIGI'],
                    'visibilityFlag':[{'Visible': 'Y', 'FileType': 'DIGI'}]}]

    # First create the production object
    prod = self.pr._buildProduction('MCSimulation', stepsInProd, {'DIGI': 'Tier1_MC-DST'}, 0, 100,
                                    outputFileStep='3')
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
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(MCSuccessMultiProcessor))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(MCSuccess_new))
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)
